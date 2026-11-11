//! Custom Instagram backend.
//!
//! A Rust port of `../ig_media.py` driven by `../insta-graphql.md`. Instagram's
//! web client only accepts **persisted** GraphQL queries referenced by an opaque
//! `doc_id` / `query_hash`; those ids rot every few weeks and cannot be guessed,
//! so they live in `Settings.backends.instagram` where the user keeps them
//! current. The response parser is schema-agnostic: it walks the whole JSON tree
//! and picks up anything that looks like a media node (`is_video`,
//! `video_resources`, `display_resources`, `display_url`, `video_url`), which
//! covers single posts, carousels, reels, stories and the timeline grid alike.
//!
//! CDN media URLs are signed and expire within hours, so `run` fetches
//! everything immediately and never persists a media URL.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use futures::StreamExt;
use luedd_net::{HttpClient, ProgressTracker, ProgressTx, RequestContext};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use super::{Confidence, DownloadBackend, DownloadReq, EntryMeta, Outcome, Sniff};
use crate::jobs;

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                  (KHTML, like Gecko) Chrome/125.0 Safari/537.36";
static BROWSER_UA: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None);

/// Instagram binds a session to the browser UA that created it and answers a
/// different UA with a login wall / 400. The extension reports the real one.
pub fn set_user_agent(ua: &str) {
    let ua = ua.trim();
    if ua.starts_with("Mozilla/") && ua.len() < 400 {
        if let Ok(mut g) = BROWSER_UA.write() {
            *g = Some(ua.to_string());
        }
    }
}

fn user_agent() -> String {
    BROWSER_UA.read().ok().and_then(|g| g.clone()).unwrap_or_else(|| UA.to_string())
}


/// Post / reel JSON handed over by the browser extension, keyed by shortcode.
/// The extension loads the post page inside the user's own logged-in browser and
/// sends the media object it finds there, so posts need no Instagram API call
/// from the app. The CDN URLs inside are signed and expire, hence the TTL.
static CAPTURED: std::sync::Mutex<Option<HashMap<String, (Instant, Value)>>> = std::sync::Mutex::new(None);
const CAPTURE_TTL: Duration = Duration::from_secs(6 * 3600);
const CAPTURE_MAX: usize = 300;

/// The fields of a user/owner object the app reads.
fn slim_user(u: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for k in ["username", "pk", "id", "full_name", "is_private"] {
        if let Some(x) = u.get(k) {
            out.insert(k.to_string(), x.clone());
        }
    }
    Value::Object(out)
}

/// A resource list (`candidates`, `video_versions`, ...) cut down to `keys`.
///
/// Only the largest and the smallest entry can ever be picked (`best_resource` /
/// `smallest_resource`), so the others - a dozen signed ~600-char URLs per
/// image - are dropped. The same tie rules as those two functions decide which.
fn slim_list(list: &Value, keys: &[&str]) -> Value {
    match list.as_array() {
        Some(a) => Value::Array(
            prune_to_extremes(a)
                .into_iter()
                .map(|r| {
                    let mut o = serde_json::Map::new();
                    for k in keys {
                        if let Some(x) = r.get(*k) {
                            o.insert((*k).to_string(), x.clone());
                        }
                    }
                    Value::Object(o)
                })
                .collect(),
        ),
        None => list.clone(), // e.g. a `null` video_versions on an image item
    }
}

/// The entries `best_resource` (last maximum width) and `smallest_resource`
/// (first minimum width) would choose, in their original order.
fn prune_to_extremes(items: &[Value]) -> Vec<&Value> {
    let width = |r: &Value| r.get("config_width").or_else(|| r.get("width")).and_then(Value::as_i64);
    let has_src = |r: &Value| ["url", "src"].iter().any(|k| r.get(*k).and_then(Value::as_str).is_some());
    let (mut best, mut best_w): (Option<usize>, i64) = (None, i64::MIN);
    let (mut small, mut small_w): (Option<usize>, i64) = (None, i64::MAX);
    for (i, r) in items.iter().enumerate() {
        if !has_src(r) {
            continue;
        }
        if best.is_none() || width(r).unwrap_or(0) >= best_w {
            best = Some(i);
            best_w = width(r).unwrap_or(0);
        }
        if small.is_none() || width(r).unwrap_or(i64::MAX) < small_w {
            small = Some(i);
            small_w = width(r).unwrap_or(i64::MAX);
        }
    }
    items
        .iter()
        .enumerate()
        .filter(|(i, r)| !has_src(r) || Some(*i) == best || Some(*i) == small)
        .map(|(_, r)| r)
        .collect()
}

/// Keep only what the parsers read from a captured post / reel / story item.
/// A raw page object carries ~20 KB of likers, comments, music, tags and layout
/// data nothing here uses; hundreds of them stay in memory for hours.
fn slim_media(v: &Value) -> Value {
    let Value::Object(m) = v else { return v.clone() };
    let mut out = serde_json::Map::new();
    for k in [
        "id", "pk", "code", "shortcode", "taken_at", "taken_at_timestamp", "device_timestamp", "media_type",
        "product_type", "is_video", "video_url", "display_url", "thumbnail_src", "carousel_media_count",
        "edge_media_to_caption",
    ] {
        if let Some(x) = m.get(k) {
            out.insert(k.to_string(), x.clone());
        }
    }
    for k in ["user", "owner"] {
        if let Some(u) = m.get(k) {
            out.insert(k.to_string(), slim_user(u));
        }
    }
    if let Some(c) = m.get("caption") {
        let slim = match c {
            Value::Object(_) => c.get("text").map(|t| serde_json::json!({ "text": t })).unwrap_or(Value::Null),
            other => other.clone(),
        };
        out.insert("caption".to_string(), slim);
    }
    if let Some(iv) = m.get("image_versions2") {
        let cands = iv.get("candidates").map(|c| slim_list(c, &["url", "width", "height"])).unwrap_or(Value::Null);
        out.insert("image_versions2".to_string(), serde_json::json!({ "candidates": cands }));
    }
    if let Some(vv) = m.get("video_versions") {
        out.insert("video_versions".to_string(), slim_list(vv, &["url", "width", "height", "type"]));
    }
    for k in ["display_resources", "video_resources"] {
        if let Some(l) = m.get(k) {
            out.insert(k.to_string(), slim_list(l, &["src", "url", "config_width", "width", "height"]));
        }
    }
    if let Some(cm) = m.get("carousel_media") {
        let kids = cm.as_array().map(|a| Value::Array(a.iter().map(slim_media).collect())).unwrap_or_else(|| cm.clone());
        out.insert("carousel_media".to_string(), kids);
    }
    if let Some(edges) = m.get("edge_sidecar_to_children").and_then(|s| s.get("edges")).and_then(Value::as_array) {
        let slim: Vec<Value> = edges
            .iter()
            .map(|e| serde_json::json!({ "node": e.get("node").map(slim_media).unwrap_or(Value::Null) }))
            .collect();
        out.insert("edge_sidecar_to_children".to_string(), serde_json::json!({ "edges": slim }));
    }
    Value::Object(out)
}

/// A story / highlight reel: owner, cover and its items, slimmed.
fn slim_reel(v: &Value) -> Value {
    let Value::Object(m) = v else { return v.clone() };
    let mut out = serde_json::Map::new();
    for k in ["id", "title", "reel_type"] {
        if let Some(x) = m.get(k) {
            out.insert(k.to_string(), x.clone());
        }
    }
    if let Some(u) = m.get("user") {
        out.insert("user".to_string(), slim_user(u));
    }
    let cover_of = |o: &Value| {
        let mut c = serde_json::Map::new();
        for k in ["cropped_image_version", "cover_image_version"] {
            if let Some(x) = o.get(k) {
                c.insert(k.to_string(), slim_list(&Value::Array(vec![x.clone()]), &["url", "width", "height"])[0].clone());
            }
        }
        Value::Object(c)
    };
    if let Some(cover) = m.get("cover_media") {
        out.insert("cover_media".to_string(), cover_of(cover));
    }
    for (k, x) in cover_of(v).as_object().into_iter().flatten() {
        out.insert(k.clone(), x.clone());
    }
    if let Some(items) = m.get("items").and_then(Value::as_array) {
        out.insert("items".to_string(), Value::Array(items.iter().map(slim_media).collect()));
    }
    Value::Object(out)
}

/// Keep the media object the browser captured for `code` (slimmed first).
pub fn store_capture(code: &str, media: Value) {
    let slim = slim_media(&media);
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!(%code, raw_bytes = media.to_string().len(), slim_bytes = slim.to_string().len(), "capture slimmed");
    }
    store_slim(code, slim);
}

fn store_slim(code: &str, media: Value) {
    if let Ok(mut g) = CAPTURED.lock() {
        let map = g.get_or_insert_with(HashMap::new);
        map.retain(|_, (at, _)| at.elapsed() < CAPTURE_TTL);
        if map.len() >= CAPTURE_MAX {
            if let Some(oldest) = map.iter().min_by_key(|(_, (at, _))| *at).map(|(k, _)| k.clone()) {
                map.remove(&oldest);
            }
        }
        map.insert(code.to_string(), (Instant::now(), media));
    }
}

fn captured(code: &str) -> Option<Value> {
    let g = CAPTURED.lock().ok()?;
    let (at, v) = g.as_ref()?.get(code)?;
    (at.elapsed() < CAPTURE_TTL).then(|| v.clone())
}

/// Story reels / highlights the page loaded, keyed `u:<user>` or `h:<id>`.
static REELS: std::sync::Mutex<Option<HashMap<String, (Instant, Value)>>> = std::sync::Mutex::new(None);
/// Posts seen per account (profile grid / feed responses), newest first on read.
static PROFILE_POSTS: std::sync::Mutex<Option<HashMap<String, Vec<Value>>>> = std::sync::Mutex::new(None);
const PROFILE_POSTS_MAX: usize = 800;

fn reel_key_user(user: &str) -> String {
    format!("u:{}", user.trim_start_matches('@').to_ascii_lowercase())
}

fn reel_key_hl(id: &str) -> String {
    format!("h:{id}")
}

fn reel_for(key: &str) -> Option<Value> {
    let g = REELS.lock().ok()?;
    let (at, v) = g.as_ref()?.get(key)?;
    (at.elapsed() < CAPTURE_TTL).then(|| v.clone())
}

/// Posts captured for `user`, newest first.
fn profile_posts_captured(user: &str) -> Vec<Value> {
    let Ok(g) = PROFILE_POSTS.lock() else { return Vec::new() };
    let mut posts = g
        .as_ref()
        .and_then(|m| m.get(&user.trim_start_matches('@').to_ascii_lowercase()))
        .cloned()
        .unwrap_or_default();
    posts.sort_by_key(|p| std::cmp::Reverse(p.get("taken_at").and_then(Value::as_i64).unwrap_or(0)));
    posts
}

fn richness(v: &Value) -> usize {
    ["video_versions", "carousel_media", "image_versions2"].iter().filter(|k| v.get(**k).is_some()).count()
}

fn post_owner(v: &Value) -> Option<String> {
    ["user", "owner"]
        .iter()
        .find_map(|k| v.get(*k).and_then(|o| o.get("username")).and_then(Value::as_str))
        .map(|u| u.to_ascii_lowercase())
}

fn ingest_walk(v: &Value, posts: &mut Vec<Value>, reels: &mut Vec<(String, Value)>, depth: usize) {
    if depth > 60 {
        return;
    }
    match v {
        Value::Array(a) => a.iter().for_each(|x| ingest_walk(x, posts, reels, depth + 1)),
        Value::Object(m) => {
            let items = m.get("items").and_then(Value::as_array).filter(|a| !a.is_empty());
            let user = m.get("user").and_then(|u| u.get("username")).and_then(Value::as_str);
            if let (Some(items), Some(user)) = (items, user) {
                if items[0].get("image_versions2").is_some() || items[0].get("video_versions").is_some() {
                    let id = m
                        .get("id")
                        .and_then(|x| x.as_str().map(str::to_string).or_else(|| x.as_i64().map(|n| n.to_string())));
                    let key = match id.as_deref().and_then(|i| i.strip_prefix("highlight:")) {
                        Some(h) => reel_key_hl(h),
                        None => reel_key_user(user),
                    };
                    reels.push((key, slim_reel(v)));
                    return;
                }
            }
            let has_media = m.contains_key("image_versions2")
                || m.contains_key("video_versions")
                || m.contains_key("carousel_media");
            if has_media && m.get("code").and_then(Value::as_str).is_some() {
                posts.push(v.clone());
                return;
            }
            m.values().for_each(|x| ingest_walk(x, posts, reels, depth + 1));
        }
        _ => {}
    }
}

/// Take whatever the Instagram page itself just loaded (a story reel, a profile
/// grid page, a feed / explore response) and keep the posts and reels in it, so
/// they can be previewed and downloaded without the app calling Instagram.
/// Returns `(posts, reels)` kept.
pub fn ingest(json: &Value) -> (usize, usize) {
    let (mut posts, mut reels) = (Vec::new(), Vec::new());
    ingest_walk(json, &mut posts, &mut reels, 0);
    let n_reels = reels.len();
    if let Ok(mut g) = REELS.lock() {
        let map = g.get_or_insert_with(HashMap::new);
        map.retain(|_, (at, _)| at.elapsed() < CAPTURE_TTL);
        for (k, v) in reels {
            map.insert(k, (Instant::now(), v));
        }
    }
    let mut kept = 0;
    for post in posts {
        let Some(code) = post.get("code").and_then(Value::as_str).map(str::to_string) else { continue };
        if captured(&code).is_some_and(|old| richness(&old) > richness(&post)) {
            continue; // a thinner copy of a post already captured in full
        }
        let owner = post_owner(&post);
        let post = slim_media(&post); // once, then shared by the per-code and per-profile stores
        store_slim(&code, post.clone());
        kept += 1;
        if let (Some(owner), Ok(mut g)) = (owner, PROFILE_POSTS.lock()) {
            let map = g.get_or_insert_with(HashMap::new);
            if map.len() > 100 && !map.contains_key(&owner) {
                map.clear();
            }
            let list = map.entry(owner).or_default();
            match list.iter().position(|p| p.get("code").and_then(Value::as_str) == Some(code.as_str())) {
                Some(i) => list[i] = post,
                None => {
                    if list.len() < PROFILE_POSTS_MAX {
                        list.push(post);
                    }
                }
            }
        }
    }
    (kept, n_reels)
}

/// Profile header + highlights tray as the profile page showed them.
static PROFILE_META: std::sync::Mutex<Option<HashMap<String, (Instant, ProfileHeader, Vec<HighlightMeta>)>>> =
    std::sync::Mutex::new(None);

/// Keep a profile's header and highlight tiles read from the page by the extension.
pub fn store_profile_meta(hdr: ProfileHeader, tray: Vec<HighlightMeta>) {
    if hdr.username.is_empty() {
        return;
    }
    let key = hdr.username.to_ascii_lowercase();
    if let Ok(mut g) = PROFILE_META.lock() {
        let map = g.get_or_insert_with(HashMap::new);
        map.retain(|_, (at, _, _)| at.elapsed() < CAPTURE_TTL);
        if map.len() >= 200 {
            map.clear();
        }
        map.insert(key, (Instant::now(), hdr, tray));
    }
}

fn profile_meta_for(user: &str) -> Option<(ProfileHeader, Vec<HighlightMeta>)> {
    let g = PROFILE_META.lock().ok()?;
    let (at, h, t) = g.as_ref()?.get(&user.trim_start_matches('@').to_ascii_lowercase())?;
    (at.elapsed() < CAPTURE_TTL).then(|| (h.clone(), t.clone()))
}

/// A profile download asks the browser extension to scroll that profile so its
/// posts load (and get captured). Set while a download waits; read by `/sync`.
static SCROLL_REQ: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// The profile a running download wants scrolled in the browser, if any.
pub fn scroll_request() -> Option<String> {
    SCROLL_REQ.lock().ok().and_then(|g| g.clone())
}

fn set_scroll_request(user: Option<&str>) {
    if let Ok(mut g) = SCROLL_REQ.lock() {
        *g = user.map(str::to_ascii_lowercase);
    }
}

/// Unix seconds of the browser extension's last `/sync` poll (it polls every few
/// seconds while the browser is open). 0 = never seen this run.
static EXT_SEEN: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Called on every extension poll.
pub fn note_extension_seen() {
    EXT_SEEN.store(now_secs(), std::sync::atomic::Ordering::Relaxed);
}

/// Is the browser extension connected right now (polled within the last 30 s)?
pub fn extension_alive() -> bool {
    now_secs() - EXT_SEEN.load(std::sync::atomic::Ordering::Relaxed) < 30
}

/// Pages a running download needs the browser to open (so the extension captures
/// them). Set while a download waits; read by `/sync`.
static CAPTURE_REQ: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Instagram pages the extension should load in a background tab right now.
pub fn capture_requests() -> Vec<String> {
    CAPTURE_REQ.lock().map(|g| g.clone()).unwrap_or_default()
}

/// Ask the browser to load `url` and wait (up to ~30 s) for `ready` to turn true.
async fn ensure_captured(url: &str, ready: impl Fn() -> bool) {
    if ready() || !extension_alive() {
        return;
    }
    if let Ok(mut g) = CAPTURE_REQ.lock() {
        if !g.iter().any(|u| u == url) && g.len() < 10 {
            g.push(url.to_string());
        }
    }
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(750)).await;
        if ready() {
            break;
        }
    }
    if let Ok(mut g) = CAPTURE_REQ.lock() {
        g.retain(|u| u != url);
    }
}

/// Profiles the browser could not give a header for recently (private /
/// missing / page never loaded), so a viewer re-render does not reopen them.
static PROFILE_MISS: std::sync::Mutex<Option<HashMap<String, Instant>>> = std::sync::Mutex::new(None);
static PROFILE_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PROFILE_OPENS: std::sync::Mutex<Vec<Instant>> = std::sync::Mutex::new(Vec::new());

/// Make sure a profile's header was captured: if not, ask the browser to load
/// the profile page in a background tab and wait for it. One profile at a time
/// with a gap, so the browser opens pages like a person would.
pub async fn ensure_profile_meta(user: &str) {
    let user = user.trim_start_matches('@');
    ensure_profile_page(user, "h", || profile_meta_for(user).is_some()).await;
}

/// Same for a profile's post grid: the page's own first grid request supplies it.
pub async fn ensure_profile_posts(user: &str) {
    let user = user.trim_start_matches('@');
    ensure_profile_page(user, "p", || !profile_posts_captured(user).is_empty()).await;
}

async fn ensure_profile_page(user: &str, what: &str, ready: impl Fn() -> bool) {
    if user.is_empty() || ready() || !extension_alive() {
        return;
    }
    let key = format!("{what}:{}", user.to_ascii_lowercase());
    if PROFILE_MISS
        .lock()
        .ok()
        .and_then(|g| g.as_ref().and_then(|m| m.get(&key).copied()))
        .is_some_and(|at| at.elapsed() < Duration::from_secs(600))
    {
        return;
    }
    let _gate = PROFILE_GATE.lock().await;
    if ready() {
        return;
    }
    // A budget, not a crawl: at most a few background profile loads per window.
    // The rest fill in when the profile is visited in the browser.
    {
        let Ok(mut opens) = PROFILE_OPENS.lock() else { return };
        let win = Duration::from_secs(600);
        opens.retain(|t| t.elapsed() < win);
        if opens.len() >= 6 {
            return;
        }
        opens.push(Instant::now());
    }
    ensure_captured(&format!("https://www.instagram.com/{user}/"), &ready).await;
    if !ready() {
        if let Ok(mut g) = PROFILE_MISS.lock() {
            g.get_or_insert_with(HashMap::new).insert(key, Instant::now());
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
}

/// Shortcode of a post / reel / tv URL, for the capture endpoint.
pub fn shortcode_of(url: &str) -> Option<String> {
    match classify(url)? {
        Target::Shortcode(code, _) => Some(code),
        _ => None,
    }
}

/// Permit pool for byte transfers (`run`). A download acquires
/// `DL_POOL / concurrency` permits, so concurrency 1/2/3 -> 6/3/2 -> 1/2/3 runs.
const DL_POOL: usize = 6;
/// Separate pool for viewer metadata reads (`profile_posts`, `post_media`,
/// `resolve_account`) so a busy archive download can never block the grid.
const READ_POOL: usize = 4;


/// URL roots on `instagram.com` that are not a profile.
const RESERVED_ROOTS: &[&str] = &[
    "explore", "accounts", "direct", "stories", "p", "reel", "reels", "tv", "about", "developer", "legal",
    "press", "api", "graphql", "ajax", "web", "session",
];

/// The `data.user` fields the profile viewer needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileHeader {
    pub username: String,
    pub full_name: String,
    pub biography: String,
    pub profile_pic_url: String,
    pub is_private: bool,
    pub is_verified: bool,
    pub external_url: Option<String>,
    pub post_count: u64,
    pub follower_count: u64,
    pub following_count: u64,
    /// `false` = only the username is known (web_profile_info was blocked).
    pub complete: bool,
}

/// One tile in the live profile grid - metadata only, no media URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMeta {
    pub shortcode: String,
    pub thumb_url: Option<String>,
    pub is_video: bool,
    pub is_carousel: bool,
    /// Number of items in a carousel (0 when not a carousel / unknown) - lets the
    /// viewer show the real frame count before the full post is fetched.
    #[serde(default)]
    pub carousel_count: u32,
    pub taken_at: Option<i64>,
    pub caption: String,
}

/// One story / highlight frame - a directly-downloadable media node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReelItem {
    pub thumb_url: Option<String>,
    pub media_url: String,
    pub is_video: bool,
    pub taken_at: Option<i64>,
}

/// A highlight in the account's tray.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightMeta {
    pub id: String,
    pub title: String,
    pub cover_url: Option<String>,
}

const META_TTL: Duration = Duration::from_secs(300);

pub struct InstagramBackend {
    client: HttpClient,
    /// Byte-transfer concurrency (`run`). Instagram soft-blocks a bursting
    /// session, so keep it low.
    dl_slots: Semaphore,
    /// Viewer-read concurrency — its own pool so downloads can't starve it.
    read_slots: Semaphore,
    /// Panel-preview thumbnail (url, square) per page URL - the panel calls
    /// `thumbnail()` on every visible row; `None` = looked, found nothing -
    /// only trusted for `META_TTL`, since a miss is usually IG throttling.
    thumb_cache: tokio::sync::Mutex<HashMap<String, (Instant, Option<(String, bool)>)>>,
    /// Viewer metadata (headers, grid pages, reels, trays) keyed by a string,
    /// value JSON-serialised, 5-minute TTL. Cheap re-polls without hammering IG.
    meta_cache: tokio::sync::Mutex<HashMap<String, (Instant, Value)>>,
    /// url -> resolved `@owner`; owners don't change, so no TTL.
    owner_cache: tokio::sync::Mutex<HashMap<String, String>>,
}

impl InstagramBackend {
    pub fn new(client: HttpClient) -> Self {
        Self {
            client,
            dl_slots: Semaphore::new(DL_POOL),
            read_slots: Semaphore::new(READ_POOL),
            thumb_cache: tokio::sync::Mutex::new(HashMap::new()),
            meta_cache: tokio::sync::Mutex::new(HashMap::new()),
            owner_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn cache_get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let g = self.meta_cache.lock().await;
        let (t, v) = g.get(key)?;
        if t.elapsed() > META_TTL {
            return None;
        }
        serde_json::from_value(v.clone()).ok()
    }
    async fn cache_put<T: Serialize>(&self, key: &str, val: &T) {
        if let Ok(v) = serde_json::to_value(val) {
            self.meta_cache.lock().await.insert(key.to_string(), (Instant::now(), v));
        }
    }
}

#[derive(Debug)]
pub(crate) enum Target {
    /// `/p/<code>/`, `/reel/<code>/`, `/tv/<code>/` - one post / reel / carousel.
    /// The bool is `true` for a reel URL.
    Shortcode(String, bool),
    /// `/stories/<user>/` - the user's live story reel.
    Stories(String),
    /// `/stories/highlights/<id>/` - one saved highlight.
    Highlight(String),
    /// `/<user>/` - the whole post grid (paginated).
    Profile(String),
}

pub(crate) fn classify(url: &str) -> Option<Target> {
    let u = url::Url::parse(url).ok()?;
    let host = u.host_str()?.trim_start_matches("www.").to_ascii_lowercase();
    if host != "instagram.com" && !host.ends_with(".instagram.com") {
        return None;
    }
    // Keep original case: shortcodes are case-sensitive. Only the leading
    // segment (p / reel / tv / stories) is matched case-insensitively.
    let segs: Vec<&str> = u.path_segments()?.filter(|s| !s.is_empty()).collect();
    let head = segs.first()?.to_ascii_lowercase();
    match (head.as_str(), segs.get(1), segs.get(2)) {
        ("p" | "tv", Some(code), _) => Some(Target::Shortcode((*code).to_string(), false)),
        ("reel" | "reels", Some(code), _) => Some(Target::Shortcode((*code).to_string(), true)),
        ("stories", Some(&"highlights"), Some(id)) => Some(Target::Highlight((*id).to_string())),
        ("stories", Some(user), _) => Some(Target::Stories((*user).to_string())),
        (user, None, _) if !RESERVED_ROOTS.contains(&head.as_str()) => Some(Target::Profile(user.to_string())),
        _ => None,
    }
}

#[async_trait]
impl DownloadBackend for InstagramBackend {
    fn id(&self) -> &'static str {
        "instagram"
    }

    fn can_handle(&self, url: &str, _sniff: Option<&Sniff>) -> Confidence {
        // `Certain` so it wins over the yt-dlp backend, which also claims
        // instagram.com (kept there as a fallback for shapes this can't do).
        if classify(url).is_some() {
            Confidence::Certain
        } else {
            Confidence::No
        }
    }

    fn page_hosts(&self) -> &'static [&'static str] {
        &["instagram.com"]
    }

    /// A small thumbnail for the detection panel. One cheap request per page
    /// URL, cached. `square` is `true` for a 1:1 source (profile pic / highlight
    /// cover) so the panel sizes its slot to match.
    async fn thumbnail(&self, req: &DownloadReq) -> Result<Option<(String, bool)>> {
        if let Some((at, hit)) = self.thumb_cache.lock().await.get(&req.url).cloned() {
            if hit.is_some() || at.elapsed() < META_TTL {
                return Ok(hit);
            }
        }
        let Some(target) = classify(&req.url) else { return Ok(None) };
        let square = matches!(target, Target::Profile(_) | Target::Highlight(_));

        let resp = match &target {
            Target::Shortcode(code, _) => self.shortcode_resp(code).await.ok(),
            // the avatar from the header the profile page showed in the browser
            Target::Profile(user) => {
                match profile_meta_for(user).map(|(h, _)| h.profile_pic_url).filter(|u| !u.is_empty()) {
                    Some(pic) => {
                        let spec = (pic, true);
                        self.thumb_cache.lock().await.insert(req.url.clone(), (Instant::now(), Some(spec.clone())));
                        return Ok(Some(spec));
                    }
                    None => {
                        // not captured yet: have the browser load it (budgeted, in the
                        // background) so the next refresh of the panel has it
                        let user = user.clone();
                        tokio::spawn(async move { ensure_profile_meta(&user).await });
                        None
                    }
                }
            }
            Target::Stories(user) => reel_for(&reel_key_user(user)).and_then(|tray| match story_id_of(&req.url) {
                Some(id) => find_story_item(&tray, id),
                None => Some(tray),
            }),
            Target::Highlight(id) => reel_for(&reel_key_hl(id)),
        };

        let spec = resp.as_ref().and_then(smallest_image_url).map(|u| (u, square));
        // nothing captured yet must not cache "no thumbnail" - the capture lands later
        if spec.is_some() {
            self.thumb_cache.lock().await.insert(req.url.clone(), (Instant::now(), spec.clone()));
        }
        Ok(spec)
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        // Concurrency: acquire SLOT_POOL/concurrency permits so 1/2/3 configured
        // concurrency -> 6/3/2 permits -> 1/2/3 runs at once.
        let (_pg, _it, concurrency) = req.config.instagram.pace_values();
        let want = (DL_POOL / concurrency).max(1) as u32;
        let _permit = self.dl_slots.acquire_many(want).await.expect("semaphore closed");
        let target = classify(&req.url)
            .ok_or_else(|| anyhow!("not an Instagram post / reel / story / profile URL"))?;

        // Grouping metadata for the plugin views, refined as an engine learns it.
        let mut meta = EntryMeta::default();
        meta.media_class = Some(match &target {
            Target::Shortcode(_, true) => "reel",
            Target::Shortcode(_, false) => "post",
            Target::Stories(_) => "story",
            Target::Highlight(_) => "highlight",
            Target::Profile(_) => "profile",
        }
        .to_string());
        if let Target::Stories(u) | Target::Profile(u) = &target {
            meta.author = Some(format!("@{u}"));
        }

        // Lüdd-Insta has two interchangeable engines: the custom crawler and the
        // `instaloader` CLI. `engine_plan` orders them (primary, then a distinct
        // fallback); a failing engine hands off to the next, and the last one's
        // error surfaces.
        let plan = super::instaloader::engine_plan(&req.config, &target);
        let mut last_err: Option<anyhow::Error> = None;
        for (idx, engine) in plan.iter().enumerate() {
            let is_last = idx + 1 == plan.len();
            let result = match engine {
                super::instaloader::Engine::Custom => {
                    self.run_custom(req, progress, &target, meta.clone()).await
                }
                super::instaloader::Engine::Instaloader => super::instaloader::run(req, &target, progress)
                    .await
                    .map(|files| Outcome { files, meta: meta.clone(), ..Default::default() }),
            };
            match result {
                Ok(outcome) => return Ok(outcome),
                Err(e) if is_last => return Err(e),
                Err(e) => {
                    tracing::warn!(?engine, error = %e, "Instagram engine failed - trying the fallback");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no Instagram download engine configured")))
    }

    async fn describe(&self, req: &DownloadReq) -> EntryMeta {
        let mut meta = EntryMeta::default();
        // Folder layout mirrors instaloader's `{target}/` - the owner's username
        // when known, else the shortcode / highlight id.
        let group = match classify(&req.url) {
            Some(Target::Shortcode(code, is_reel)) => {
                meta.media_class = Some(if is_reel { "reel" } else { "post" }.to_string());
                Some(code)
            }
            Some(Target::Stories(user)) => {
                meta.author = Some(format!("@{user}"));
                meta.media_class = Some("story".to_string());
                Some(user)
            }
            Some(Target::Highlight(id)) => {
                meta.media_class = Some("highlight".to_string());
                Some(format!("highlight_{id}"))
            }
            Some(Target::Profile(user)) => {
                meta.author = Some(format!("@{user}"));
                meta.media_class = Some("profile".to_string());
                Some(user)
            }
            None => None,
        };
        // Every Instagram entry gets its own folder so a delete (or a
        // pause-then-delete of a half-finished carousel/profile) can wipe it whole.
        if let Some(g) = group {
            let safe: String = g
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
                .collect();
            meta.out_dir = Some(req.dest_dir.join(safe));
        }
        meta
    }
}

impl InstagramBackend {
    /// A post/reel by shortcode: the media object the browser extension captured
    /// from the post page. No request is made to Instagram - its API answers a
    /// non-browser client with a login wall, so the browser does the fetching.
    async fn shortcode_resp(&self, code: &str) -> Result<Value> {
        captured(code).ok_or_else(|| {
            anyhow!(
                "Instagram post {code} has not been captured yet - open it in your browser with the \
                 Lüdd extension enabled (logged in to Instagram), then retry"
            )
        })
    }


    /// The custom engine: crawl Instagram's own GraphQL/REST endpoints for the
    /// media list, then fetch each item off the CDN.
    async fn run_custom(
        &self,
        req: &DownloadReq,
        progress: Option<&ProgressTx>,
        target: &Target,
        mut meta: EntryMeta,
    ) -> Result<Outcome> {
        let cfg = &req.config.instagram;
        let cookie = req.ctx.cookie.as_deref().filter(|c| !c.trim().is_empty()).or(cfg.session_cookie.as_deref());
        let mut items: Vec<MediaItem> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        match target {
            Target::Shortcode(code, _) => {
                ensure_captured(&req.url, || captured(code).is_some()).await;
                let resp = self.shortcode_resp(code).await?;
                if let Some(owner) = find_owner(&resp) {
                    meta.author = Some(format!("@{owner}"));
                }
                walk(&resp, &mut items, &mut seen);
            }
            Target::Highlight(id) => {
                ensure_captured(&req.url, || reel_for(&reel_key_hl(id)).is_some()).await;
                let resp = reel_for(&reel_key_hl(id)).ok_or_else(|| {
                    anyhow!("Instagram highlight not captured yet - open it in your browser with the Lüdd extension enabled, then retry")
                })?;
                if let Some(owner) = find_owner(&resp) {
                    meta.author = Some(format!("@{owner}"));
                }
                walk(&resp, &mut items, &mut seen);
            }
            Target::Stories(user) => {
                ensure_captured(&req.url, || reel_for(&reel_key_user(user)).is_some()).await;
                let resp = reel_for(&reel_key_user(user)).ok_or_else(|| {
                    anyhow!("@{user}'s story not captured yet - open it in your browser with the Lüdd extension enabled, then retry")
                })?;
                walk(&resp, &mut items, &mut seen);
            }
            Target::Profile(user) => {
                let cap = cfg.max_posts.unwrap_or(usize::MAX);
                // How many posts the account has (from the header the page showed).
                let total = profile_meta_for(user).map(|(h, _)| h.post_count as usize).filter(|n| *n > 0);
                let target = cap.min(total.unwrap_or(usize::MAX));
                // Ask the browser to scroll the profile, then wait until the posts
                // stop arriving (or all are in). The extension does the scrolling.
                if !extension_alive() && profile_posts_captured(user).is_empty() {
                    bail!("the browser extension is not connected - open your browser with the Lüdd extension enabled (logged in to Instagram), then retry");
                }
                set_scroll_request(Some(user));
                let started = Instant::now();
                let mut last_n = profile_posts_captured(user).len();
                let mut last_growth = Instant::now();
                loop {
                    if last_n >= target {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    let n = profile_posts_captured(user).len();
                    if n > last_n {
                        last_n = n;
                        last_growth = Instant::now();
                    }
                    let idle = last_growth.elapsed();
                    // the browser needs time to pick up the request, open the profile
                    // and start scrolling, so be patient before giving up
                    if idle > Duration::from_secs(45) || started.elapsed() > Duration::from_secs(900)
                    {
                        break;
                    }
                }
                set_scroll_request(None);
                let posts = profile_posts_captured(user);
                if posts.is_empty() {
                    bail!("no posts of @{user} could be captured - keep the browser open with the Lüdd extension enabled and logged in to Instagram, then retry");
                }
                walk(&Value::Array(posts.into_iter().take(cap).collect()), &mut items, &mut seen);
            }
        }

        if items.is_empty() {
            bail!(
                "Instagram: no media found - a private/non-followed account, an expired story, \
                 or a rotted doc_id/query_hash (refresh it in Settings)"
            );
        }

        // Fresh context for the CDN fetch: the media hosts want a browser UA and
        // a Referer, and the same cookie (some signed URLs are session-scoped).
        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_string(), user_agent());
        headers.insert("Referer".to_string(), "https://www.instagram.com/".to_string());
        let dl_ctx = RequestContext { headers, cookie: cookie.map(str::to_string) };

        let tracker = ProgressTracker::new(progress, None, items.len() as u64);
        let mut last_err: Option<anyhow::Error> = None;
        // instaloader numbers only the members of one carousel (they share a
        // taken-at); a profile crawl's separate posts each keep a bare date name.
        let mut ts_total: std::collections::HashMap<Option<i64>, usize> = std::collections::HashMap::new();
        for it in &items {
            *ts_total.entry(it.timestamp).or_default() += 1;
        }
        let mut ts_seq: std::collections::HashMap<Option<i64>, usize> = std::collections::HashMap::new();
        let mut jobs_todo: Vec<(usize, String, std::path::PathBuf)> = Vec::with_capacity(items.len());
        for (i, it) in items.iter().enumerate() {
            let seq = {
                let e = ts_seq.entry(it.timestamp).or_default();
                *e += 1;
                *e
            };
            let n = if ts_total.get(&it.timestamp).copied().unwrap_or(1) > 1 { Some(seq) } else { None };
            jobs_todo.push((i, it.url.clone(), req.dest_dir.join(item_filename(it, i, n))));
        }

        // The files come off Instagram's CDN, not its API, so no per-item pause is
        // needed: fetch a few at a time. Results are put back in item order.
        let cdn_cc = cdn_concurrency(cfg);
        let client = &self.client;
        let (tracker_ref, ctx_ref) = (&tracker, &dl_ctx);
        let mut done: Vec<(usize, Option<std::path::PathBuf>, Option<anyhow::Error>)> = futures::stream::iter(jobs_todo)
            .map(|(i, url, dest)| async move {
                // Already on disk (a re-run of an interrupted post/story, or the
                // profile walker re-visiting): the final file only appears after an
                // atomic rename, so its presence means that item completed. Skip the
                // re-fetch - CDN bytes are immutable for a given shortcode/timestamp.
                if let Some(len) = std::fs::metadata(&dest).ok().map(|m| m.len()).filter(|l| *l > 0) {
                    tracker_ref.add_unit(len);
                    return (i, Some(dest), None);
                }
                match jobs::run_http_simple(client, &url, &dest, ctx_ref).await {
                    Ok(path) => {
                        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        tracker_ref.add_unit(bytes);
                        (i, Some(path), None)
                    }
                    Err(e) => {
                        tracing::warn!("Instagram item failed ({url}): {e:#}");
                        tracker_ref.add_unit(0);
                        (i, None, Some(e))
                    }
                }
            })
            .buffer_unordered(cdn_cc)
            .collect()
            .await;
        done.sort_by_key(|(i, _, _)| *i);
        let mut files = Vec::with_capacity(done.len());
        for (_, path, err) in done {
            if let Some(p) = path {
                files.push(p);
            }
            if err.is_some() {
                last_err = err;
            }
        }
        tracker.finish();

        if files.is_empty() {
            bail!(
                "Instagram: every media item failed to download (CDN URLs may have expired){}",
                last_err.map(|e| format!(" - last error: {e}")).unwrap_or_default()
            );
        }
        Ok(Outcome { files, meta, expected_units: Some(items.len() as u64) })
    }





    // -- read-only surface for the profile viewer (all from browser captures) --


    /// Account header (name / bio / stats / pic) as the profile page showed it in
    /// the browser. A username-only `complete: false` header when the profile has
    /// not been opened in the browser yet - no request is made to Instagram.
    pub async fn profile_header(
        &self,
        user: &str,
        _cookie: Option<&str>,
        _cfg: &super::InstagramConfig,
    ) -> ProfileHeader {
        if let Some((hdr, _)) = profile_meta_for(user) {
            return hdr;
        }
        ProfileHeader {
            username: user.to_string(),
            full_name: String::new(),
            biography: String::new(),
            profile_pic_url: String::new(),
            is_private: false,
            is_verified: false,
            external_url: None,
            post_count: 0,
            follower_count: 0,
            following_count: 0,
            complete: false,
        }
    }

    /// One timeline page: `(tiles, next_cursor)`. Frontend drives pagination.
    pub async fn profile_posts(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
        after: Option<&str>,
    ) -> Result<(Vec<PostMeta>, Option<String>)> {
        let _ = (cookie, cfg);
        // the grid comes from what the profile page itself loaded in the browser
        if after.is_some() {
            return Ok((Vec::new(), None));
        }
        ensure_profile_posts(user).await;
        let posts = profile_posts_captured(user);
        if posts.is_empty() {
            bail!("no posts of @{user} captured yet - open the profile in your browser with the Lüdd extension enabled and scroll");
        }
        let edges: Vec<Value> = posts.into_iter().map(|p| json!({ "node": p })).collect();
        Ok((parse_timeline_nodes(&json!({ "edges": edges })), None))
    }

    /// Every frame of one post (a carousel's children, or a single item) plus
    /// its caption - for the media overlay. `(items, caption)`.
    pub async fn post_media(
        &self,
        shortcode: &str,
        _cookie: Option<&str>,
        _cfg: &super::InstagramConfig,
    ) -> Result<(Vec<ReelItem>, String)> {
        let ckey = format!("post:{shortcode}");
        if let Some(hit) = self.cache_get::<(Vec<ReelItem>, String)>(&ckey).await {
            return Ok(hit);
        }
        // opened from the viewer: if the browser hasn't captured it yet, have it load the post
        ensure_captured(&format!("https://www.instagram.com/p/{shortcode}/"), || captured(shortcode).is_some()).await;
        let _permit = self.read_slots.acquire().await.expect("semaphore closed");
        let resp = self.shortcode_resp(shortcode).await?;
        if std::env::var("IG_DEBUG").is_ok() {
            let s = resp.to_string();
            eprintln!("IG post_media resp ({} bytes): {}", s.len(), &s[..s.len().min(3000)]);
        }
        let out = parse_post_items(&resp);
        self.cache_put(&ckey, &out).await;
        Ok(out)
    }

    /// Active-story frames for a username. Empty = no active story.
    pub async fn story_items(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Result<Vec<ReelItem>> {
        let _ = (cookie, cfg);
        let reel = reel_for(&reel_key_user(user)).ok_or_else(|| {
            anyhow!("@{user}'s story is not captured - open it in your browser with the Lüdd extension enabled")
        })?;
        Ok(parse_reel_items(&reel))
    }

    /// Frames of one saved highlight.
    pub async fn highlight_items(
        &self,
        id: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Result<Vec<ReelItem>> {
        let _ = (cookie, cfg);
        ensure_captured(&format!("https://www.instagram.com/stories/highlights/{id}/"), || reel_for(&reel_key_hl(id)).is_some()).await;
        let reel = reel_for(&reel_key_hl(id)).ok_or_else(|| {
            anyhow!("Instagram highlight is not captured - open it in your browser with the Lüdd extension enabled")
        })?;
        Ok(parse_reel_items(&reel))
    }

    /// The highlights tray for a profile. Best effort - the GraphQL `query_hash`
    /// has rotted; try the REST tray, else return `[]` and the UI hides the row.
    pub async fn highlights_tray(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Vec<HighlightMeta> {
        let _ = (cookie, cfg);
        profile_meta_for(user).map(|(_, tray)| tray).unwrap_or_default()
    }

    /// Resolve the owner (`@handle`) of an account-less caught URL (a `/p/<code>`
    /// or `/stories/highlights/<id>`). One request, cached forever.
    pub async fn resolve_account(
        &self,
        url: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Option<String> {
        if let Some(hit) = self.owner_cache.lock().await.get(url).cloned() {
            return Some(hit);
        }
        let _ = (cookie, cfg);
        let resp = match classify(url)? {
            Target::Shortcode(code, _) => self.shortcode_resp(&code).await.ok()?,
            Target::Highlight(id) => reel_for(&reel_key_hl(&id))?,
            _ => return None,
        };
        let owner = find_owner(&resp)?;
        self.owner_cache.lock().await.insert(url.to_string(), owner.clone());
        Some(owner)
    }
}

/// `xdt_api__v1__feed__user_timeline_graphql_connection.edges[].node` -> tiles.
fn parse_timeline_nodes(resp: &Value) -> Vec<PostMeta> {
    fn nodes(v: &Value) -> Option<&Vec<Value>> {
        match v {
            Value::Object(m) => {
                if let Some(e) = m.get("edges").and_then(Value::as_array) {
                    return Some(e);
                }
                m.values().find_map(nodes)
            }
            Value::Array(a) => a.iter().find_map(nodes),
            _ => None,
        }
    }
    let Some(edges) = nodes(resp) else { return Vec::new() };
    edges
        .iter()
        .filter_map(|e| {
            let n = e.get("node").unwrap_or(e);
            let shortcode = n.get("code").or_else(|| n.get("shortcode")).and_then(Value::as_str)?.to_string();
            let mt = n.get("media_type").and_then(Value::as_i64);
            Some(PostMeta {
                shortcode,
                thumb_url: smallest_resource(n.get("image_versions2").and_then(|x| x.get("candidates")), &["url"])
                    .or_else(|| n.get("display_url").and_then(Value::as_str).map(str::to_string))
                    .or_else(|| n.get("thumbnail_src").and_then(Value::as_str).map(str::to_string)),
                is_video: mt == Some(2),
                is_carousel: mt == Some(8)
                    || n.get("carousel_media").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
                    || n.get("carousel_media_count").and_then(Value::as_i64).is_some_and(|c| c > 1)
                    || n.get("edge_sidecar_to_children").and_then(|s| s.get("edges")).and_then(Value::as_array).is_some_and(|a| !a.is_empty()),
                carousel_count: n
                    .get("carousel_media_count")
                    .and_then(Value::as_u64)
                    .or_else(|| n.get("carousel_media").and_then(Value::as_array).map(|a| a.len() as u64))
                    .or_else(|| {
                        n.get("edge_sidecar_to_children")
                            .and_then(|s| s.get("edges"))
                            .and_then(Value::as_array)
                            .map(|a| a.len() as u64)
                    })
                    .unwrap_or(0) as u32,
                taken_at: n.get("taken_at").or_else(|| n.get("taken_at_timestamp")).and_then(Value::as_i64),
                caption: n
                    .get("caption")
                    .and_then(|c| if c.is_string() { c.as_str() } else { c.get("text").and_then(Value::as_str) })
                    .unwrap_or_default()
                    .chars()
                    .take(200)
                    .collect(),
            })
        })
        .collect()
}

/// True only when the node actually carries a video (a `null` `video_versions`
/// key must not count - IG includes it on image items).
fn node_is_video(n: &Value) -> bool {
    n.get("is_video").and_then(Value::as_bool).unwrap_or(false)
        || n.get("media_type").and_then(Value::as_i64) == Some(2)
        || n.get("video_url").and_then(Value::as_str).is_some_and(|s| !s.is_empty())
        || n.get("video_versions").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
        || n.get("video_resources").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
}

/// One post -> `(frames, caption)`. Handles both the `xdt_shortcode_media`
/// (`edge_sidecar_to_children`) and `xdt_api__v1__media__shortcode__web_info`
/// (`carousel_media`) shapes.
fn parse_post_items(resp: &Value) -> (Vec<ReelItem>, String) {
    fn find_media(v: &Value) -> Option<&Value> {
        match v {
            Value::Object(m) => {
                // a captured post is the bare media object itself
                if m.contains_key("image_versions2") || m.contains_key("video_versions") || m.contains_key("carousel_media") {
                    return Some(v);
                }
                for k in ["xdt_shortcode_media", "shortcode_media"] {
                    if let Some(n) = m.get(k) {
                        return Some(n);
                    }
                }
                if let Some(items) = m.get("items").and_then(Value::as_array).and_then(|a| a.first()) {
                    return Some(items);
                }
                m.values().find_map(find_media)
            }
            Value::Array(a) => a.iter().find_map(find_media),
            _ => None,
        }
    }
    let Some(node) = find_media(resp) else { return (Vec::new(), String::new()) };

    let caption = node
        .pointer("/edge_media_to_caption/edges/0/node/text")
        .and_then(Value::as_str)
        .or_else(|| node.get("caption").and_then(|c| if c.is_string() { c.as_str() } else { c.get("text").and_then(Value::as_str) }))
        .unwrap_or_default()
        .to_string();

    let children: Vec<&Value> = node
        .pointer("/edge_sidecar_to_children/edges")
        .and_then(Value::as_array)
        .map(|e| e.iter().filter_map(|c| c.get("node")).collect())
        .or_else(|| node.get("carousel_media").and_then(Value::as_array).map(|a| a.iter().collect()))
        .unwrap_or_else(|| vec![node]);

    let frames = children
        .iter()
        .filter_map(|c| {
            let is_video = node_is_video(c);
            let media_url = if is_video {
                best_resource(c.get("video_resources"), &["src", "url"])
                    .or_else(|| c.get("video_url").and_then(Value::as_str).map(str::to_string))
                    .or_else(|| best_resource(c.get("video_versions"), &["url"]))
            } else {
                best_resource(c.get("display_resources"), &["src", "url"])
                    .or_else(|| c.get("display_url").and_then(Value::as_str).map(str::to_string))
                    .or_else(|| best_resource(c.get("image_versions2").and_then(|x| x.get("candidates")), &["url"]))
            }?;
            Some(ReelItem {
                thumb_url: smallest_resource(c.get("image_versions2").and_then(|x| x.get("candidates")), &["url"])
                    .or_else(|| smallest_resource(c.get("display_resources"), &["src", "url"]))
                    .or_else(|| c.get("display_url").and_then(Value::as_str).map(str::to_string)),
                media_url,
                is_video,
                taken_at: node.get("taken_at").or_else(|| node.get("taken_at_timestamp")).and_then(Value::as_i64),
            })
        })
        .collect();
    (frames, caption)
}

/// `reels_media` (GraphQL `reels_media[]` or REST `reels{}`) -> item frames.
fn parse_reel_items(resp: &Value) -> Vec<ReelItem> {
    fn items(v: &Value) -> Option<&Vec<Value>> {
        match v {
            Value::Object(m) => {
                if m.contains_key("image_versions2") || m.contains_key("video_versions") {
                    return None; // this IS an item, handled by the caller's map
                }
                if let Some(a) = m.get("items").and_then(Value::as_array) {
                    return Some(a);
                }
                m.values().find_map(items)
            }
            Value::Array(a) => a.iter().find_map(items),
            _ => None,
        }
    }
    let Some(arr) = items(resp) else { return Vec::new() };
    arr.iter()
        .filter_map(|it| {
            let is_video = node_is_video(it);
            let media_url = if is_video {
                best_resource(it.get("video_versions"), &["url"])
                    .or_else(|| best_resource(it.get("video_resources"), &["src", "url"]))
            } else {
                best_resource(it.get("image_versions2").and_then(|x| x.get("candidates")), &["url"])
                    .or_else(|| it.get("display_url").and_then(Value::as_str).map(str::to_string))
            }?;
            Some(ReelItem {
                thumb_url: smallest_resource(it.get("image_versions2").and_then(|x| x.get("candidates")), &["url"]),
                media_url,
                is_video,
                taken_at: it.get("taken_at").and_then(Value::as_i64),
            })
        })
        .collect()
}



// ---------------------------------------------------------------------------
// Extraction - a direct port of ../ig_media.py
// ---------------------------------------------------------------------------

/// Keys that mark a node as carrying downloadable media. The first five are the
/// older `xdt_shortcode_media` / web shape (what `../ig_media.py` targeted); the
/// rest are the current `xdt_api__v1__feed` / `feed_user_timeline` shape.
const MEDIA_KEYS: &[&str] = &[
    "is_video",
    "video_resources",
    "display_resources",
    "display_url",
    "video_url",
    "image_versions2",
    "video_versions",
    "carousel_media",
];

#[derive(Debug)]
struct MediaItem {
    id: String,
    url: String,
    is_video: bool,
    timestamp: Option<i64>,
    shortcode: Option<String>,
}

/// Highest-resolution `src` (or `url`) from a `*_resources` / `candidates` list.
fn best_resource(list: Option<&Value>, keys: &[&str]) -> Option<String> {
    let arr = list?.as_array()?;
    let mut best: Option<(String, i64)> = None;
    for r in arr {
        let Some(src) = keys.iter().find_map(|k| r.get(*k).and_then(Value::as_str)) else { continue };
        let w = r
            .get("config_width")
            .or_else(|| r.get("width"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if best.as_ref().map_or(true, |(_, bw)| w >= *bw) {
            best = Some((src.to_string(), w));
        }
    }
    best.map(|(s, _)| s)
}


/// The numeric story id in `/stories/<user>/<id>/`, if the URL has one.
fn story_id_of(url: &str) -> Option<&str> {
    let rest = url.split("/stories/").nth(1)?;
    let mut segs = rest.split(['/', '?', '#']).filter(|s| !s.is_empty());
    segs.next()?;
    segs.next().filter(|s| s.chars().all(|c| c.is_ascii_digit()))
}

/// The reel item whose `pk`/`id` is `id` (ids come as `<pk>` or `<pk>_<owner>`).
fn find_story_item(v: &Value, id: &str) -> Option<Value> {
    match v {
        Value::Array(a) => a.iter().find_map(|x| find_story_item(x, id)),
        Value::Object(m) => {
            let pk = m
                .get("pk")
                .or_else(|| m.get("id"))
                .and_then(|p| p.as_str().map(str::to_string).or_else(|| p.as_i64().map(|n| n.to_string())));
            if m.contains_key("image_versions2") && pk.as_deref().is_some_and(|p| p.split('_').next() == Some(id)) {
                return Some(v.clone());
            }
            m.values().find_map(|x| find_story_item(x, id))
        }
        _ => None,
    }
}

/// *Smallest* `src`/`url` from a resource list - for a lightweight panel thumb.
fn smallest_resource(list: Option<&Value>, keys: &[&str]) -> Option<String> {
    let arr = list?.as_array()?;
    let mut best: Option<(String, i64)> = None;
    for r in arr {
        let Some(src) = keys.iter().find_map(|k| r.get(*k).and_then(Value::as_str)) else { continue };
        let w = r
            .get("config_width")
            .or_else(|| r.get("width"))
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX);
        if best.as_ref().map_or(true, |(_, bw)| w < *bw) {
            best = Some((src.to_string(), w));
        }
    }
    best.map(|(s, _)| s)
}

/// First usable image anywhere in a response, at its lowest resolution.
fn smallest_image_url(resp: &Value) -> Option<String> {
    match resp {
        Value::Object(m) => {
            // A highlight/story cover comes as a single cropped image.
            if let Some(u) = m
                .get("cropped_image_version")
                .or_else(|| m.get("cover_image_version"))
                .and_then(|x| x.get("url"))
                .and_then(Value::as_str)
            {
                return Some(u.to_string());
            }
            if let Some(u) =
                smallest_resource(m.get("image_versions2").and_then(|x| x.get("candidates")), &["url"])
            {
                return Some(u);
            }
            if let Some(u) = smallest_resource(m.get("display_resources"), &["src", "url"]) {
                return Some(u);
            }
            for k in ["thumbnail_src", "display_url"] {
                if let Some(u) = m.get(k).and_then(Value::as_str) {
                    return Some(u.to_string());
                }
            }
            m.values().find_map(smallest_image_url)
        }
        Value::Array(a) => a.iter().find_map(smallest_image_url),
        _ => None,
    }
}

fn media_url(node: &Value) -> Option<(String, bool)> {
    // A carousel container: no media of its own - its children get walked.
    if node.get("carousel_media").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
        || node.get("edge_sidecar_to_children").is_some()
    {
        return None;
    }
    let is_video = node.get("is_video").and_then(Value::as_bool).unwrap_or(false)
        || node.get("video_url").is_some()
        || node.get("video_resources").is_some()
        || node.get("media_type").and_then(Value::as_i64) == Some(2);
    let url = if is_video {
        best_resource(node.get("video_resources"), &["src", "url"])
            .or_else(|| node.get("video_url").and_then(Value::as_str).map(str::to_string))
            .or_else(|| best_resource(node.get("video_versions"), &["url"]))
    } else {
        best_resource(node.get("display_resources"), &["src", "url"])
            .or_else(|| node.get("display_url").and_then(Value::as_str).map(str::to_string))
            .or_else(|| {
                best_resource(node.get("image_versions2").and_then(|v| v.get("candidates")), &["url"])
            })
    };
    url.map(|u| (u, is_video))
}

fn node_id(node: &Value) -> String {
    node.get("id")
        .or_else(|| node.get("pk"))
        .map(|x| x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string()))
        .unwrap_or_default()
}

fn walk(v: &Value, out: &mut Vec<MediaItem>, seen: &mut HashSet<String>) {
    match v {
        Value::Object(map) => {
            if MEDIA_KEYS.iter().any(|k| map.contains_key(*k)) {
                if let Some((url, is_video)) = media_url(v) {
                    let id = node_id(v);
                    let key = if id.is_empty() { url.clone() } else { id.clone() };
                    if seen.insert(key) {
                        out.push(MediaItem {
                            id,
                            url,
                            is_video,
                            timestamp: v
                                .get("taken_at_timestamp")
                                .or_else(|| v.get("taken_at"))
                                .or_else(|| v.get("device_timestamp"))
                                .and_then(Value::as_i64),
                            shortcode: v
                                .get("shortcode")
                                .or_else(|| v.get("code"))
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        });
                    }
                }
            }
            // Carousel children / nested reels still need walking regardless.
            for val in map.values() {
                walk(val, out, seen);
            }
        }
        Value::Array(arr) => {
            for val in arr {
                walk(val, out, seen);
            }
        }
        _ => {}
    }
}

/// First `username` found under an `owner` / `user` object anywhere in the tree.
fn find_owner(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => {
            for key in ["owner", "user"] {
                if let Some(name) = m.get(key).and_then(|o| o.get("username")).and_then(Value::as_str) {
                    return Some(name.to_string());
                }
            }
            m.values().find_map(find_owner)
        }
        Value::Array(a) => a.iter().find_map(find_owner),
        _ => None,
    }
}




fn ext_from_url(url: &str, is_video: bool) -> String {
    let path = url::Url::parse(url).ok().map(|u| u.path().to_ascii_lowercase()).unwrap_or_default();
    for e in ["mp4", "jpg", "jpeg", "png", "webp", "heic"] {
        if path.ends_with(&format!(".{e}")) {
            return if e == "jpeg" { "jpg".to_string() } else { e.to_string() };
        }
    }
    if is_video { "mp4".to_string() } else { "jpg".to_string() }
}

/// instaloader-style name: `YYYY-MM-DD_HH-MM-SS_UTC[_N].ext`. `n` = the 1-based
/// index within a carousel (None for a lone item / a story frame).
/// How many media files to fetch at once from Instagram's CDN (not its API).
/// The pace preset scales it; an explicit `max_concurrent` (1-3) maps to 2/4/6.
fn cdn_concurrency(cfg: &super::InstagramConfig) -> usize {
    match (cfg.max_concurrent, cfg.pace.as_deref()) {
        (Some(n), _) => n.clamp(1, 3) * 2,
        (None, Some("gentle")) => 2,
        (None, Some("fast")) => 6,
        _ => 4,
    }
}

fn item_filename(it: &MediaItem, idx: usize, n: Option<usize>) -> String {
    let base = it
        .timestamp
        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
        .map(|dt| dt.format("%Y-%m-%d_%H-%M-%S_UTC").to_string())
        .or_else(|| it.shortcode.clone())
        .unwrap_or_else(|| format!("ig_media_{}", idx + 1));
    let stem = match n {
        Some(k) => format!("{base}_{k}"),
        None => base,
    };
    format!("{}.{}", crate::naming::sanitize_filename_component(&stem), ext_from_url(&it.url, it.is_video))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_keeps_story_reel_profile_posts_and_highlight() {
        let img = |u: &str| json!({ "candidates": [{ "url": u, "width": 100, "height": 100 }] });
        let page = json!({ "data": {
            "reels": { "42": { "id": "42", "user": { "username": "Ing_Story" }, "items": [
                { "pk": "9001", "media_type": 1, "image_versions2": img("https://x/s1.jpg") },
                { "pk": "9002", "media_type": 2, "video_versions": [{ "url": "https://x/s2.mp4" }],
                  "image_versions2": img("https://x/s2.jpg") } ] } },
            "hl": { "id": "highlight:777", "user": { "username": "ing_story" }, "items": [
                { "pk": "5", "media_type": 1, "image_versions2": img("https://x/h.jpg") } ] },
            "timeline": { "edges": [
                { "node": { "code": "IngP1", "pk": "1", "taken_at": 10, "user": { "username": "ing_prof" },
                            "image_versions2": img("https://x/p1.jpg") } },
                { "node": { "code": "IngP2", "pk": "2", "taken_at": 20, "user": { "username": "ing_prof" },
                            "image_versions2": img("https://x/p2.jpg") } } ] }
        } });
        assert_eq!(ingest(&page), (2, 2));

        // story: whole reel, user key is case-insensitive
        let reel = reel_for(&reel_key_user("ING_STORY")).unwrap();
        let (mut items, mut seen) = (Vec::new(), HashSet::new());
        walk(&reel, &mut items, &mut seen);
        assert_eq!(items.len(), 2);
        assert!(items.iter().any(|i| i.is_video));
        // a single story frame is found by the id in its URL, for the panel thumbnail
        let one = find_story_item(&reel, "9001").unwrap();
        assert_eq!(smallest_image_url(&one).as_deref(), Some("https://x/s1.jpg"));

        // highlight keyed by its id
        assert!(reel_for(&reel_key_hl("777")).is_some());

        // profile posts: newest first, each also individually capturable
        let posts = profile_posts_captured("@Ing_Prof");
        assert_eq!(posts.iter().map(|p| p["code"].as_str().unwrap()).collect::<Vec<_>>(), ["IngP2", "IngP1"]);
        assert!(captured("IngP1").is_some());

        // a thinner copy of an already-captured post must not replace the full one
        store_capture("IngRich", json!({ "code": "IngRich", "image_versions2": {}, "video_versions": [] }));
        ingest(&json!({ "n": { "code": "IngRich", "image_versions2": {} } }));
        assert!(captured("IngRich").unwrap().get("video_versions").is_some());
    }

    #[tokio::test]
    async fn profile_header_and_tray_come_from_the_browser_capture() {
        let backend = InstagramBackend::new(HttpClient::new().unwrap());
        let cfg = crate::backend::InstagramConfig::default();
        // nothing captured: username-only header, no tray, and no network is touched
        let h = backend.profile_header("Cap_Nobody", None, &cfg).await;
        assert!(!h.complete && h.username == "Cap_Nobody");
        assert!(backend.highlights_tray("cap_nobody", None, &cfg).await.is_empty());

        store_profile_meta(
            ProfileHeader {
                username: "Cap_Person".into(), full_name: "Cap Person".into(), biography: String::new(),
                profile_pic_url: "https://x/a.jpg".into(), is_private: false, is_verified: true,
                external_url: None, post_count: 34, follower_count: 1000, following_count: 5, complete: true,
            },
            vec![HighlightMeta { id: "9".into(), title: "Trip".into(), cover_url: None }],
        );
        let h = backend.profile_header("@cap_person", None, &cfg).await;
        assert!(h.complete && h.post_count == 34 && h.full_name == "Cap Person");
        let t = backend.highlights_tray("CAP_PERSON", None, &cfg).await;
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].title, "Trip");
    }

    #[tokio::test]
    async fn nothing_waits_for_a_browser_that_is_not_connected() {
        EXT_SEEN.store(0, std::sync::atomic::Ordering::Relaxed);
        assert!(!extension_alive());
        // returns at once instead of waiting ~30 s for a capture that cannot come
        let t = Instant::now();
        ensure_captured("https://www.instagram.com/p/NoBrowser1/", || false).await;
        ensure_profile_meta("no_browser_user").await;
        assert!(t.elapsed() < Duration::from_secs(2));
        assert!(capture_requests().is_empty());
        note_extension_seen();
        assert!(extension_alive());
    }

    #[test]
    fn viewer_opens_a_captured_post_carousel_with_caption() {
        let img = |u: &str| json!({ "candidates": [{ "url": u, "width": 1080, "height": 1080 }, { "url": format!("{u}s"), "width": 150, "height": 150 }] });
        let post = json!({
            "code": "ViewCar1", "taken_at": 77, "user": { "username": "someone" },
            "caption": { "text": "hello viewer" },
            "carousel_media": [
                { "media_type": 1, "image_versions2": img("https://x/a.jpg") },
                { "media_type": 2, "video_versions": [{ "url": "https://x/b.mp4", "width": 720, "height": 1280 }], "image_versions2": img("https://x/b.jpg") },
            ],
        });
        let (frames, caption) = parse_post_items(&post);
        assert_eq!(caption, "hello viewer");
        assert_eq!(frames.len(), 2);
        assert!(!frames[0].is_video && frames[0].media_url.starts_with("https://x/a.jpg"));
        assert!(frames[1].is_video && frames[1].media_url == "https://x/b.mp4");
        // single image / reel (no carousel) is one frame
        let single = json!({ "code": "ViewOne1", "media_type": 2, "video_versions": [{ "url": "https://x/r.mp4" }], "image_versions2": img("https://x/r.jpg") });
        let (frames, _) = parse_post_items(&single);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].is_video);
    }

    /// Minimal HTTP server: `/bad*` is a 404, anything else answers with its own path.
    fn serve_paths() -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut s) = conn else { continue };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let mut head = Vec::new();
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match s.read(&mut buf) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    let line = String::from_utf8_lossy(&head).lines().next().unwrap_or("").to_string();
                    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
                    std::thread::sleep(Duration::from_millis(30));
                    let (status, body) = if path.starts_with("/bad") { ("404 Not Found", String::new()) } else { ("200 OK", path) };
                    let resp = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    s.write_all(resp.as_bytes()).ok();
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn carousel_items_download_in_parallel_in_order_and_survive_one_failure() {
        let base = serve_paths();
        let cand = |path: &str| json!({ "candidates": [{ "url": format!("{base}{path}"), "width": 1080, "height": 1080 }] });
        let code = "ParCar1";
        store_capture(code, json!({
            "code": code, "taken_at": 1_700_000_000, "user": { "username": "par_user" },
            "carousel_media": [
                { "pk": "1", "media_type": 1, "image_versions2": cand("/a1") },
                { "pk": "2", "media_type": 1, "image_versions2": cand("/bad2") },
                { "pk": "3", "media_type": 1, "image_versions2": cand("/a3") },
                { "pk": "4", "media_type": 1, "image_versions2": cand("/a4") },
                { "pk": "5", "media_type": 1, "image_versions2": cand("/a5") },
            ],
        }));
        let dir = std::env::temp_dir().join(format!("luedd-ig-par-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let backend = InstagramBackend::new(HttpClient::new().unwrap());
        let req = DownloadReq {
            url: format!("https://www.instagram.com/p/{code}/"),
            dest_dir: dir.clone(),
            filename_hint: None,
            ctx: RequestContext { headers: HashMap::new(), cookie: None },
            quality: None,
            extras: Default::default(),
            concurrency: 1,
            config: crate::backend::BackendConfig::default(),
        };
        let t = Instant::now();
        let out = backend.run(&req, None).await.unwrap();
        let took = t.elapsed();

        assert_eq!(out.files.len(), 4, "the 404 item is skipped, the other four arrive");
        let bodies: Vec<String> = out.files.iter().map(|f| std::fs::read_to_string(f).unwrap()).collect();
        assert_eq!(bodies, ["/a1", "/a3", "/a4", "/a5"], "files come back in item order");
        // old loop: >= 5 x (30 ms + 300 ms pause); parallel with no pause is far below that
        assert!(took < Duration::from_millis(900), "took {took:?}");
        assert!(std::fs::read_dir(&dir).unwrap().flatten().all(|e| !e.file_name().to_string_lossy().ends_with(".luedd-part")));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A post as the page really delivers it: the media fields plus a pile of
    /// likers, comments, music, tags and layout data nothing here reads.
    fn bloated_post(code: &str, carousel: bool) -> Value {
        let cand = |u: &str, w: i64| json!({ "url": u, "width": w, "height": w, "scans_profile": "e35", "estimated_scans_sizes": [1, 2, 3, 4] });
        let child = |n: i64, video: bool| {
            let mut c = json!({
                "pk": format!("{n}"), "id": format!("{n}_9"), "media_type": if video { 2 } else { 1 },
                "image_versions2": { "candidates": [cand(&format!("https://c/{n}_big.jpg"), 1080), cand(&format!("https://c/{n}_small.jpg"), 150)],
                                     "additional_candidates": { "igtv_first_frame": cand("https://c/x.jpg", 10) } },
                "accessibility_caption": "Photo by someone. May be an image of 3 people and text ".repeat(4),
                "original_width": 1080, "original_height": 1350, "usertags": { "in": (0..8).map(|i| json!({ "user": { "username": format!("tag{i}"), "pk": i }, "position": [0.1, 0.2] })).collect::<Vec<_>>() },
            });
            if video {
                c["video_versions"] = json!([{ "url": format!("https://c/{n}_hi.mp4"), "width": 720, "height": 1280, "type": 101 }, { "url": format!("https://c/{n}_lo.mp4"), "width": 360, "height": 640, "type": 102 }]);
                c["video_dash_manifest"] = json!("<MPD>".repeat(1500));
            } else {
                c["video_versions"] = Value::Null;
            }
            c
        };
        let mut p = json!({
            "code": code, "pk": "777", "id": "777_9", "taken_at": 1_700_000_123, "media_type": if carousel { 8 } else { 2 },
            "product_type": if carousel { "carousel_container" } else { "clips" },
            "user": { "username": "big_user", "pk": "9", "id": "9", "full_name": "Big User", "is_private": false,
                      "profile_pic_url": "https://c/avatar.jpg", "friendship_status": { "following": false }, "latest_reel_media": 1 },
            "caption": { "text": "a caption", "pk": "1", "user": { "username": "big_user" }, "created_at": 5, "has_translation": false },
            "facepile_top_likers": (0..30).map(|i| json!({ "username": format!("liker{i}"), "profile_pic_url": format!("https://c/l{i}.jpg"), "pk": i })).collect::<Vec<_>>(),
            "preview_comments": (0..20).map(|i| json!({ "text": format!("nice one number {i} ").repeat(6), "user": { "username": format!("c{i}") } })).collect::<Vec<_>>(),
            "clips_metadata": { "music_info": { "music_asset_info": { "title": "song", "progressive_download_url": "https://c/a.m4a", "waveform_data": vec![0.5; 400] } } },
            "sharing_friction_info": { "should_have_sharing_friction": false, "bloks_app_url": null },
            "like_count": 12345, "comment_count": 321, "view_count": 99999,
        });
        if carousel {
            p["carousel_media"] = json!([child(1, false), child(2, true), child(3, false)]);
            p["carousel_media_count"] = json!(3);
        } else {
            let c = child(0, true);
            p["image_versions2"] = c["image_versions2"].clone();
            p["video_versions"] = c["video_versions"].clone();
            p["video_dash_manifest"] = c["video_dash_manifest"].clone();
        }
        p
    }

    #[test]
    fn slimming_keeps_everything_the_parsers_use_and_shrinks_it_a_lot() {
        for carousel in [false, true] {
            let full = bloated_post("SlimOne1", carousel);
            let slim = slim_media(&full);

            // the downloader
            let (mut a, mut sa) = (Vec::new(), HashSet::new());
            let (mut b, mut sb) = (Vec::new(), HashSet::new());
            walk(&full, &mut a, &mut sa);
            walk(&slim, &mut b, &mut sb);
            assert_eq!(format!("{a:?}"), format!("{b:?}"), "walk() must see the same items");
            assert!(!a.is_empty());
            // the viewer's post overlay
            assert_eq!(serde_json::to_value(parse_post_items(&full)).unwrap(), serde_json::to_value(parse_post_items(&slim)).unwrap());
            // the profile grid tile
            let tile = |v: &Value| serde_json::to_value(parse_timeline_nodes(&json!({ "edges": [{ "node": v }] }))).unwrap();
            assert_eq!(tile(&full), tile(&slim));
            // owner folder + panel thumbnail
            assert_eq!(find_owner(&full), find_owner(&slim));
            assert_eq!(find_owner(&slim).as_deref(), Some("big_user"));
            assert_eq!(smallest_image_url(&full), smallest_image_url(&slim));
            // size
            let (before, after) = (full.to_string().len(), slim.to_string().len());
            assert!(after * 2 < before, "carousel={carousel}: only {before} -> {after} bytes");
        }
    }

    #[test]
    fn pruning_keeps_exactly_the_entries_the_parsers_choose() {
        // many sizes, with ties for both the largest and the smallest width
        let widths = [640, 1080, 750, 150, 1080, 320, 150, 240, 1080, 100, 100, 480];
        let cands: Vec<Value> = widths.iter().enumerate().map(|(i, w)| json!({ "url": format!("https://c/{i}_{w}.jpg"), "width": w, "height": w })).collect();
        let vids: Vec<Value> = widths.iter().enumerate().map(|(i, w)| json!({ "url": format!("https://c/{i}_{w}.mp4"), "width": w, "height": w, "type": 101 })).collect();
        let full = json!({ "code": "Prune1", "pk": "1", "media_type": 2, "taken_at": 5, "user": { "username": "p" },
            "image_versions2": { "candidates": cands }, "video_versions": vids });
        let slim = slim_media(&full);
        assert_eq!(slim["image_versions2"]["candidates"].as_array().unwrap().len(), 2);
        assert_eq!(slim["video_versions"].as_array().unwrap().len(), 2);

        let (mut a, mut sa) = (Vec::new(), HashSet::new());
        let (mut b, mut sb) = (Vec::new(), HashSet::new());
        walk(&full, &mut a, &mut sa);
        walk(&slim, &mut b, &mut sb);
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
        assert_eq!(smallest_image_url(&full), smallest_image_url(&slim));
        assert_eq!(serde_json::to_value(parse_post_items(&full)).unwrap(), serde_json::to_value(parse_post_items(&slim)).unwrap());
        // an entry with no url is kept untouched (it can never be picked)
        let odd = slim_media(&json!({ "code": "Prune2", "image_versions2": { "candidates": [{ "width": 5 }, { "url": "https://c/a.jpg", "width": 9 }] } }));
        assert_eq!(odd["image_versions2"]["candidates"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn slimming_a_story_reel_keeps_items_owner_cover_and_frame_lookup() {
        let item = |pk: &str, video: bool| {
            let mut i = json!({ "pk": pk, "id": format!("{pk}_9"), "media_type": if video { 2 } else { 1 }, "taken_at": 1_700_000_000,
                "image_versions2": { "candidates": [{ "url": format!("https://c/{pk}.jpg"), "width": 1080, "height": 1920 }] },
                "story_cta": { "x": "y".repeat(2000) }, "story_hashtags": [], "viewers": (0..50).map(|n| json!({ "pk": n })).collect::<Vec<_>>() });
            if video {
                i["video_versions"] = json!([{ "url": format!("https://c/{pk}.mp4"), "width": 720, "height": 1280 }]);
            }
            i
        };
        let reel = json!({ "id": "highlight:55", "title": "Trip", "user": { "username": "Reel_User", "pk": "1", "profile_pic_url": "https://c/a.jpg" },
            "cover_media": { "cropped_image_version": { "url": "https://c/cover.jpg", "width": 150, "height": 150 }, "extra": [1, 2, 3] },
            "items": [item("1001", false), item("1002", true)], "seen": 0, "ranked_position": 3 });
        let slim = slim_reel(&reel);
        assert_eq!(find_owner(&slim).as_deref(), Some("Reel_User"));
        assert_eq!(parse_reel_items(&slim).len(), 2);
        assert_eq!(format!("{:?}", parse_reel_items(&reel)), format!("{:?}", parse_reel_items(&slim)));
        assert_eq!(smallest_image_url(&slim).as_deref(), Some("https://c/cover.jpg"));
        assert_eq!(smallest_image_url(&reel), smallest_image_url(&slim));
        assert_eq!(smallest_image_url(&find_story_item(&slim, "1002").unwrap()).as_deref(), Some("https://c/1002.jpg"));
        assert!(slim.to_string().len() * 2 < reel.to_string().len());
    }

    #[test]
    fn cdn_concurrency_follows_the_pace_preset() {
        let mut c = crate::backend::InstagramConfig::default();
        assert_eq!(cdn_concurrency(&c), 4);
        c.pace = Some("gentle".into());
        assert_eq!(cdn_concurrency(&c), 2);
        c.pace = Some("fast".into());
        assert_eq!(cdn_concurrency(&c), 6);
        c.max_concurrent = Some(9); // clamps to 3 -> 6
        assert_eq!(cdn_concurrency(&c), 6);
    }

    #[test]
    fn captured_post_is_served_without_network() {
        let code = "TestCapture1";
        assert!(captured(code).is_none());
        store_capture(code, json!({ "code": code, "image_versions2": { "candidates": [] } }));
        assert_eq!(captured(code).unwrap()["code"], code);
        assert_eq!(shortcode_of("https://www.instagram.com/reel/AbC_1-x/"), Some("AbC_1-x".into()));
        assert_eq!(shortcode_of("https://www.instagram.com/someuser/"), None);
    }

    #[test]
    fn classify_recognises_content_urls() {
        assert!(matches!(classify("https://www.instagram.com/p/DcLwQy_idBc/"), Some(Target::Shortcode(_, false))));
        assert!(matches!(classify("https://instagram.com/reel/ABC123/"), Some(Target::Shortcode(_, true))));
        assert!(matches!(classify("https://www.instagram.com/tv/XYZ/"), Some(Target::Shortcode(_, false))));
        assert!(matches!(classify("https://www.instagram.com/stories/quynhingx/"), Some(Target::Stories(_))));
        assert!(matches!(classify("https://www.instagram.com/stories/highlights/1811234/"), Some(Target::Highlight(_))));
        assert!(matches!(classify("https://www.instagram.com/quynhingx/"), Some(Target::Profile(_))));
        // shortcode case is preserved
        assert!(matches!(classify("https://www.instagram.com/p/DcLwQy_idBc/"),
            Some(Target::Shortcode(c, _)) if c == "DcLwQy_idBc"));
        assert!(classify("https://www.instagram.com/explore/").is_none());
        assert!(classify("https://www.instagram.com/accounts/login/").is_none());
        assert!(classify("https://youtube.com/watch?v=x").is_none());
        assert!(classify("https://scontent.cdninstagram.com/v/x.mp4").is_none());
    }

    #[test]
    fn parse_timeline_nodes_reads_shortcodes_and_flags() {
        let resp = json!({ "data": { "xdt_api__v1__feed__user_timeline_graphql_connection": {
            "edges": [
                { "node": { "code": "AAA", "media_type": 1,
                            "image_versions2": { "candidates": [{ "url": "https://cdn/a.jpg", "width": 320 }] },
                            "caption": { "text": "hello world" } } },
                { "node": { "code": "BBB", "media_type": 2,
                            "image_versions2": { "candidates": [{ "url": "https://cdn/b.jpg", "width": 320 }] } } },
                { "node": { "code": "CCC", "media_type": 8, "carousel_media_count": 3,
                            "display_url": "https://cdn/c.jpg" } }
            ],
            "page_info": { "has_next_page": true, "end_cursor": "CURSOR123" }
        }}});
        let posts = parse_timeline_nodes(&resp);
        assert_eq!(posts.len(), 3);
        assert_eq!(posts[0].shortcode, "AAA");
        assert_eq!(posts[0].caption, "hello world");
        assert!(!posts[0].is_video && !posts[0].is_carousel);
        assert!(posts[1].is_video);
        assert!(posts[2].is_carousel);
        assert_eq!(posts[2].thumb_url.as_deref(), Some("https://cdn/c.jpg"));
    }

    #[test]
    fn walk_extracts_carousel_and_video() {
        let resp = json!({
            "data": { "xdt_shortcode_media": {
                "id": "1", "shortcode": "AA", "is_video": false,
                "display_resources": [
                    { "src": "https://cdn/x_small.jpg", "config_width": 640 },
                    { "src": "https://cdn/x_big.jpg", "config_width": 1080 }
                ],
                "edge_sidecar_to_children": { "edges": [
                    { "node": { "id": "2", "is_video": true, "video_url": "https://cdn/v.mp4" } },
                    { "node": { "id": "3", "is_video": false, "display_url": "https://cdn/y.jpg" } }
                ]}
            }}
        });
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        walk(&resp, &mut out, &mut seen);
        // The sidecar container itself is skipped (its display_url just repeats
        // child 0); only the two children are collected.
        assert_eq!(out.len(), 2);
        assert!(out.iter().any(|m| m.is_video && m.url == "https://cdn/v.mp4"));
        assert!(out.iter().any(|m| !m.is_video && m.url == "https://cdn/y.jpg"));
    }

    #[test]
    fn walk_dedupes_by_id() {
        let resp = json!([
            { "id": "9", "is_video": false, "display_url": "https://cdn/a.jpg" },
            { "id": "9", "is_video": false, "display_url": "https://cdn/a.jpg" }
        ]);
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        walk(&resp, &mut out, &mut seen);
        assert_eq!(out.len(), 1);
    }

    /// Live network probe. `cargo test -p luedd-core -- --ignored --nocapture
    /// instagram::tests::live_profile`. Set `IG_COOKIE` for private/rate-limited
    /// runs; `IG_URL` to override the target.
    #[tokio::test]
    #[ignore]
    async fn live_profile() {
        let url = std::env::var("IG_URL").unwrap_or_else(|_| "https://www.instagram.com/quynhingx/".into());
        let cookie = std::env::var("IG_COOKIE").ok();
        let dir = std::env::temp_dir().join("luedd-ig-live");
        std::fs::create_dir_all(&dir).unwrap();

        let backend = InstagramBackend::new(HttpClient::new().unwrap());
        let req = DownloadReq {
            url,
            dest_dir: dir.clone(),
            filename_hint: None,
            ctx: RequestContext { headers: HashMap::new(), cookie },
            quality: None,
            extras: Default::default(),
            concurrency: 1,
            config: crate::backend::BackendConfig::default(),
        };
        match backend.run(&req, None).await {
            Ok(o) => {
                eprintln!("meta: author={:?} class={:?} title={:?}", o.meta.author, o.meta.media_class, o.meta.title);
                eprintln!("OK - {} files:", o.files.len());
                for f in &o.files {
                    eprintln!("  {} ({} bytes)", f.display(), std::fs::metadata(f).map(|m| m.len()).unwrap_or(0));
                }
                assert!(!o.files.is_empty());
            }
            Err(e) => panic!("instagram run failed: {e:#}"),
        }
    }

    #[test]
    fn newer_shapes_image_and_video_versions() {
        let node = json!({
            "media_type": 2,
            "video_versions": [ { "url": "https://cdn/lo.mp4", "width": 480 }, { "url": "https://cdn/hi.mp4", "width": 1080 } ],
            "image_versions2": { "candidates": [ { "url": "https://cdn/thumb.jpg", "width": 320 } ] }
        });
        let (url, is_video) = media_url(&node).unwrap();
        assert!(is_video);
        assert_eq!(url, "https://cdn/hi.mp4");
    }
}
