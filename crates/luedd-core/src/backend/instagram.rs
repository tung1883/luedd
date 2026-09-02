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

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use luedd_net::{HttpClient, ProgressTracker, ProgressTx, RequestContext, RequestOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use super::{Confidence, DownloadBackend, DownloadReq, EntryMeta, Outcome, Sniff};
use crate::jobs;

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                  (KHTML, like Gecko) Chrome/125.0 Safari/537.36";
const GRAPHQL: &str = "https://www.instagram.com/graphql/query/";
const PROFILE_INFO: &str = "https://www.instagram.com/api/v1/users/web_profile_info/";
const REELS_MEDIA: &str = "https://www.instagram.com/api/v1/feed/reels_media/";

/// insta-graphql.md §5: "Sleep 1-3 s between pages. A tight cursor loop is the
/// fastest way to a temporary block on the session."
const PAGE_PACE: Duration = Duration::from_secs(2);
const MAX_PAGES: usize = 40;
const PER_ITEM_PACE: Duration = Duration::from_millis(300);

/// Fallback persisted-query ids (insta-graphql.md §4), used when the user hasn't
/// set one in Settings. Instagram rotates these; when they 404, the fix is a
/// fresh value in Settings, not a rebuild.
const DEFAULT_DOC_SHORTCODE: &str = "24368985919464652";
const DEFAULT_DOC_TIMELINE: &str = "8759034877476257";

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

/// One tile in the live profile grid — metadata only, no media URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMeta {
    pub shortcode: String,
    pub thumb_url: Option<String>,
    pub is_video: bool,
    pub is_carousel: bool,
    /// Number of items in a carousel (0 when not a carousel / unknown) — lets the
    /// viewer show the real frame count before the full post is fetched.
    #[serde(default)]
    pub carousel_count: u32,
    pub taken_at: Option<i64>,
    pub caption: String,
}

/// One story / highlight frame — a directly-downloadable media node.
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
    /// Instagram soft-blocks a session that bursts; keep concurrency low.
    slots: Semaphore,
    /// Panel-preview thumbnail (url, square) per page URL — the panel calls
    /// `thumbnail()` on every visible row; `None` = looked, found nothing.
    thumb_cache: tokio::sync::Mutex<HashMap<String, Option<(String, bool)>>>,
    /// Viewer metadata (headers, grid pages, reels, trays) keyed by a string,
    /// value JSON-serialised, 5-minute TTL. Cheap re-polls without hammering IG.
    meta_cache: tokio::sync::Mutex<HashMap<String, (Instant, Value)>>,
    /// url -> resolved `@owner`; owners don't change, so no TTL.
    owner_cache: tokio::sync::Mutex<HashMap<String, String>>,
    /// When IG returns 429 on `web_profile_info`, skip that endpoint entirely
    /// until this instant — retrying a rate-limit only deepens the block.
    profile_info_rl_until: tokio::sync::Mutex<Option<Instant>>,
}

impl InstagramBackend {
    pub fn new(client: HttpClient) -> Self {
        Self {
            client,
            slots: Semaphore::new(2),
            thumb_cache: tokio::sync::Mutex::new(HashMap::new()),
            meta_cache: tokio::sync::Mutex::new(HashMap::new()),
            owner_cache: tokio::sync::Mutex::new(HashMap::new()),
            profile_info_rl_until: tokio::sync::Mutex::new(None),
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
    /// Cache with a much shorter effective lifetime (~90 s) by back-dating the
    /// timestamp — for a rate-limited/incomplete result we still want to stop
    /// re-hitting IG on every viewer open, but recover quickly once it clears.
    async fn cache_put_short<T: Serialize>(&self, key: &str, val: &T) {
        if let Ok(v) = serde_json::to_value(val) {
            let stamp = Instant::now()
                .checked_sub(META_TTL.saturating_sub(Duration::from_secs(90)))
                .unwrap_or_else(Instant::now);
            self.meta_cache.lock().await.insert(key.to_string(), (stamp, v));
        }
    }
}

#[derive(Debug)]
enum Target {
    /// `/p/<code>/`, `/reel/<code>/`, `/tv/<code>/` — one post / reel / carousel.
    /// The bool is `true` for a reel URL.
    Shortcode(String, bool),
    /// `/stories/<user>/` — the user's live story reel.
    Stories(String),
    /// `/stories/highlights/<id>/` — one saved highlight.
    Highlight(String),
    /// `/<user>/` — the whole post grid (paginated).
    Profile(String),
}

fn classify(url: &str) -> Option<Target> {
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
        if let Some(hit) = self.thumb_cache.lock().await.get(&req.url).cloned() {
            return Ok(hit);
        }
        let cfg = &req.config.instagram;
        let cookie =
            req.ctx.cookie.as_deref().filter(|c| !c.trim().is_empty()).or(cfg.session_cookie.as_deref());
        let Some(target) = classify(&req.url) else { return Ok(None) };
        let square = matches!(target, Target::Profile(_) | Target::Highlight(_));

        let resp = match &target {
            Target::Shortcode(code, _) => {
                let doc_id = cfg.doc_id_shortcode.as_deref().unwrap_or(DEFAULT_DOC_SHORTCODE);
                let vars = json!({
                    "shortcode": code, "fetch_tagged_user_count": null,
                    "hoisted_comment_id": null, "hoisted_reply_id": null,
                });
                self.gql(("doc_id", doc_id), &vars, cookie, "https://www.instagram.com/", &cfg.app_id).await.ok()
            }
            Target::Profile(user) => {
                // Prefer the account's profile picture (web_profile_info, §4.1);
                // if that endpoint is rate-limited, fall back to the newest
                // post's image from the timeline query.
                let pic = self.web_profile_info(user, cookie, &cfg.app_id).await.and_then(|u| {
                    u.get("profile_pic_url")
                        .or_else(|| u.get("profile_pic_url_hd"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
                if let Some(pic) = pic {
                    let spec = (pic, true);
                    self.thumb_cache.lock().await.insert(req.url.clone(), Some(spec.clone()));
                    return Ok(Some(spec));
                }
                let doc_id = cfg.doc_id_timeline.as_deref().unwrap_or(DEFAULT_DOC_TIMELINE);
                let vars = json!({
                    "data": { "count": 3, "include_relationship_info": false,
                              "latest_besties_reel_media": false, "latest_reel_media": false },
                    "username": user,
                    "__relay_internal__pv__PolarisIsLoggedInrelayprovider": true,
                    "__relay_internal__pv__PolarisFeedShareMenurelayprovider": true,
                });
                let referer = format!("https://www.instagram.com/{user}/");
                self.gql(("doc_id", doc_id), &vars, cookie, &referer, &cfg.app_id).await.ok()
            }
            // Stories / highlights need a session cookie; use the same
            // reels_media REST endpoint the download does.
            Target::Stories(user) if cookie.is_some() => {
                match self.resolve_user_id(user, cookie, &cfg.app_id).await {
                    Ok(uid) => self.reels_media(&format!("{uid}"), cookie, &cfg.app_id).await,
                    Err(_) => None,
                }
            }
            Target::Highlight(id) if cookie.is_some() => {
                self.reels_media(&format!("highlight%3A{id}"), cookie, &cfg.app_id).await
            }
            _ => None,
        };

        let spec = resp.as_ref().and_then(smallest_image_url).map(|u| (u, square));
        self.thumb_cache.lock().await.insert(req.url.clone(), spec.clone());
        Ok(spec)
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let _permit = self.slots.acquire().await.expect("semaphore closed");
        let cfg = &req.config.instagram;
        let cookie = req.ctx.cookie.as_deref().filter(|c| !c.trim().is_empty()).or(cfg.session_cookie.as_deref());
        let target = classify(&req.url)
            .ok_or_else(|| anyhow!("not an Instagram post / reel / story / profile URL"))?;

        let mut items: Vec<MediaItem> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        // Grouping metadata for the plugin views, refined as we learn it.
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

        match &target {
            Target::Shortcode(code, _) => {
                let doc_id = cfg.doc_id_shortcode.as_deref().unwrap_or(DEFAULT_DOC_SHORTCODE);
                let vars = json!({
                    "shortcode": code,
                    "fetch_tagged_user_count": null,
                    "hoisted_comment_id": null,
                    "hoisted_reply_id": null,
                });
                let referer = format!("https://www.instagram.com/p/{code}/");
                let resp = self.gql(("doc_id", doc_id), &vars, cookie, &referer, &cfg.app_id).await?;
                if let Some(owner) = find_owner(&resp) {
                    meta.author = Some(format!("@{owner}"));
                }
                walk(&resp, &mut items, &mut seen);
            }
            Target::Highlight(id) => {
                if cookie.is_none() {
                    bail!("Instagram: highlights need a logged-in session — open the highlight in your browser or paste a session cookie in Settings");
                }
                let url = format!("{REELS_MEDIA}?reel_ids=highlight%3A{id}");
                let text = self
                    .client
                    .get_text(&url, &ig_opts(cookie, "https://www.instagram.com/", &cfg.app_id))
                    .await
                    .context("Instagram highlight request failed")?;
                let resp = parse_ig_json(&text)?;
                if let Some(owner) = find_owner(&resp) {
                    meta.author = Some(format!("@{owner}"));
                }
                walk(&resp, &mut items, &mut seen);
            }
            Target::Stories(user) => {
                // Stories need a live session, and Instagram has moved them off
                // GraphQL onto this REST endpoint (insta-graphql.md §4.6 / §8);
                // the old `query_hash` for reels no longer resolves.
                if cookie.is_none() {
                    bail!("Instagram: stories need a logged-in session — open the story in your browser (so the extension captures the cookie) or paste a session cookie in Settings");
                }
                let uid = self.resolve_user_id(user, cookie, &cfg.app_id).await?;
                let referer = format!("https://www.instagram.com/stories/{user}/");
                let url = format!("{REELS_MEDIA}?reel_ids={uid}");
                let text = self
                    .client
                    .get_text(&url, &ig_opts(cookie, &referer, &cfg.app_id))
                    .await
                    .context("Instagram stories request failed")?;
                let resp = parse_ig_json(&text)?;
                walk(&resp, &mut items, &mut seen);
            }
            Target::Profile(user) => {
                let doc_id = cfg.doc_id_timeline.as_deref().unwrap_or(DEFAULT_DOC_TIMELINE);
                let referer = format!("https://www.instagram.com/{user}/");
                let mut after: Option<String> = None;
                for page in 0..MAX_PAGES {
                    if page > 0 {
                        tokio::time::sleep(PAGE_PACE).await;
                    }
                    let mut vars = json!({
                        "data": {
                            "count": 12,
                            "include_relationship_info": true,
                            "latest_besties_reel_media": true,
                            "latest_reel_media": true,
                        },
                        "username": user,
                        "__relay_internal__pv__PolarisIsLoggedInrelayprovider": true,
                        "__relay_internal__pv__PolarisFeedShareMenurelayprovider": true,
                    });
                    if let Some(cur) = &after {
                        vars["after"] = json!(cur);
                        vars["first"] = json!(12);
                    }
                    let resp = self.gql(("doc_id", doc_id), &vars, cookie, &referer, &cfg.app_id).await?;
                    let before = items.len();
                    walk(&resp, &mut items, &mut seen);
                    let page_info = find_page_info(&resp);
                    let has_next = page_info
                        .as_ref()
                        .and_then(|p| p.get("has_next_page"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    after = page_info
                        .as_ref()
                        .and_then(|p| p.get("end_cursor"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    // Stop on the last page, a missing cursor, or a page that
                    // added nothing (insta-graphql.md §7: an empty page is a
                    // soft-block signal — back off).
                    if !has_next || after.is_none() || items.len() == before {
                        break;
                    }
                }
            }
        }

        if items.is_empty() {
            bail!(
                "Instagram: no media found — a private/non-followed account, an expired story, \
                 or a rotted doc_id/query_hash (refresh it in Settings)"
            );
        }

        // Fresh context for the CDN fetch: the media hosts want a browser UA and
        // a Referer, and the same cookie (some signed URLs are session-scoped).
        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_string(), UA.to_string());
        headers.insert("Referer".to_string(), "https://www.instagram.com/".to_string());
        let dl_ctx = RequestContext { headers, cookie: cookie.map(str::to_string) };

        let tracker = ProgressTracker::new(progress, None, items.len() as u64);
        let mut files = Vec::new();
        let mut last_err: Option<anyhow::Error> = None;
        for (i, it) in items.iter().enumerate() {
            let dest = req.dest_dir.join(item_filename(it, i));
            match jobs::run_http(&self.client, &it.url, &dest, 1, &dl_ctx, None).await {
                Ok(path) => {
                    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    tracker.add_unit(bytes);
                    files.push(path);
                }
                Err(e) => {
                    tracing::warn!("Instagram item failed ({}): {e:#}", it.url);
                    last_err = Some(e);
                    tracker.add_unit(0);
                }
            }
            tokio::time::sleep(PER_ITEM_PACE).await;
        }
        tracker.finish();

        if files.is_empty() {
            bail!(
                "Instagram: every media item failed to download (CDN URLs may have expired){}",
                last_err.map(|e| format!(" — last error: {e}")).unwrap_or_default()
            );
        }
        Ok(Outcome { files, meta })
    }

    async fn describe(&self, req: &DownloadReq) -> EntryMeta {
        let mut meta = EntryMeta::default();
        let group = match classify(&req.url) {
            Some(Target::Shortcode(code, is_reel)) => {
                meta.media_class = Some(if is_reel { "reel" } else { "post" }.to_string());
                Some(format!("instagram_{}_{code}", if is_reel { "reel" } else { "post" }))
            }
            Some(Target::Stories(user)) => {
                meta.author = Some(format!("@{user}"));
                meta.media_class = Some("story".to_string());
                Some(format!("instagram_{user}_story"))
            }
            Some(Target::Highlight(id)) => {
                meta.media_class = Some("highlight".to_string());
                Some(format!("instagram_highlight_{id}"))
            }
            Some(Target::Profile(user)) => {
                meta.author = Some(format!("@{user}"));
                meta.media_class = Some("profile".to_string());
                Some(format!("instagram_{user}"))
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
    /// One GraphQL read. `doc_key` is `("doc_id", <id>)` or `("query_hash", <hash>)`.
    async fn gql(
        &self,
        doc_key: (&str, &str),
        variables: &Value,
        cookie: Option<&str>,
        referer: &str,
        app_id: &str,
    ) -> Result<Value> {
        let vars = serde_json::to_string(variables)?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair(doc_key.0, doc_key.1)
            .append_pair("variables", &vars)
            .finish();
        let full = format!("{GRAPHQL}?{query}");
        if std::env::var("IG_DEBUG").is_ok() {
            eprintln!("IG GET {full}");
        }
        let text = self
            .client
            .get_text(&full, &ig_opts(cookie, referer, app_id))
            .await
            .context("Instagram GraphQL request failed")?;
        parse_ig_json(&text)
    }

    /// `GET /api/v1/feed/reels_media/?reel_ids=<id>` -> parsed JSON, or `None`.
    /// `reel_id` is already URL-encoded (`<uid>` or `highlight%3A<id>`).
    async fn reels_media(&self, reel_id: &str, cookie: Option<&str>, app_id: &str) -> Option<Value> {
        let url = format!("{REELS_MEDIA}?reel_ids={reel_id}");
        let text = self
            .client
            .get_text(&url, &ig_opts(cookie, "https://www.instagram.com/", app_id))
            .await
            .ok()?;
        parse_ig_json(&text).ok()
    }

    /// insta-graphql.md §4.1: the `data.user` object for a username.
    async fn web_profile_info(&self, user: &str, cookie: Option<&str>, app_id: &str) -> Option<Value> {
        let debug = std::env::var("IG_DEBUG").is_ok();
        {
            let g = self.profile_info_rl_until.lock().await;
            if let Some(until) = *g {
                if Instant::now() < until {
                    if debug {
                        eprintln!("[ig] web_profile_info {user} skipped — rate-limit backoff active");
                    }
                    return None;
                }
            }
        }
        let url = format!("{PROFILE_INFO}?username={user}");
        let referer = format!("https://www.instagram.com/{user}/");
        // one shot only — retrying a 429 deepens the block
        match self.client.get_text(&url, &ig_opts(cookie, &referer, app_id)).await {
            Ok(text) => match parse_ig_json(&text).ok().and_then(|v| v.pointer("/data/user").cloned()) {
                Some(u) => Some(u),
                None => {
                    if debug {
                        eprintln!(
                            "[ig] web_profile_info {user} no /data/user; body: {}",
                            &text.chars().take(300).collect::<String>()
                        );
                    }
                    None
                }
            },
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("429") {
                    *self.profile_info_rl_until.lock().await = Some(Instant::now() + Duration::from_secs(600));
                }
                if debug {
                    eprintln!("[ig] web_profile_info {user} error: {e}");
                }
                None
            }
        }
    }

    /// username -> numeric user id (needed for stories).
    async fn resolve_user_id(&self, user: &str, cookie: Option<&str>, app_id: &str) -> Result<String> {
        self.web_profile_info(user, cookie, app_id)
            .await
            .as_ref()
            .and_then(|u| u.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("Instagram: could not resolve a user id for @{user} (stale cookie?)"))
    }

    // -- read-only surface for the profile viewer -----------------------------

    fn ig_cookie<'a>(cfg: &'a super::InstagramConfig, cookie: Option<&'a str>) -> Option<&'a str> {
        cookie.filter(|c| !c.trim().is_empty()).or(cfg.session_cookie.as_deref())
    }

    /// Account header (name / bio / stats / pic). Falls back to a username-only
    /// [`ProfileHeader`] with `complete: false` when web_profile_info is blocked.
    pub async fn profile_header(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> ProfileHeader {
        if let Some(hit) = self.cache_get::<ProfileHeader>(&format!("hdr:{user}")).await {
            return hit;
        }
        let cookie = Self::ig_cookie(cfg, cookie);
        let count = |u: &Value, k: &str| u.get(k).and_then(|x| x.get("count")).and_then(Value::as_u64).unwrap_or(0);
        let hdr = match self.web_profile_info(user, cookie, &cfg.app_id).await {
            Some(u) => ProfileHeader {
                username: u.get("username").and_then(Value::as_str).unwrap_or(user).to_string(),
                full_name: u.get("full_name").and_then(Value::as_str).unwrap_or_default().to_string(),
                biography: u.get("biography").and_then(Value::as_str).unwrap_or_default().to_string(),
                profile_pic_url: deep_find_hd_pic(&u)
                    .or_else(|| u.get("profile_pic_url_hd").and_then(Value::as_str).map(str::to_string))
                    .or_else(|| u.get("profile_pic_url").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_default(),
                is_private: u.get("is_private").and_then(Value::as_bool).unwrap_or(false),
                is_verified: u.get("is_verified").and_then(Value::as_bool).unwrap_or(false),
                external_url: u.get("external_url").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string),
                post_count: count(&u, "edge_owner_to_timeline_media"),
                follower_count: count(&u, "edge_followed_by"),
                following_count: count(&u, "edge_follow"),
                complete: true,
            },
            None => ProfileHeader {
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
            },
        };
        let mut hdr = hdr;
        // web_profile_info blocked → still get an avatar (and name) from the
        // timeline GraphQL query, which isn't rate-limited the same way. Same
        // fallback the detection-panel thumbnail already uses.
        if !hdr.complete && hdr.profile_pic_url.is_empty() {
            let doc_id = cfg.doc_id_timeline.as_deref().unwrap_or(DEFAULT_DOC_TIMELINE);
            let vars = json!({
                "data": { "count": 3, "include_relationship_info": false,
                          "latest_besties_reel_media": false, "latest_reel_media": false },
                "username": user,
                "__relay_internal__pv__PolarisIsLoggedInrelayprovider": true,
                "__relay_internal__pv__PolarisFeedShareMenurelayprovider": true,
            });
            let referer = format!("https://www.instagram.com/{user}/");
            if let Ok(resp) = self.gql(("doc_id", doc_id), &vars, cookie, &referer, &cfg.app_id).await {
                if let Some(pic) = deep_find_hd_pic(&resp)
                    .or_else(|| deep_find_str(&resp, "profile_pic_url_hd"))
                    .or_else(|| deep_find_str(&resp, "profile_pic_url"))
                    .or_else(|| smallest_image_url(&resp))
                {
                    hdr.profile_pic_url = pic;
                }
                if hdr.full_name.is_empty() {
                    if let Some(n) = deep_find_str(&resp, "full_name") {
                        hdr.full_name = n;
                    }
                }
            }
        }
        // a good header caches for the full TTL; an incomplete one caches briefly
        // so repeated viewer opens don't each pay the timeline-fallback round-trip
        if hdr.complete {
            self.cache_put(&format!("hdr:{user}"), &hdr).await;
        } else if !hdr.profile_pic_url.is_empty() || !hdr.full_name.is_empty() {
            self.cache_put_short(&format!("hdr:{user}"), &hdr).await;
        }
        hdr
    }

    /// One timeline page: `(tiles, next_cursor)`. Frontend drives pagination.
    pub async fn profile_posts(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
        after: Option<&str>,
    ) -> Result<(Vec<PostMeta>, Option<String>)> {
        let ckey = format!("posts:{user}:{}", after.unwrap_or(""));
        if let Some(hit) = self.cache_get::<(Vec<PostMeta>, Option<String>)>(&ckey).await {
            return Ok(hit);
        }
        let _permit = self.slots.acquire().await.expect("semaphore closed");
        let cookie = Self::ig_cookie(cfg, cookie);
        let doc_id = cfg.doc_id_timeline.as_deref().unwrap_or(DEFAULT_DOC_TIMELINE);
        let mut vars = json!({
            "data": { "count": 24, "include_relationship_info": true,
                      "latest_besties_reel_media": true, "latest_reel_media": true },
            "username": user,
            "__relay_internal__pv__PolarisIsLoggedInrelayprovider": true,
            "__relay_internal__pv__PolarisFeedShareMenurelayprovider": true,
        });
        if let Some(cur) = after {
            vars["after"] = json!(cur);
            vars["first"] = json!(24);
        }
        let referer = format!("https://www.instagram.com/{user}/");
        let resp = self.gql(("doc_id", doc_id), &vars, cookie, &referer, &cfg.app_id).await?;
        let posts = parse_timeline_nodes(&resp);
        let next = find_page_info(&resp)
            .filter(|p| p.get("has_next_page").and_then(Value::as_bool).unwrap_or(false))
            .and_then(|p| p.get("end_cursor").and_then(Value::as_str).map(str::to_string));
        let out = (posts, next);
        self.cache_put(&ckey, &out).await;
        Ok(out)
    }

    /// Every frame of one post (a carousel's children, or a single item) plus
    /// its caption — for the media overlay. `(items, caption)`.
    pub async fn post_media(
        &self,
        shortcode: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Result<(Vec<ReelItem>, String)> {
        let ckey = format!("post:{shortcode}");
        if let Some(hit) = self.cache_get::<(Vec<ReelItem>, String)>(&ckey).await {
            return Ok(hit);
        }
        let _permit = self.slots.acquire().await.expect("semaphore closed");
        let cookie = Self::ig_cookie(cfg, cookie);
        let doc_id = cfg.doc_id_shortcode.as_deref().unwrap_or(DEFAULT_DOC_SHORTCODE);
        let vars = json!({ "shortcode": shortcode, "fetch_tagged_user_count": null,
                           "hoisted_comment_id": null, "hoisted_reply_id": null });
        let resp = self.gql(("doc_id", doc_id), &vars, cookie, "https://www.instagram.com/", &cfg.app_id).await?;
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
        let ckey = format!("story:{user}");
        if let Some(hit) = self.cache_get::<Vec<ReelItem>>(&ckey).await {
            return Ok(hit);
        }
        let cookie = Self::ig_cookie(cfg, cookie);
        let uid = self.resolve_user_id(user, cookie, &cfg.app_id).await?;
        let resp = self
            .reels_media(&uid, cookie, &cfg.app_id)
            .await
            .ok_or_else(|| anyhow!("Instagram: stories need a logged-in session"))?;
        let items = parse_reel_items(&resp);
        self.cache_put(&ckey, &items).await;
        Ok(items)
    }

    /// Frames of one saved highlight.
    pub async fn highlight_items(
        &self,
        id: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Result<Vec<ReelItem>> {
        let ckey = format!("hl:{id}");
        if let Some(hit) = self.cache_get::<Vec<ReelItem>>(&ckey).await {
            return Ok(hit);
        }
        let cookie = Self::ig_cookie(cfg, cookie);
        let resp = self
            .reels_media(&format!("highlight%3A{id}"), cookie, &cfg.app_id)
            .await
            .ok_or_else(|| anyhow!("Instagram: highlights need a logged-in session"))?;
        let items = parse_reel_items(&resp);
        self.cache_put(&ckey, &items).await;
        Ok(items)
    }

    /// The highlights tray for a profile. Best effort — the GraphQL `query_hash`
    /// has rotted; try the REST tray, else return `[]` and the UI hides the row.
    pub async fn highlights_tray(
        &self,
        user: &str,
        cookie: Option<&str>,
        cfg: &super::InstagramConfig,
    ) -> Vec<HighlightMeta> {
        let ckey = format!("tray:{user}");
        if let Some(hit) = self.cache_get::<Vec<HighlightMeta>>(&ckey).await {
            return hit;
        }
        let cookie = Self::ig_cookie(cfg, cookie);
        let Ok(uid) = self.resolve_user_id(user, cookie, &cfg.app_id).await else {
            return Vec::new();
        };
        let url = format!("https://www.instagram.com/api/v1/highlights/{uid}/highlights_tray/");
        let referer = format!("https://www.instagram.com/{user}/");
        let tray = match self.client.get_text(&url, &ig_opts(cookie, &referer, &cfg.app_id)).await {
            Ok(text) => parse_ig_json(&text).ok().map(|v| parse_highlights_tray(&v)).unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        self.cache_put(&ckey, &tray).await;
        tray
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
        let _permit = self.slots.acquire().await.expect("semaphore closed");
        let cookie = Self::ig_cookie(cfg, cookie);
        let resp = match classify(url)? {
            Target::Shortcode(code, _) => {
                let doc_id = cfg.doc_id_shortcode.as_deref().unwrap_or(DEFAULT_DOC_SHORTCODE);
                let vars = json!({ "shortcode": code, "fetch_tagged_user_count": null,
                                   "hoisted_comment_id": null, "hoisted_reply_id": null });
                self.gql(("doc_id", doc_id), &vars, cookie, "https://www.instagram.com/", &cfg.app_id).await.ok()?
            }
            Target::Highlight(id) => self.reels_media(&format!("highlight%3A{id}"), cookie, &cfg.app_id).await?,
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
/// key must not count — IG includes it on image items).
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

fn parse_highlights_tray(resp: &Value) -> Vec<HighlightMeta> {
    fn trays(v: &Value) -> Option<&Vec<Value>> {
        match v {
            Value::Object(m) => {
                if let Some(a) = m.get("tray").and_then(Value::as_array) {
                    return Some(a);
                }
                m.values().find_map(trays)
            }
            Value::Array(a) => a.iter().find_map(trays),
            _ => None,
        }
    }
    let Some(arr) = trays(resp) else { return Vec::new() };
    arr.iter()
        .filter_map(|t| {
            let id = t
                .get("id")
                .and_then(Value::as_str)
                .map(|s| s.trim_start_matches("highlight:").to_string())?;
            Some(HighlightMeta {
                id,
                title: t.get("title").and_then(Value::as_str).unwrap_or_default().to_string(),
                cover_url: t
                    .get("cover_media")
                    .and_then(|c| c.get("cropped_image_version").or_else(|| c.get("cover_photo")))
                    .and_then(|c| c.get("url"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
}

fn ig_opts(cookie: Option<&str>, referer: &str, app_id: &str) -> RequestOptions {
    let mut headers = HashMap::new();
    headers.insert("User-Agent".to_string(), UA.to_string());
    headers.insert("X-IG-App-ID".to_string(), app_id.to_string());
    headers.insert("Referer".to_string(), referer.to_string());
    headers.insert("X-Requested-With".to_string(), "XMLHttpRequest".to_string());
    // Instagram's GraphQL executor throws "execution error" for some persisted
    // queries (notably the shortcode/web_info one) unless the request carries
    // the headers a real Polaris XHR sends. These two are the load-bearing ones.
    headers.insert("X-ASBD-ID".to_string(), "129477".to_string());
    headers.insert("X-IG-WWW-Claim".to_string(), "0".to_string());
    if let Some(c) = cookie {
        if let Some(csrf) = c.split(';').find_map(|p| p.trim().strip_prefix("csrftoken=")) {
            headers.insert("X-CSRFToken".to_string(), csrf.to_string());
        }
    }
    RequestOptions { headers, cookies: cookie.map(str::to_string), byte_range: None }
}

/// A logged-out or challenged request comes back as HTML or thin JSON rather
/// than an error (insta-graphql.md §7), so check the shape, not just the status.
fn parse_ig_json(text: &str) -> Result<Value> {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('{') {
        bail!(
            "Instagram returned a non-JSON response (a login or challenge page) — \
             the session cookie is missing or stale"
        );
    }
    let v: Value = serde_json::from_str(trimmed).context("Instagram response was not valid JSON")?;
    if v.get("errors").is_some() || v.get("status").and_then(Value::as_str) == Some("fail") {
        let msg = v.get("message").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| {
            v.get("errors")
                .and_then(|e| e.as_array())
                .and_then(|a| a.first())
                .and_then(|e| e.get("description").or_else(|| e.get("message")))
                .and_then(Value::as_str)
                .unwrap_or("check the doc_id / query_hash and cookie in Settings")
                .to_string()
        });
        if std::env::var("IG_DEBUG").is_ok() {
            eprintln!("IG raw response: {}", &trimmed[..trimmed.len().min(600)]);
        }
        bail!("Instagram API error: {msg}");
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
// Extraction — a direct port of ../ig_media.py
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

/// *Smallest* `src`/`url` from a resource list — for a lightweight panel thumb.
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
    // A carousel container: no media of its own — its children get walked.
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

/// First non-empty string value for `key` anywhere in the tree.
fn deep_find_str(v: &Value, key: &str) -> Option<String> {
    match v {
        Value::Object(m) => {
            if let Some(s) = m.get(key).and_then(Value::as_str).filter(|s| !s.is_empty()) {
                return Some(s.to_string());
            }
            m.values().find_map(|c| deep_find_str(c, key))
        }
        Value::Array(a) => a.iter().find_map(|c| deep_find_str(c, key)),
        _ => None,
    }
}

/// Largest available profile picture anywhere in the tree. IG exposes the
/// full-res avatar as `hd_profile_pic_url_info` (an object) or
/// `hd_profile_pic_versions` (a sized array); `profile_pic_url` alone is the
/// 150px thumbnail.
fn deep_find_hd_pic(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => {
            if let Some(u) = m
                .get("hd_profile_pic_url_info")
                .and_then(|o| o.get("url"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                return Some(u.to_string());
            }
            if let Some(arr) = m.get("hd_profile_pic_versions").and_then(Value::as_array) {
                if let Some(u) = arr
                    .iter()
                    .max_by_key(|c| c.get("width").and_then(Value::as_i64).unwrap_or(0))
                    .and_then(|c| c.get("url"))
                    .and_then(Value::as_str)
                {
                    return Some(u.to_string());
                }
            }
            m.values().find_map(deep_find_hd_pic)
        }
        Value::Array(a) => a.iter().find_map(deep_find_hd_pic),
        _ => None,
    }
}

/// First `page_info`-shaped object anywhere in the response.
fn find_page_info(v: &Value) -> Option<Value> {
    match v {
        Value::Object(m) => {
            if m.contains_key("has_next_page") {
                return Some(v.clone());
            }
            m.values().find_map(find_page_info)
        }
        Value::Array(a) => a.iter().find_map(find_page_info),
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

fn item_filename(it: &MediaItem, idx: usize) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(ts) = it.timestamp {
        parts.push(ts.to_string());
    }
    if !it.id.is_empty() {
        parts.push(it.id.clone());
    } else if let Some(sc) = &it.shortcode {
        parts.push(sc.clone());
    }
    let stem = if parts.is_empty() { format!("ig_media_{}", idx + 1) } else { parts.join("_") };
    format!("{}.{}", crate::naming::sanitize_filename_component(&stem), ext_from_url(&it.url, it.is_video))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cur = find_page_info(&resp)
            .and_then(|p| p.get("end_cursor").and_then(Value::as_str).map(str::to_string));
        assert_eq!(cur.as_deref(), Some("CURSOR123"));
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
            concurrency: 1,
            config: crate::backend::BackendConfig::default(),
        };
        match backend.run(&req, None).await {
            Ok(o) => {
                eprintln!("meta: author={:?} class={:?} title={:?}", o.meta.author, o.meta.media_class, o.meta.title);
                eprintln!("OK — {} files:", o.files.len());
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
