
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::Method;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use luedd_core::backend::instagram::{InstagramBackend, ProfileHeader};
use luedd_core::backend::{DownloadBackend, YtdlpBackend};
use luedd_core::ig_library::{IgCaught, IgLibraryStore, UNRESOLVED};
use luedd_core::jobs::DownloadKind;
use luedd_core::yt_library::{YtCaught, YtLibraryStore, YtResolve};
use luedd_core::queue::{DownloadEntry, DownloadManager, DownloadStore, SettingsStore};
use luedd_net::{HttpClient, RequestContext};

pub struct ServerConfig {
    pub settings: Arc<SettingsStore>,
    /// Opaque build marker surfaced on `/sync` so you can tell which binary a
    /// running instance is (helps catch "the fix didn't take" = stale process).
    pub build_id: String,
    pub on_new_detection: Option<tokio::sync::mpsc::UnboundedSender<VideoListItem>>,
    /// Fired when a second app instance pings `/focus-main` asking the running
    /// instance to surface its main window.
    pub on_focus_request: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    /// Where to cache the last Instagram `sessionid` cookie so it survives a
    /// restart (the extension only re-pushes it once it reconnects). `None`
    /// disables the on-disk cache.
    pub ig_cookie_cache: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone)]
struct DetectedMedia {
    id: String,
    url: String,
    tab_url: Option<String>,
    page_title: Option<String>,
    page_url: Option<String>,
    request_headers: HashMap<String, Vec<String>>,
    cookie: Option<String>,
    user_agent: Option<String>,
    /// Whether this detection is an image (from the response Content-Type the
    /// extension captured, or the URL extension). Drives the inline thumbnail.
    is_image: bool,
    /// A page-URL detection (a yt-dlp watch page). Its URL has no media
    /// extension, so the panel's type filter must be told it's a video.
    is_page: bool,
    /// Panel `kind` derived from the response Content-Type at detect time
    /// (`hls` / `video` / `audio` / …). Lets an extensionless stream URL show
    /// the right icon. `None` = let the panel guess from the URL.
    kind_hint: Option<String>,
    /// Human provider name ("Lüdd", "yt-dlp", …) — a cheap guess made when the
    /// detection is recorded, for the panel's group-by / filter-by-provider.
    provider: String,
}

/// A generated thumbnail plus how the client should render it.
type Preview = (String, &'static str); // (data URL, kind: "image" | "video" | "pdf")

struct AppState {
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    registry: Arc<luedd_core::backend::BackendRegistry>,
    /// The concrete Instagram backend — the `/ig/*` viewer endpoints call its
    /// read-only metadata methods directly (not through the `DownloadBackend` trait).
    instagram: Arc<InstagramBackend>,
    /// Persistent index of caught Instagram accounts (survives restarts).
    ig_library: Arc<IgLibraryStore>,
    /// The concrete yt-dlp backend — the `/yt/*` viewer endpoints call
    /// `probe_meta` / `probe_qualities` directly.
    ytdlp: Arc<YtdlpBackend>,
    /// Persistent index of caught yt-dlp channels (survives restarts).
    yt_library: Arc<YtLibraryStore>,
    /// Set while the background `-J` resolver is walking the unresolved bucket,
    /// so overlapping `/yt/channels` calls don't double-spawn it.
    yt_resolving: AtomicBool,
    /// Instagram `sessionid` cookies seen this run (or restored from the cache),
    /// newest first. The `/ig/*` endpoints have no per-request cookie of their
    /// own — they try these in order and promote whichever one works, so any
    /// number of signed-in browsers can contribute a session.
    ig_cookies: Mutex<Vec<String>>,
    config: ServerConfig,
    detected: Mutex<Vec<DetectedMedia>>,
    next_id: AtomicU64,
    /// url -> resolved preview (None = tried, nothing to show). Previews are
    /// expensive (an HTTP fetch or an ffmpeg spawn) and the client re-polls, so
    /// every result is memoised, successes and failures alike.
    preview_cache: Mutex<HashMap<String, Option<Preview>>>,
    /// url -> quality variants, memoised like previews (probing re-fetches the
    /// playlist each time otherwise).
    quality_cache: Mutex<HashMap<String, Vec<luedd_media::quality::QualityOption>>>,
    /// Caps concurrent ffmpeg thumbnail jobs so a page full of video detections
    /// can't fork-bomb the machine.
    ffmpeg_slots: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Serialize)]
struct SyncResponse {
    enabled: bool,
    #[serde(rename = "fileExts")]
    file_exts: Vec<String>,
    #[serde(rename = "blockedHosts")]
    blocked_hosts: Vec<String>,
    #[serde(rename = "tabsWatcher")]
    tabs_watcher: Vec<String>,
    #[serde(rename = "videoList")]
    video_list: Vec<VideoListItem>,
    #[serde(rename = "requestFileExts")]
    request_file_exts: Vec<String>,
    #[serde(rename = "matchingHosts")]
    matching_hosts: Vec<String>,
    #[serde(rename = "mediaTypes")]
    media_types: Vec<String>,
    /// Hosts whose *page* URL the extension should offer as a detection
    /// (yt-dlp watch pages etc). Populated only on `/sync`.
    #[serde(rename = "pageHosts")]
    page_hosts: Vec<String>,
    #[serde(rename = "serverBuild")]
    server_build: String,
    /// Human-facing provider labels for every installed backend (so the panel
    /// lists a provider even when it has 0 links). Populated only on `/sync`.
    #[serde(rename = "providers")]
    providers: Vec<String>,
    #[serde(rename = "newDetection", skip_serializing_if = "Option::is_none")]
    new_detection: Option<VideoListItem>,
    #[serde(rename = "vidQueued", skip_serializing_if = "Option::is_none")]
    vid_queued: Option<bool>,
    #[serde(rename = "qualityVariants", skip_serializing_if = "Option::is_none")]
    quality_variants: Option<Vec<luedd_media::quality::QualityOption>>,
    #[serde(rename = "previewDataUrl", skip_serializing_if = "Option::is_none")]
    preview_data_url: Option<String>,
    #[serde(rename = "previewKind", skip_serializing_if = "Option::is_none")]
    preview_kind: Option<String>,
    /// Why a `/preview` produced nothing — surfaced in the panel so an
    /// undecodable / blocked source isn't just a silent missing thumbnail.
    #[serde(rename = "previewError", skip_serializing_if = "Option::is_none")]
    preview_error: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct VideoListItem {
    id: String,
    text: String,
    info: String,
    url: String,
    #[serde(rename = "pageUrl")]
    page_url: Option<String>,
    #[serde(rename = "isImage")]
    is_image: bool,
    /// Coarse media kind for the panel's type filter ("video" for page
    /// detections); `None` = let the client infer it from the URL.
    #[serde(rename = "kind", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(rename = "provider")]
    provider: String,
}

const DEFAULT_MEDIA_EXTS: &[&str] = &[
    ".M3U8", ".MPD", ".MP4", ".M4V", ".M4A", ".WEBM", ".MKV", ".MOV", ".AVI", ".FLV", ".TS", ".MP3", ".AAC", ".WAV",
    ".OGG", ".FLAC", ".PDF", ".JPG", ".JPEG", ".PNG", ".GIF", ".WEBP", ".BMP", ".SVG",
];

const DEFAULT_MEDIA_TYPES: &[&str] = &[
    "video/",
    "audio/",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "application/dash+xml",
    "application/vnd.ms-sstr+xml",
    "application/pdf",
    "image/",
];

/// Hosts that only ever serve UI chrome / thumbnails / avatars — never a real
/// download. Filtered out of detections so the panel isn't flooded (e.g. a
/// YouTube playlist auto-advancing spews a `hqdefault.jpg` per video).
const DEFAULT_BLOCKED_HOSTS: &[&str] = &[
    "ytimg.com",
    "ggpht.com",
    "gstatic.com",
    "googleusercontent.com",
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
    "googlesyndication.com",
    "fonts.googleapis.com",
    "cdn.jsdelivr.net",
];

/// Whether the browser extension should keep feeding detections. Toggled from
/// the detection window (`POST /monitoring`); the extension reads it off every
/// sync response and stops sending when it's false.
static MONITORING: AtomicBool = AtomicBool::new(true);

fn default_sync_response(video_list: Vec<VideoListItem>) -> SyncResponse {
    sync_response(video_list, None)
}

fn sync_response(video_list: Vec<VideoListItem>, new_detection: Option<VideoListItem>) -> SyncResponse {
    SyncResponse {
        enabled: MONITORING.load(Ordering::Relaxed),
        file_exts: Vec::new(),
        blocked_hosts: DEFAULT_BLOCKED_HOSTS.iter().map(|s| s.to_string()).collect(),
        tabs_watcher: Vec::new(),
        video_list,
        request_file_exts: DEFAULT_MEDIA_EXTS.iter().map(|s| s.to_string()).collect(),
        matching_hosts: Vec::new(),
        media_types: DEFAULT_MEDIA_TYPES.iter().map(|s| s.to_string()).collect(),
        page_hosts: Vec::new(),
        server_build: String::new(),
        providers: Vec::new(),
        new_detection,
        vid_queued: None,
        quality_variants: None,
        preview_data_url: None,
        preview_kind: None,
        preview_error: None,
    }
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
    url: String,
    filename: Option<String>,
    #[serde(rename = "mimeType")]
    #[allow(dead_code)]
    mime_type: Option<String>,
    #[serde(default, rename = "requestHeaders")]
    request_headers: HashMap<String, Vec<String>>,
    cookie: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MediaRequest {
    url: String,
    file: Option<String>,
    #[serde(rename = "tabUrl")]
    tab_url: Option<String>,
    #[serde(default, rename = "requestHeaders")]
    request_headers: HashMap<String, Vec<String>>,
    #[serde(default, rename = "responseHeaders")]
    response_headers: HashMap<String, Vec<String>>,
    cookie: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PreviewRequest {
    vid: String,
}

#[derive(Debug, Deserialize)]
struct VidRequest {
    vid: String,
    #[serde(default)]
    quality: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProbeQualityRequest {
    vid: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    registry: Arc<luedd_core::backend::BackendRegistry>,
    instagram: Arc<InstagramBackend>,
    ig_library: Arc<IgLibraryStore>,
    ytdlp: Arc<YtdlpBackend>,
    yt_library: Arc<YtLibraryStore>,
    config: ServerConfig,
    listener: std::net::TcpListener,
) -> anyhow::Result<()> {
    // Restore the cookie pool from last run's cache, if any (JSON array; a bare
    // single-line cookie from the old format is still accepted).
    let seeded_cookies: Vec<String> = match &config.ig_cookie_cache {
        Some(p) => tokio::fs::read_to_string(p)
            .await
            .ok()
            .map(|s| {
                serde_json::from_str::<Vec<String>>(&s)
                    .unwrap_or_else(|_| s.lines().map(str::to_string).collect())
            })
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.trim().to_string())
            .filter(|c| c.contains("sessionid="))
            .collect(),
        None => Vec::new(),
    };

    let state = Arc::new(AppState {
        store,
        manager,
        registry,
        instagram,
        ig_library,
        ytdlp,
        yt_library,
        yt_resolving: AtomicBool::new(false),
        ig_cookies: Mutex::new(seeded_cookies),
        config,
        detected: Mutex::new(Vec::new()),
        next_id: AtomicU64::new(1),
        preview_cache: Mutex::new(HashMap::new()),
        quality_cache: Mutex::new(HashMap::new()),
        ffmpeg_slots: Arc::new(tokio::sync::Semaphore::new(6)),
    });

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/sync", get(sync))
        .route("/download", post(download))
        .route("/media", post(media))
        .route("/page", post(page))
        .route("/tab-update", post(ignored))
        .route("/vid", post(vid))
        .route("/probe-quality", post(probe_quality))
        .route("/preview", post(preview))
        .route("/clear", post(clear))
        .route("/monitoring", post(set_monitoring))
        .route("/focus-main", get(focus_main))
        .route("/ig/profiles", get(ig_profiles))
        .route("/ig/profile", post(ig_profile))
        .route("/ig/posts", post(ig_posts))
        .route("/ig/highlights", post(ig_highlights))
        .route("/ig/story", post(ig_story))
        .route("/ig/highlight", post(ig_highlight))
        .route("/ig/post", post(ig_post))
        .route("/ig/queue", post(ig_queue))
        .route("/ig/cookie", post(ig_cookie))
        .route("/ig/img", get(ig_img))
        .route("/yt/channels", get(yt_channels))
        .route("/yt/channel", post(yt_channel))
        .route("/yt/video", post(yt_video))
        .route("/yt/queue", post(yt_queue))
        .route("/library/counts", get(library_counts))
        .layer(cors)
        .with_state(state);

    tracing::info!(addr = ?listener.local_addr().ok(), "luedd-ipc server listening");
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn to_video_list_item(
    id: &str,
    url: &str,
    tab_url: Option<&str>,
    page_title: Option<&str>,
    page_url: Option<&str>,
    is_image: bool,
    is_page: bool,
    provider: &str,
    kind: Option<&str>,
) -> VideoListItem {
    let text = if is_page {
        page_label(url, page_title, provider)
    } else {
        tab_url.or(page_title).unwrap_or(url).to_string()
    };
    // For a page detection the secondary line is just the site — a URL-derived
    // "filename" like "watch" is noise (and misread as the title before the real
    // <title> settles).
    let info = if is_page {
        page_title.map(str::to_string).unwrap_or_default()
    } else {
        luedd_core::naming::suggest_filename(page_title, url, None)
    };
    VideoListItem {
        id: id.to_string(),
        text,
        info,
        url: url.to_string(),
        page_url: page_url.map(str::to_string),
        is_image,
        kind: kind
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .or_else(|| is_page.then(|| "video".to_string())),
        provider: provider.to_string(),
    }
}

/// Panel item `kind` ("video" / "hls" / "dash" / "audio" / "image" / "doc")
/// from a response Content-Type — so a stream URL with no file extension
/// (`…/api/stream?t=…` served as `application/vnd.apple.mpegurl`) still shows a
/// video icon and sorts under the right type filter.
fn kind_from_content_type(ct: Option<&str>) -> Option<&'static str> {
    let ct = ct?.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    Some(match ct.as_str() {
        _ if ct.starts_with("image/") => "image",
        _ if ct.contains("mpegurl") => "hls",
        _ if ct.contains("dash+xml") || ct.contains("ms-sstr+xml") => "dash",
        _ if ct.starts_with("video/") => "video",
        _ if ct.starts_with("audio/") => "audio",
        _ if ct.contains("pdf") => "doc",
        _ => return None,
    })
}

/// A friendly one-line label for a *page* detection (a yt-dlp watch page, an
/// Instagram post…), derived from the URL — the raw URL is still on the row's
/// detail view.
fn page_label(url: &str, page_title: Option<&str>, provider: &str) -> String {
    if provider == "Lüdd-Insta" {
        if let Some(rest) = url.split("instagram.com/").nth(1) {
            let segs: Vec<&str> = rest.split(['/', '?', '#']).filter(|s| !s.is_empty()).collect();
            match segs.as_slice() {
                ["p", code, ..] => return format!("Post · {code}"),
                ["reel" | "reels", code, ..] => return format!("Reel · {code}"),
                ["tv", code, ..] => return format!("IGTV · {code}"),
                ["stories", "highlights", ..] => return "Highlight".to_string(),
                ["stories", user, ..] => return format!("@{user} · story"),
                [user] => return format!("@{user}"),
                _ => {}
            }
        }
    }
    // Other page hosts (yt-dlp): the page title if the extension captured one,
    // trimmed of the site-name suffix; otherwise the URL.
    if let Some(t) = page_title.map(str::trim).filter(|s| !s.is_empty()) {
        for sep in [" - YouTube", " | ", " • ", " - "] {
            if let Some((head, _)) = t.split_once(sep) {
                if head.len() >= 4 {
                    return head.to_string();
                }
            }
        }
        return t.to_string();
    }
    url.to_string()
}

/// First value for a header name (case-insensitive) from an extension-captured
/// header dict.
fn first_header<'a>(headers: &'a HashMap<String, Vec<String>>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.first())
        .map(String::as_str)
}

const IMAGE_URL_EXTS: &[&str] = &["jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "avif"];

fn url_looks_like_image(url: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    IMAGE_URL_EXTS.contains(&ext.as_str())
}

fn looks_like_image(content_type: Option<&str>, url: &str) -> bool {
    content_type.map(|ct| ct.trim().to_ascii_lowercase().starts_with("image/")).unwrap_or(false)
        || url_looks_like_image(url)
}

async fn video_list(state: &AppState) -> Vec<VideoListItem> {
    state
        .detected
        .lock()
        .await
        .iter()
        .map(|m| {
            to_video_list_item(
                &m.id,
                &m.url,
                m.tab_url.as_deref(),
                m.page_title.as_deref(),
                m.page_url.as_deref(),
                m.is_image,
                m.is_page,
                &m.provider,
                m.kind_hint.as_deref(),
            )
        })
        .collect()
}

async fn sync(State(state): State<Arc<AppState>>) -> Json<SyncResponse> {
    let mut resp = default_sync_response(video_list(&state).await);
    resp.page_hosts = state.registry.page_hosts();
    resp.server_build = state.config.build_id.clone();
    resp.providers = state.registry.provider_labels();
    Json(resp)
}

#[derive(Debug, Deserialize)]
struct PageRequest {
    url: String,
    title: Option<String>,
    cookie: Option<String>,
}

/// Mirror the extension's `canonicalPageUrl`: drop the fragment, strip tracking
/// / view-state query params (`img_index`, `igsh`, `utm_*`…), trim a trailing
/// slash — so the same post/profile isn't recorded once per URL variant.
/// Identity params like YouTube's `v` are kept.
fn canonical_page_url(raw: &str) -> String {
    let no_frag = raw.split('#').next().unwrap_or(raw);
    let (base, query) = match no_frag.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (no_frag, None),
    };
    let base = base.strip_suffix('/').unwrap_or(base);
    let kept: Vec<&str> = query
        .map(|q| {
            q.split('&')
                .filter(|p| {
                    let k = p.split('=').next().unwrap_or(p).to_ascii_lowercase();
                    !k.starts_with("utm_")
                        && !k.starts_with("__")
                        && !matches!(
                            k.as_str(),
                            "img_index" | "igsh" | "igshid" | "hl" | "si" | "feature"
                                | "fbclid" | "ref_src" | "ref_url" | "source" | "_r"
                        )
                })
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

/// The extension saw the user land on a page whose host a backend claims
/// (a yt-dlp watch page, an Instagram post…). Record it as a detection so it
/// shows in the panel; `/vid` then routes it to the right backend.
async fn page(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    if !MONITORING.load(Ordering::Relaxed) {
        return Json(default_sync_response(video_list(&state).await));
    }
    let mut new_item = None;
    if let Ok(req) = serde_json::from_slice::<PageRequest>(&body) {
        let url = canonical_page_url(&req.url);
        if std::env::var("IG_DEBUG").is_ok() {
            eprintln!("[/page] url={url} title={:?} cookie={}", req.title, req.cookie.is_some());
        }
        let cfg = state.config.settings.get().await.backends;
        let provider = luedd_core::backend::provider_label(state.registry.quick_id(&url, &cfg)).to_string();
        if provider == "Lüdd-Insta" {
            record_ig_catch(&state, &url, req.cookie.as_deref()).await;
        } else if provider == "yt-dlp" {
            record_yt_catch(&state, &url, req.title.clone()).await;
        }
        new_item = ensure_page_detection(&state, &url, req.title, req.cookie).await;
        if let Some(it) = &new_item {
            tracing::info!(url = %url, id = %it.id, provider = %provider, "page detection from browser extension");
        }
    } else {
        tracing::warn!("malformed /page payload from extension");
        if std::env::var("IG_DEBUG").is_ok() {
            eprintln!("[/page] MALFORMED body: {}", String::from_utf8_lossy(&body).chars().take(200).collect::<String>());
        }
    }
    Json(sync_response(video_list(&state).await, new_item))
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// How many browser sessions to keep in the fallback pool.
const IG_COOKIE_MAX: usize = 6;

/// The `sessionid=` value inside a cookie header, if present.
fn sessionid_of(cookie: &str) -> Option<&str> {
    cookie.split(';').map(str::trim).find_map(|kv| kv.strip_prefix("sessionid="))
}

fn push_uniq(out: &mut Vec<String>, c: &str) {
    let c = c.trim();
    if !c.is_empty() && !out.iter().any(|x| x == c) {
        out.push(c.to_string());
    }
}

impl AppState {
    /// Remember an Instagram cookie at the front of the fallback pool (newest
    /// first), deduped by its `sessionid` value, and mirror the pool to the
    /// on-disk cache so it survives a restart. Logged-out cookies (no
    /// `sessionid=`) are ignored.
    async fn remember_ig_cookie(&self, cookie: &str) {
        let cookie = cookie.trim();
        let Some(sid) = sessionid_of(cookie).map(str::to_string) else { return };
        let snapshot = {
            let mut pool = self.ig_cookies.lock().await;
            if pool.first().map(String::as_str) == Some(cookie) {
                return; // already the active one
            }
            pool.retain(|c| sessionid_of(c) != Some(sid.as_str()));
            pool.insert(0, cookie.to_string());
            pool.truncate(IG_COOKIE_MAX);
            pool.clone()
        };
        self.persist_ig_cookies(&snapshot).await;
    }

    /// Move a cookie that just worked to the front of the pool and persist, so a
    /// fallback that succeeded becomes the default for the next call.
    async fn promote_ig_cookie(&self, cookie: &str) {
        let snapshot = {
            let mut pool = self.ig_cookies.lock().await;
            if pool.first().map(String::as_str) == Some(cookie) {
                return;
            }
            pool.retain(|c| c != cookie);
            pool.insert(0, cookie.to_string());
            pool.clone()
        };
        self.persist_ig_cookies(&snapshot).await;
    }

    async fn persist_ig_cookies(&self, pool: &[String]) {
        let Some(path) = &self.config.ig_cookie_cache else { return };
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let json = serde_json::to_vec_pretty(pool).unwrap_or_default();
        if let Err(e) = tokio::fs::write(path, json).await {
            tracing::warn!(error = %e, "failed to cache Instagram cookies");
        }
    }

    /// Ordered cookie candidates for an `/ig/*` call: the per-request one first,
    /// then every remembered browser session (newest first), then the Settings
    /// fallback.
    async fn ig_candidates(&self, per_req: Option<&str>) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(c) = per_req {
            push_uniq(&mut out, c);
        }
        for c in self.ig_cookies.lock().await.iter() {
            push_uniq(&mut out, c);
        }
        if let Some(c) = self.config.settings.get().await.backends.instagram.session_cookie.clone() {
            push_uniq(&mut out, &c);
        }
        out
    }

    /// Best single cookie for callers that don't need fallback.
    async fn ig_cookie(&self, per_req: Option<&str>) -> Option<String> {
        self.ig_candidates(per_req).await.into_iter().next()
    }

    /// Run `f` with each cookie candidate until one returns `Ok`; the winning
    /// cookie is promoted to the front of the pool. With no candidates, `f` runs
    /// once with `None`. Returns `(winning_cookie, result)` — `result` is the
    /// last error when every candidate failed.
    async fn ig_try<T, F, Fut>(
        &self,
        per_req: Option<&str>,
        mut f: F,
    ) -> (Option<String>, anyhow::Result<T>)
    where
        F: FnMut(Option<String>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let cands = self.ig_candidates(per_req).await;
        if cands.is_empty() {
            return (None, f(None).await);
        }
        let mut last_err = None;
        for (i, ck) in cands.iter().enumerate() {
            match f(Some(ck.clone())).await {
                Ok(v) => {
                    if i > 0 {
                        self.promote_ig_cookie(ck).await;
                    }
                    return (Some(ck.clone()), Ok(v));
                }
                Err(e) => last_err = Some(e),
            }
        }
        (None, Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all Instagram sessions failed"))))
    }
}

/// Classify a canonical Instagram page URL into `(account_from_url, IgCaught)`.
/// `account` is `None` for `/p/…` and `/stories/highlights/…` (no username in
/// the path) — those get a background owner resolve.
fn ig_caught_of(url: &str) -> Option<(Option<String>, IgCaught)> {
    let rest = url.split("instagram.com/").nth(1)?;
    let segs: Vec<&str> = rest.split(['/', '?', '#']).filter(|s| !s.is_empty()).collect();
    let mk = |account: Option<String>, kind: &str, key: String| {
        Some((account, IgCaught { kind: kind.to_string(), key, url: url.to_string(), seen: unix_now() }))
    };
    match segs.as_slice() {
        ["p", code, ..] => mk(None, "post", code.to_string()),
        ["reel" | "reels", code, ..] => mk(None, "reel", code.to_string()),
        ["tv", code, ..] => mk(None, "igtv", code.to_string()),
        ["stories", "highlights", id, ..] => mk(None, "highlight", id.to_string()),
        ["stories", user, ..] => mk(Some(user.to_string()), "story", String::new()),
        [user] => mk(Some(user.to_string()), "profile", String::new()),
        _ => None,
    }
}

/// Record a page-URL detection into `state.detected` (deduped) so it shows in
/// the detection panel. Mirrors the inner block of `page()`. Returns the new
/// `VideoListItem` when one was actually added.
async fn ensure_page_detection(
    state: &Arc<AppState>,
    url: &str,
    title: Option<String>,
    cookie: Option<String>,
) -> Option<VideoListItem> {
    let cfg = state.config.settings.get().await.backends;
    let provider = luedd_core::backend::provider_label(state.registry.quick_id(url, &cfg)).to_string();
    let mut detected = state.detected.lock().await;
    if let Some(existing) = detected.iter_mut().find(|m| m.url == url) {
        // Same page already offered — but a SPA (YouTube, Instagram) posts the
        // stale/generic title first and re-posts the settled one a beat later.
        // An explicit non-empty title that differs always wins.
        let update = title
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty() && Some(*t) != existing.page_title.as_deref());
        if let Some(t) = update {
            existing.page_title = Some(t.to_string());
            let refreshed = to_video_list_item(
                &existing.id, url, Some(url), Some(t), Some(url), false, true, &provider, None,
            );
            if let Some(tx) = &state.config.on_new_detection {
                let _ = tx.send(refreshed.clone());
            }
            return Some(refreshed);
        }
        return None;
    }
    let id = format!("v{}", state.next_id.fetch_add(1, Ordering::Relaxed));
    let item =
        to_video_list_item(&id, url, Some(url), title.as_deref(), Some(url), false, true, &provider, None);
    if let Some(tx) = &state.config.on_new_detection {
        let _ = tx.send(item.clone());
    }
    detected.push(DetectedMedia {
        id,
        url: url.to_string(),
        tab_url: Some(url.to_string()),
        page_title: title,
        page_url: Some(url.to_string()),
        request_headers: HashMap::new(),
        cookie,
        user_agent: None,
        is_image: false,
        is_page: true,
        kind_hint: None,
        provider,
    });
    Some(item)
}

/// Record an Instagram detection into the persistent library + remember its
/// cookie; kick off a background owner resolve for account-less URLs.
async fn record_ig_catch(state: &Arc<AppState>, url: &str, cookie: Option<&str>) {
    if let Some(c) = cookie {
        state.remember_ig_cookie(c).await;
    }
    let Some((account, caught)) = ig_caught_of(url) else { return };
    let _ = state.ig_library.record(account.as_deref(), caught).await;
    if account.is_none() {
        let st = state.clone();
        let u = url.to_string();
        tokio::spawn(async move {
            let cfg = st.config.settings.get().await.backends;
            let cookie = st.ig_cookie(None).await;
            if let Some(owner) = st.instagram.resolve_account(&u, cookie.as_deref(), &cfg.instagram).await {
                let owner = owner.trim_start_matches('@').to_string();
                let _ = st.ig_library.set_account(&u, &owner).await;
                // the viewer polls /ig/profiles, so it picks the move up on its own
            }
        });
    }
}

// ------------------------------------------------------------------------
// yt-dlp viewer — caught channels, mirrors the /ig/* block above
// ------------------------------------------------------------------------

/// `(site, YtCaught)` for a yt-dlp *watchable* page URL. Returns `None` for
/// feed / home / search / bare-profile pages that aren't a single video, so
/// they never enter the library. The `-J` resolver fills in the channel later.
fn yt_caught_of(url: &str, title: Option<String>) -> Option<(String, YtCaught)> {
    let after = url.split("://").nth(1)?;
    let host = after.split(['/', '?', '#']).next()?;
    let site = host.strip_prefix("www.").unwrap_or(host).to_ascii_lowercase();
    let path = after.split_once('/').map(|(_, p)| p).unwrap_or("");
    let segs: Vec<&str> = path.split(['/', '?', '#']).filter(|s| !s.is_empty()).collect();

    // A watchable URL yields a video id; anything else is not a catch.
    let (id, thumbnail): (String, Option<String>) = match site.as_str() {
        "youtu.be" => {
            let v = segs.first().filter(|s| s.len() >= 6)?;
            ((*v).to_string(), Some(format!("https://i.ytimg.com/vi/{v}/hqdefault.jpg")))
        }
        "youtube.com" => {
            let v = if url.contains("watch?") || url.contains("&v=") || url.contains("?v=") {
                url.split("v=").nth(1).map(|s| s.split('&').next().unwrap_or(s))
            } else {
                ["shorts", "live", "embed", "clip"]
                    .iter()
                    .find_map(|p| (segs.first() == Some(p)).then(|| segs.get(1).copied()).flatten())
            };
            let v = v.filter(|s| s.len() >= 6)?;
            (v.to_string(), Some(format!("https://i.ytimg.com/vi/{v}/hqdefault.jpg")))
        }
        "twitch.tv" => match segs.as_slice() {
            ["videos", id, ..] if id.chars().all(|c| c.is_ascii_digit()) => (format!("v{id}"), None),
            [_ch, "clip", slug, ..] => ((*slug).to_string(), None),
            _ => return None,
        },
        "twitter.com" | "x.com" => match segs.as_slice() {
            [_user, "status", id, ..] if id.chars().all(|c| c.is_ascii_digit()) => {
                ((*id).to_string(), None)
            }
            _ => return None,
        },
        "vimeo.com" => {
            let id = segs.first().filter(|s| s.chars().all(|c| c.is_ascii_digit()))?;
            ((*id).to_string(), None)
        }
        "tiktok.com" => match segs.as_slice() {
            [_user, "video", id, ..] => ((*id).to_string(), None),
            _ => return None,
        },
        "reddit.com" => match segs.as_slice() {
            ["r", _sub, "comments", id, ..] => ((*id).to_string(), None),
            _ => return None,
        },
        // dailymotion / bilibili / soundcloud / facebook: accept a 2+-segment
        // path (a real permalink), reject the bare host / a 1-segment profile.
        _ if segs.len() >= 2 => (slug(url), None),
        _ => return None,
    };

    // Canonical URL, so the same video caught via different query params
    // (`&list=…&index=2` vs `&index=3`, `?si=…`, tracking) is one entry and
    // re-downloads don't drag in a whole autoplay playlist.
    let canon = match site.as_str() {
        "youtube.com" | "youtu.be" => match segs.first().copied() {
            Some(k @ ("shorts" | "live" | "embed" | "clip")) => {
                format!("https://www.youtube.com/{k}/{id}")
            }
            _ => format!("https://www.youtube.com/watch?v={id}"),
        },
        _ => {
            let b = url.split(['?', '#']).next().unwrap_or(url);
            b.strip_suffix('/').unwrap_or(b).to_string()
        }
    };

    Some((
        site,
        YtCaught {
            id,
            url: canon,
            title: title.unwrap_or_default(),
            thumbnail,
            duration: None,
            uploader: None,
            upload_date: None,
            view_count: None,
            resolved: false,
            seen: unix_now(),
        },
    ))
}

fn slug(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).take(24).collect()
}

/// Record a caught yt-dlp video (channel unknown → the UNRESOLVED bucket).
/// Does NOT fork `yt-dlp -J` — resolution is lazy, kicked by `/yt/channels`.
async fn record_yt_catch(state: &Arc<AppState>, url: &str, title: Option<String>) {
    let Some((site, caught)) = yt_caught_of(url, title) else { return };
    let _ = state.yt_library.record(luedd_core::yt_library::UNRESOLVED, &site, caught).await;
}

/// Walk the UNRESOLVED bucket, one `yt-dlp -J` at a time with a gap, moving each
/// caught video under its real channel. Idempotent — guarded by `yt_resolving`.
fn spawn_yt_resolver(state: Arc<AppState>) {
    if state.yt_resolving.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async move {
        loop {
            let next = {
                let lib = state.yt_library.snapshot().await;
                lib.channels
                    .get(luedd_core::yt_library::UNRESOLVED)
                    .and_then(|c| c.caught.first().map(|x| x.url.clone()))
            };
            let Some(url) = next else { break };
            let cfg = state.config.settings.get().await.backends;
            match state.ytdlp.probe_meta(&url, &cfg, None).await {
                Ok(m) => {
                    let key = m
                        .channel_id
                        .clone()
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| slug(m.channel_url.as_deref().unwrap_or(&url)));
                    let r = YtResolve {
                        channel_key: key,
                        channel_name: m.channel.clone().unwrap_or_default(),
                        channel_url: m.channel_url.clone(),
                        avatar_url: None,
                        video_id: m.id.clone(),
                        title: m.title.clone(),
                        thumbnail: m.thumbnail.clone(),
                        duration: m.duration,
                        uploader: m.uploader.clone(),
                        upload_date: m.upload_date.clone(),
                        view_count: m.view_count,
                    };
                    let _ = state.yt_library.resolve(&url, &r).await;

                    // Fetch the channel avatar once, if this channel still lacks one.
                    if let Some(chan_url) = m.channel_url.clone().filter(|s| !s.is_empty()) {
                        let key = m
                            .channel_id
                            .clone()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| slug(&chan_url));
                        let need_avatar = state
                            .yt_library
                            .snapshot()
                            .await
                            .channels
                            .get(&key)
                            .is_some_and(|c| c.avatar_url.is_none());
                        if need_avatar {
                            if let Some(av) = state.ytdlp.channel_avatar(&chan_url, &cfg).await {
                                let _ = state.yt_library.set_channel_avatar(&key, &av).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    // Not a real video page (private / deleted / a feed or profile
                    // page). Drop it so the loop doesn't spin forever.
                    tracing::warn!(%url, error = %e, "yt-dlp -J resolve failed; forgetting");
                    let _ = state.yt_library.forget(&url).await;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
        state.yt_resolving.store(false, Ordering::SeqCst);
    });
}

#[derive(Debug, Deserialize)]
struct YtKeyReq {
    key: String,
}

#[derive(Debug, Deserialize)]
struct YtUrlReq {
    url: String,
}

#[derive(Debug, Deserialize)]
struct YtQueueReq {
    url: String,
    #[serde(default)]
    quality: Option<String>,
    #[serde(default)]
    subs: Option<String>,
    #[serde(default)]
    thumbnail: bool,
    #[serde(default)]
    chapters: bool,
}

/// Caught-channels list for the yt-dlp viewer home. Also kicks the lazy resolver.
async fn yt_channels(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let lib = state.yt_library.snapshot().await;
    let mut channels = Vec::new();
    let mut unresolved = 0usize;
    for (key, ch) in &lib.channels {
        if key == luedd_core::yt_library::UNRESOLVED {
            unresolved = ch.caught.len();
            continue;
        }
        channels.push(serde_json::json!({
            "key": ch.key,
            "name": if ch.name.is_empty() { ch.key.clone() } else { ch.name.clone() },
            "site": ch.site,
            "avatar": ch.avatar_url,
            "channel_url": ch.channel_url,
            "caught_count": ch.caught.len(),
            "last_seen": ch.last_seen,
        }));
    }
    channels.sort_by(|a, b| b["last_seen"].as_i64().unwrap_or(0).cmp(&a["last_seen"].as_i64().unwrap_or(0)));
    if unresolved > 0 {
        spawn_yt_resolver(state.clone());
    }
    Json(serde_json::json!({ "channels": channels, "unresolved": unresolved }))
}

/// One channel's caught videos, from the library (no network).
async fn yt_channel(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<YtKeyReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let lib = state.yt_library.snapshot().await;
    let Some(ch) = lib.channels.get(&req.key) else {
        return Json(serde_json::json!({ "error": "unknown channel" }));
    };
    let mut videos: Vec<_> = ch
        .caught
        .iter()
        .map(|c| {
            serde_json::json!({
                "id": c.id, "url": c.url, "title": c.title, "thumbnail": c.thumbnail,
                "duration": c.duration, "upload_date": c.upload_date, "view_count": c.view_count,
                "uploader": c.uploader, "resolved": c.resolved,
            })
        })
        .collect();
    videos.reverse(); // newest catch first
    Json(serde_json::json!({
        "channel": {
            "key": ch.key, "name": if ch.name.is_empty() { ch.key.clone() } else { ch.name.clone() },
            "site": ch.site, "avatar": ch.avatar_url, "channel_url": ch.channel_url,
        },
        "videos": videos,
    }))
}

/// Full metadata + format list for one video — forks `yt-dlp -J` on demand.
async fn yt_video(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<YtUrlReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let meta = match state.ytdlp.probe_meta(&req.url, &cfg, None).await {
        Ok(m) => m,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })),
    };
    let ctx = RequestContext::default();
    let formats = state
        .ytdlp
        .probe_qualities(&probe_req(req.url.clone(), ctx, cfg))
        .await
        .unwrap_or_default();
    Json(serde_json::json!({ "meta": meta, "formats": formats }))
}

/// Queue a yt-dlp download with the chosen quality + extras.
async fn yt_queue(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<YtQueueReq>(&body) else {
        return Json(serde_json::json!({ "queued": false }));
    };
    let mut extras: std::collections::BTreeMap<String, String> = Default::default();
    if let Some(s) = req.subs.filter(|s| !s.trim().is_empty()) {
        extras.insert("subs".into(), s);
    }
    if req.thumbnail {
        extras.insert("thumbnail".into(), "1".into());
    }
    if req.chapters {
        extras.insert("chapters".into(), "1".into());
    }
    {
        let st = state.clone();
        let u = req.url.clone();
        tokio::spawn(async move { record_yt_catch(&st, &u, None).await });
    }
    let id =
        queue_url(&state, req.url, None, None, HashMap::new(), None, req.quality, extras, None).await;
    Json(serde_json::json!({ "queued": id.is_some() }))
}

/// Caught-library sizes for the main window's header badges.
async fn library_counts(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let ig = state
        .ig_library
        .snapshot()
        .await
        .accounts
        .keys()
        .filter(|k| *k != UNRESOLVED)
        .count();
    let yt = state
        .yt_library
        .snapshot()
        .await
        .channels
        .keys()
        .filter(|k| *k != luedd_core::yt_library::UNRESOLVED)
        .count();
    Json(serde_json::json!({ "instagram": ig, "ytdlp": yt }))
}

#[derive(Debug, Deserialize)]
struct IgUserReq {
    username: String,
}

#[derive(Debug, Deserialize)]
struct IgPostsReq {
    username: String,
    after: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IgIdReq {
    id: String,
}

#[derive(Debug, Deserialize)]
struct IgQueueReq {
    url: String,
}

/// The caught-accounts list for the profile viewer's home screen.
async fn ig_profiles(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let lib = state.ig_library.snapshot().await;
    let mut accounts = Vec::new();
    let mut unresolved = 0usize;
    for (key, acct) in &lib.accounts {
        if key == UNRESOLVED {
            unresolved = acct.caught.len();
            continue;
        }
        let mut kinds: Vec<String> = acct.caught.iter().map(|c| c.kind.clone()).collect();
        kinds.sort();
        kinds.dedup();
        // "caught" = actual content (posts / reels / highlights / a live story),
        // not a bare profile visit — that only put the account in this list.
        let caught_count = acct.caught.iter().filter(|c| c.kind != "profile").count();
        accounts.push(serde_json::json!({
            "username": acct.username,
            "caught_count": caught_count,
            "kinds": kinds,
            "last_seen": acct.last_seen,
            "avatar": acct.avatar_url,
        }));
    }
    accounts.sort_by(|a, b| b["last_seen"].as_i64().unwrap_or(0).cmp(&a["last_seen"].as_i64().unwrap_or(0)));
    Json(serde_json::json!({ "accounts": accounts, "unresolved": unresolved }))
}

async fn ig_profile(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgUserReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let header = {
        let inst = state.instagram.clone();
        let igc = cfg.instagram.clone();
        let user = req.username.clone();
        let (_ck, res) = state
            .ig_try(None, move |ck| {
                let (inst, igc, user) = (inst.clone(), igc.clone(), user.clone());
                async move {
                    let h = inst.profile_header(&user, ck.as_deref(), &igc).await;
                    if h.complete || !h.profile_pic_url.is_empty() {
                        Ok(h)
                    } else {
                        anyhow::bail!("web_profile_info blocked for this session")
                    }
                }
            })
            .await;
        match res {
            Ok(h) => h,
            // Every session was blocked (IG throttling). Don't burn another
            // request — hand back a username-only header; the viewer shows its
            // "rate limited" note.
            Err(_) => ProfileHeader {
                username: req.username.clone(),
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
        }
    };
    if !header.profile_pic_url.is_empty() {
        let (st, user, pic) = (state.clone(), req.username.clone(), header.profile_pic_url.clone());
        tokio::spawn(async move { st.ig_library.set_avatar(&user, &pic).await.ok(); });
    }
    // `story_items` is a second IG round-trip — keep it off the critical path.
    // The viewer always fetches `/ig/story` separately and hides the row if empty.
    let has_story = true;
    let caught = state
        .ig_library
        .snapshot()
        .await
        .accounts
        .get(&req.username.to_ascii_lowercase())
        .map(|a| a.caught.clone())
        .unwrap_or_default();
    // Whether we HAVE a session at all — not whether this fetch happened to
    // succeed. A blocked/throttled request must not read as "you're logged out".
    let has_cookie = state.ig_candidates(None).await.iter().any(|c| c.contains("sessionid="));
    Json(serde_json::json!({ "header": header, "has_story": has_story, "caught": caught, "cookie": has_cookie }))
}

/// The extension pushes the current instagram.com cookie here (on connect and
/// periodically), so `/ig/*` calls have a session even before any IG page
/// detection this run. Kept in memory and mirrored to an on-disk cache so it
/// also survives a restart.
async fn ig_cookie(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    #[derive(serde::Deserialize)]
    struct Req {
        cookie: Option<String>,
    }
    let ok = match serde_json::from_slice::<Req>(&body).ok().and_then(|r| r.cookie) {
        Some(c) if c.contains("sessionid=") => {
            state.remember_ig_cookie(&c).await;
            true
        }
        _ => false,
    };
    Json(serde_json::json!({ "ok": ok }))
}

async fn ig_posts(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgPostsReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let inst = state.instagram.clone();
    let igc = cfg.instagram.clone();
    let (user, after) = (req.username.clone(), req.after.clone());
    let (_ck, res) = state
        .ig_try(None, move |ck| {
            let (inst, igc, user, after) = (inst.clone(), igc.clone(), user.clone(), after.clone());
            async move { inst.profile_posts(&user, ck.as_deref(), &igc, after.as_deref()).await }
        })
        .await;
    match res {
        Ok((posts, next)) => Json(serde_json::json!({ "posts": posts, "next": next })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn ig_highlights(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgUserReq>(&body) else {
        return Json(serde_json::json!({ "highlights": [] }));
    };
    let cfg = state.config.settings.get().await.backends;
    let cookie = state.ig_cookie(None).await;
    let hl = state.instagram.highlights_tray(&req.username, cookie.as_deref(), &cfg.instagram).await;
    Json(serde_json::json!({ "highlights": hl }))
}

async fn ig_story(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgUserReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let inst = state.instagram.clone();
    let igc = cfg.instagram.clone();
    let user = req.username.clone();
    let (_ck, res) = state
        .ig_try(None, move |ck| {
            let (inst, igc, user) = (inst.clone(), igc.clone(), user.clone());
            async move { inst.story_items(&user, ck.as_deref(), &igc).await }
        })
        .await;
    match res {
        Ok(items) => Json(serde_json::json!({ "items": items })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn ig_highlight(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgIdReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let inst = state.instagram.clone();
    let igc = cfg.instagram.clone();
    let id = req.id.clone();
    let (_ck, res) = state
        .ig_try(None, move |ck| {
            let (inst, igc, id) = (inst.clone(), igc.clone(), id.clone());
            async move { inst.highlight_items(&id, ck.as_deref(), &igc).await }
        })
        .await;
    match res {
        Ok(items) => Json(serde_json::json!({ "items": items })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn ig_post(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgIdReq>(&body) else {
        return Json(serde_json::json!({ "error": "bad request" }));
    };
    let cfg = state.config.settings.get().await.backends;
    let inst = state.instagram.clone();
    let igc = cfg.instagram.clone();
    let id = req.id.clone();
    let (cookie, res) = state
        .ig_try(None, move |ck| {
            let (inst, igc, id) = (inst.clone(), igc.clone(), id.clone());
            async move { inst.post_media(&id, ck.as_deref(), &igc).await }
        })
        .await;
    match res {
        Ok((items, caption)) => {
            // a post pulled up in the viewer is also a catch: record it in the
            // background so the response isn't held up by the library write / detection
            let st = state.clone();
            let id = req.id.clone();
            tokio::spawn(async move {
                let url = format!("https://www.instagram.com/p/{id}");
                record_ig_catch(&st, &url, cookie.as_deref()).await;
                ensure_page_detection(&st, &url, None, cookie).await;
            });
            Json(serde_json::json!({ "items": items, "caption": caption }))
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn ig_queue(State(state): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let Ok(req) = serde_json::from_slice::<IgQueueReq>(&body) else {
        return Json(serde_json::json!({ "queued": false }));
    };
    let cookie = state.ig_cookie(None).await;
    // an instagram.com page URL queued from the viewer is also a catch — record
    // it in the background so the queue call returns as soon as the entry is added
    if req.url.contains("instagram.com/") && !req.url.contains("cdninstagram.com") {
        let st = state.clone();
        let canon = canonical_page_url(&req.url);
        let ck = cookie.clone();
        tokio::spawn(async move {
            record_ig_catch(&st, &canon, ck.as_deref()).await;
            ensure_page_detection(&st, &canon, None, ck).await;
        });
    }
    let id = queue_url(&state, req.url, None, None, HashMap::new(), cookie, None, Default::default(), None).await;
    Json(serde_json::json!({ "queued": id.is_some() }))
}

/// Proxy an Instagram CDN image through the server so the viewer webview gets it
/// with a proper `Referer` (IG's CDN hotlink-blocks a `tauri://` origin).
async fn ig_img(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(url) = q.get("u") else {
        return (axum::http::StatusCode::BAD_REQUEST, "missing u").into_response();
    };
    if !url.contains("cdninstagram.com") && !url.contains("fbcdn.net") {
        return (axum::http::StatusCode::FORBIDDEN, "not an IG CDN url").into_response();
    }
    let cookie = state.ig_cookie(None).await;
    let opts = luedd_net::RequestOptions {
        headers: HashMap::from([
            ("User-Agent".to_string(), FALLBACK_UA.to_string()),
            ("Referer".to_string(), "https://www.instagram.com/".to_string()),
        ]),
        cookies: cookie,
        byte_range: None,
    };
    match state.manager.http_client().get_response(url, &opts).await {
        Ok(resp) if resp.status().is_success() => {
            let ct = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();
            match resp.bytes().await {
                Ok(b) => ([(axum::http::header::CONTENT_TYPE, ct), (axum::http::header::CACHE_CONTROL, "public, max-age=3600".to_string())], b).into_response(),
                Err(_) => (axum::http::StatusCode::BAD_GATEWAY, "read failed").into_response(),
            }
        }
        _ => (axum::http::StatusCode::BAD_GATEWAY, "fetch failed").into_response(),
    }
}

async fn focus_main(State(state): State<Arc<AppState>>) -> &'static str {
    if let Some(tx) = &state.config.on_focus_request {
        let _ = tx.send(());
    }
    "ok"
}

async fn ignored(State(state): State<Arc<AppState>>, _body: Bytes) -> Json<SyncResponse> {
    Json(default_sync_response(video_list(&state).await))
}

async fn download(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let body: DownloadRequest = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "malformed /download payload from extension");
            return Json(default_sync_response(video_list(&state).await));
        }
    };
    let _ = queue_url(&state, body.url, body.filename, None, flatten_headers(body.request_headers, body.user_agent), body.cookie, None, Default::default(), None).await;
    Json(default_sync_response(video_list(&state).await))
}

async fn media(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    if !MONITORING.load(Ordering::Relaxed) {
        return Json(default_sync_response(video_list(&state).await));
    }
    let mut new_item = None;
    match serde_json::from_slice::<MediaRequest>(&body) {
        Ok(req) => {
            // Media requests fired by a page a plugin owns (Instagram thumbnails,
            // a yt-dlp watch page's HLS/ad requests) are noise — the page
            // detection is the download. Drop them; keep the page instead.
            let page_hosts = state.registry.page_hosts();
            if req.tab_url.as_deref().map(|u| is_page_host(u, &page_hosts)).unwrap_or(false) {
                return Json(default_sync_response(video_list(&state).await));
            }
            let cfg = state.config.settings.get().await.backends;
            let provider = luedd_core::backend::provider_label(state.registry.quick_id(&req.url, &cfg)).to_string();
            let mut detected = state.detected.lock().await;
            if !detected.iter().any(|m| m.url == req.url) {
                let id = format!("v{}", state.next_id.fetch_add(1, Ordering::Relaxed));
                tracing::info!(url = %req.url, %id, "detected media from browser extension");
                let ct = first_header(&req.response_headers, "content-type");
                let is_image = looks_like_image(ct, &req.url);
                let kind_hint = kind_from_content_type(ct).map(str::to_string);
                new_item = Some(to_video_list_item(
                    &id,
                    &req.url,
                    req.tab_url.as_deref(),
                    req.file.as_deref(),
                    req.tab_url.as_deref(),
                    is_image,
                    false,
                    &provider,
                    kind_hint.as_deref(),
                ));
                if let Some(item) = &new_item {
                    if let Some(tx) = &state.config.on_new_detection {
                        let _ = tx.send(item.clone());
                    }
                }
                detected.push(DetectedMedia {
                    id,
                    url: req.url,
                    tab_url: req.tab_url.clone().or_else(|| req.file.clone()),
                    page_title: req.file,
                    page_url: req.tab_url,
                    request_headers: req.request_headers,
                    cookie: req.cookie,
                    user_agent: req.user_agent,
                    is_image,
                    is_page: false,
                    kind_hint,
                    provider,
                });
            }
        }
        Err(e) => tracing::warn!(error = %e, "malformed /media payload from extension"),
    }
    Json(sync_response(video_list(&state).await, new_item))
}

async fn vid(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let queued = if let Ok(req) = serde_json::from_slice::<VidRequest>(&body) {
        let found = {
            let detected = state.detected.lock().await;
            detected.iter().find(|m| m.id == req.vid).cloned()
        };
        match found {
            Some(media) => {
                if media.provider == "yt-dlp" && media.is_page {
                    let st = state.clone();
                    let (u, t) = (media.url.clone(), media.page_title.clone());
                    tokio::spawn(async move { record_yt_catch(&st, &u, t).await });
                }
                let flat_headers = flatten_headers(media.request_headers.clone(), media.user_agent.clone());
                let cached = state
                    .preview_cache
                    .lock()
                    .await
                    .get(&media.url)
                    .cloned()
                    .flatten()
                    .map(|(data_url, kind)| (data_url, kind.to_string()));
                let had_preview = cached.is_some();
                let entry_id = queue_url(
                    &state,
                    media.url.clone(),
                    None,
                    media.page_title.clone(),
                    flat_headers,
                    media.cookie.clone(),
                    req.quality,
                    Default::default(),
                    cached,
                )
                .await;

                // Nothing cached yet (the panel row was never scrolled into
                // view): generate the thumbnail in the background and attach it
                // to the entry, so the list still gets a preview before the
                // file lands on disk.
                if !had_preview {
                    if let Some(entry_id) = entry_id {
                        let state = state.clone();
                        tokio::spawn(async move {
                            let ctx = preview_ctx_for(&media);
                            let client = state.manager.http_client();
                            if let Ok((data_url, kind)) = build_preview(&state, &client, &media.url, &ctx).await {
                                state
                                    .preview_cache
                                    .lock()
                                    .await
                                    .insert(media.url.clone(), Some((data_url.clone(), kind)));
                                state
                                    .store
                                    .update_entry(&entry_id, |e| {
                                        e.preview = Some(data_url);
                                        e.preview_kind = Some(kind.to_string());
                                    })
                                    .await
                                    .ok();
                            }
                        });
                    }
                }
                Some(true)
            }
            None => {
                tracing::warn!(vid = %req.vid, "popup clicked an unknown/expired video id");
                Some(false)
            }
        }
    } else {
        None
    };
    let mut resp = default_sync_response(video_list(&state).await);
    resp.vid_queued = queued;
    Json(resp)
}

async fn probe_quality(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let Ok(req) = serde_json::from_slice::<ProbeQualityRequest>(&body) else {
        return Json(default_sync_response(video_list(&state).await));
    };
    let found = {
        let detected = state.detected.lock().await;
        detected.iter().find(|m| m.id == req.vid).cloned()
    };
    let Some(media) = found else {
        tracing::warn!(vid = %req.vid, "probe-quality for an unknown/expired video id");
        return Json(default_sync_response(video_list(&state).await));
    };

    if let Some(cached) = state.quality_cache.lock().await.get(&media.url).cloned() {
        let mut resp = default_sync_response(video_list(&state).await);
        resp.quality_variants = Some(cached);
        return Json(resp);
    }

    let ctx = RequestContext {
        headers: flatten_headers(media.request_headers, media.user_agent),
        cookie: media.cookie,
    };

    // Progressive files have no alternate renditions to choose - don't spend a
    // request finding that out.
    let variants = if PROGRESSIVE_EXTS.iter().any(|e| url_ends_with(&media.url, e)) {
        Vec::new()
    } else {
        // Whichever backend claims the URL probes it. A streaming URL carrying
        // auth tokens or no ".m3u8"/".mpd" suffix is sniffed inside `resolve`.
        let cfg = state.config.settings.get().await.backends;
        let backend = state.registry.resolve(&media.url, &ctx, &cfg).await;
        backend.probe_qualities(&probe_req(media.url.clone(), ctx.clone(), cfg)).await.unwrap_or_default()
    };

    state.quality_cache.lock().await.insert(media.url.clone(), variants.clone());
    let mut resp = default_sync_response(video_list(&state).await);
    resp.quality_variants = Some(variants);
    Json(resp)
}

/// Cap on bytes pulled for an image thumbnail.
const MAX_IMAGE_BYTES: u64 = 8 * 1024 * 1024;
/// Cap on bytes pulled for a PDF preview (the client renders page 1 in a small
/// embed; whole-file is what a data: URL needs).
const MAX_PDF_BYTES: u64 = 16 * 1024 * 1024;
/// Cap on an ffmpeg-produced thumbnail frame.
const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
/// How much of a progressive video/audio file to pull (via the Chrome-emulated
/// client) and pipe into ffmpeg for a frame. Enough for a faststart mp4's moov
/// atom + a keyframe; if the moov is at the tail, we fall back to letting ffmpeg
/// fetch the URL itself. Kept small so the preview lands in a couple of seconds.
const MAX_FFMPEG_INPUT_BYTES: usize = 3 * 1024 * 1024;
/// When the small prefix can't be decoded (non-faststart mp4, or the host
/// rejects ffmpeg's own fetch), pull up to this much of the file through the
/// emulated client and decode from bytes. Bounded so a handful of concurrent
/// previews can't blow memory.
const MAX_PROGRESSIVE_FETCH_BYTES: usize = 64 * 1024 * 1024;
/// Hard ceiling on a single ffmpeg thumbnail attempt.
const FFMPEG_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(12);
/// Sent to ffmpeg when the detection captured no User-Agent, so CDNs that reject
/// ffmpeg's default `Lavf/*` UA still serve the segment/frame.
const FALLBACK_UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";

const PROGRESSIVE_EXTS: &[&str] =
    &["mp4", "m4v", "m4a", "webm", "mkv", "mov", "ts", "mp3", "aac", "flac", "ogg", "opus", "wav", "avi", "flv"];

/// Generate an inline preview for a detected item, using the detection's
/// captured headers/cookies so auth- or hotlink-protected media still renders.
///
/// - images (svg/png/jpg/gif/webp/bmp/ico/avif/…) -> fetched as a data URL
/// - PDFs -> fetched (capped) as a data URL for the client to embed
/// - anything ffmpeg can demux (mp4/mkv/webm/mov/ts/avi/flv, HLS .m3u8, DASH
///   .mpd, and audio with cover art) -> a single decoded frame as a JPEG
///
/// Results (including "nothing to show") are memoised per URL.
async fn preview(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let Ok(req) = serde_json::from_slice::<PreviewRequest>(&body) else {
        return Json(default_sync_response(video_list(&state).await));
    };
    let found = {
        let detected = state.detected.lock().await;
        detected.iter().find(|m| m.id == req.vid).cloned()
    };
    let Some(media) = found else {
        tracing::warn!(vid = %req.vid, "preview for an unknown/expired video id");
        return Json(default_sync_response(video_list(&state).await));
    };

    if let Some(cached) = state.preview_cache.lock().await.get(&media.url).cloned() {
        return Json(preview_response(video_list(&state).await, cached));
    }

    let client = state.manager.http_client();
    let ctx = preview_ctx_for(&media);

    // Page detections (a yt-dlp watch page etc) point at an HTML page, not a
    // media file - fetching that would just 413. Ask the backend for a
    // thumbnail image URL instead (yt-dlp `-J` provides one).
    if is_page_host(&media.url, &state.registry.page_hosts()) {
        let cfg = state.config.settings.get().await.backends;
        let backend = state.registry.resolve(&media.url, &ctx, &cfg).await;
        let result = match backend.thumbnail(&probe_req(media.url.clone(), ctx.clone(), cfg)).await {
            Ok(Some((thumb_url, square))) => fetch_image_data_url(&client, &thumb_url)
                .await
                .map(|(url, _)| (url, if square { "square" } else { "image" })),
            _ => None,
        };
        state.preview_cache.lock().await.insert(media.url.clone(), result.clone());
        return Json(preview_response(video_list(&state).await, result));
    }

    match build_preview(&state, &client, &media.url, &ctx).await {
        Ok(p) => {
            state.preview_cache.lock().await.insert(media.url.clone(), Some(p.clone()));
            Json(preview_response(video_list(&state).await, Some(p)))
        }
        Err(reason) => {
            tracing::warn!(url = %media.url, %reason, "no preview available");
            // Don't cache the miss — a later scroll-into-view retries.
            let mut resp = default_sync_response(video_list(&state).await);
            resp.preview_error = Some(reason);
            Json(resp)
        }
    }
}

fn is_page_host(url: &str, page_hosts: &[String]) -> bool {
    let Some(host) = url
        .split_once("://")
        .and_then(|(_, r)| r.split(['/', '?', '#']).next())
        .map(|h| h.split(':').next().unwrap_or(h).to_ascii_lowercase())
    else {
        return false;
    };
    page_hosts.iter().any(|h| host == *h || host.ends_with(&format!(".{h}")))
}

/// Build a request context for fetching a detection's media/preview. Firefox's
/// webRequest can't expose Referer/User-Agent (no `extraHeaders`), and private
/// windows hide cookies - so the captured headers are often bare and hosts
/// behind Cloudflare reject the fetch. Backfill a plausible Referer from the
/// page the media was detected on, and a browser UA.
fn preview_ctx_for(media: &DetectedMedia) -> RequestContext {
    let mut headers = flatten_headers(media.request_headers.clone(), media.user_agent.clone());
    if !headers.keys().any(|k| k.eq_ignore_ascii_case("referer")) {
        if let Some(page) = media.page_url.as_deref().or(media.tab_url.as_deref()) {
            let referer = page
                .split_once("://")
                .and_then(|(scheme, rest)| rest.split('/').next().map(|host| format!("{scheme}://{host}/")))
                .unwrap_or_else(|| page.to_string());
            headers.insert("Referer".to_string(), referer);
        }
    }
    if !headers.keys().any(|k| k.eq_ignore_ascii_case("user-agent")) {
        headers.insert("User-Agent".to_string(), FALLBACK_UA.to_string());
    }
    RequestContext { headers, cookie: media.cookie.clone() }
}

fn preview_response(video_list: Vec<VideoListItem>, preview: Option<Preview>) -> SyncResponse {
    let mut resp = default_sync_response(video_list);
    if let Some((data_url, kind)) = preview {
        resp.preview_data_url = Some(data_url);
        resp.preview_kind = Some(kind.to_string());
    }
    resp
}

/// Fetch a plain image URL (a yt-dlp thumbnail, a public CDN image) and return
/// it as a data URL. No cookies/headers - these are public.
async fn fetch_image_data_url(client: &HttpClient, url: &str) -> Option<Preview> {
    let ctx = RequestContext::default();
    let mut resp = client.get_response(url, &ctx.to_options(Some((0, MAX_IMAGE_BYTES)))).await.ok()?;
    if !(resp.status().is_success() || resp.status().as_u16() == 206) {
        return None;
    }
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_ascii_lowercase())
        .filter(|s| s.starts_with("image/"))
        .unwrap_or_else(|| "image/jpeg".to_string());
    let bytes = read_capped(&mut resp, MAX_IMAGE_BYTES as usize).await?;
    if bytes.is_empty() {
        return None;
    }
    Some((data_url(&ct, &bytes), "image"))
}

async fn build_preview(
    state: &AppState,
    client: &HttpClient,
    url: &str,
    ctx: &RequestContext,
) -> Result<Preview, String> {
    // One request through the Chrome-emulated client. Its headers classify the
    // target; its body feeds whichever path applies.
    let mut response = match client.get_response(url, &ctx.to_options(Some((0, MAX_PDF_BYTES)))).await {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!(%url, error = %e, "preview: initial request failed");
            None
        }
    };
    let mut fetch_err = response
        .is_none()
        .then(|| "couldn't reach the media host".to_string());
    let mut content_type = String::new();
    if let Some(r) = response.as_ref() {
        let status = r.status();
        let ct = r
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or(s).trim().to_ascii_lowercase())
            .unwrap_or_default();
        tracing::debug!(%url, %status, content_type = %ct, "preview: got response");
        if status.is_success() || status.as_u16() == 206 {
            content_type = ct;
        } else {
            tracing::warn!(%url, %status, "preview: non-success response");
            fetch_err = Some(format!("media host returned HTTP {}", status.as_u16()));
            response = None;
        }
    }

    // Images and PDFs render directly as a data URL.
    if let Some(resp) = response.as_mut() {
        if content_type.starts_with("image/") {
            return read_capped(resp, MAX_IMAGE_BYTES as usize)
                .await
                .filter(|b| !b.is_empty())
                .map(|b| (data_url(&content_type, &b), "image"))
                .ok_or_else(|| "image body was empty".to_string());
        }
        if is_pdf(&content_type, url) {
            return read_capped(resp, MAX_PDF_BYTES as usize)
                .await
                .filter(|b| !b.is_empty())
                .map(|b| (data_url("application/pdf", &b), "pdf"))
                .ok_or_else(|| "PDF body was empty".to_string());
        }
    }

    let _slot = state.ffmpeg_slots.acquire().await.map_err(|_| "preview worker unavailable".to_string())?;

    // Progressive video/audio: pull a prefix ourselves (past the CDN) and decode
    // it. If that yields nothing (e.g. a non-faststart mp4 with a trailing moov),
    // fall through to a bounded full fetch, then to ffmpeg opening the URL.
    let progressive = content_type.starts_with("video/")
        || content_type.starts_with("audio/")
        || PROGRESSIVE_EXTS.iter().any(|e| url_ends_with(url, e));
    if progressive {
        if let Some(resp) = response.as_mut() {
            match read_capped(resp, MAX_FFMPEG_INPUT_BYTES).await {
                Some(head) if !head.is_empty() => {
                    if let Some(frame) = ffmpeg_frame_from_bytes(&head).await {
                        return Ok((data_url("image/jpeg", &frame), "video"));
                    }
                    tracing::warn!(%url, prefix_bytes = head.len(), "preview: ffmpeg could not decode the fetched prefix");
                }
                other => tracing::warn!(%url, empty = other.is_none(), "preview: could not read a video prefix"),
            }
        } else {
            tracing::warn!(%url, "preview: progressive by url but no usable response to read");
        }
    }
    drop(response);

    // Prefix decode failed (non-faststart mp4 with a tail moov) or the host
    // rejects ffmpeg's own fetch (Cloudflare TLS fingerprint). Pull the file —
    // bounded — through the emulated client and decode from bytes.
    if progressive {
        match client.get_response(url, &ctx.to_options(None)).await {
            Ok(mut full) => {
                let st = full.status();
                if st.is_success() {
                    match read_capped(&mut full, MAX_PROGRESSIVE_FETCH_BYTES).await {
                        Some(bytes) if !bytes.is_empty() => {
                            if let Some(frame) = ffmpeg_frame_from_bytes(&bytes).await {
                                return Ok((data_url("image/jpeg", &frame), "video"));
                            }
                            tracing::warn!(%url, bytes = bytes.len(), "preview: ffmpeg could not decode the full fetch");
                            fetch_err = Some(format!(
                                "downloaded {} MB but ffmpeg found no decodable video",
                                bytes.len() / 1_048_576
                            ));
                        }
                        _ => fetch_err = Some("media body was empty".to_string()),
                    }
                } else {
                    fetch_err = Some(format!("media host returned HTTP {}", st.as_u16()));
                }
            }
            Err(e) => {
                tracing::warn!(%url, error = %e, "preview: full fetch failed");
                fetch_err = Some("couldn't download the media for a thumbnail".to_string());
            }
        }
    }

    // HLS: fetch the playlist and one segment ourselves, through the
    // Chrome-emulated client, so Cloudflare-gated hosts (which reject ffmpeg's
    // TLS fingerprint even with the cf_clearance cookie) still preview. Match on
    // the Content-Type too — a stream URL is often `…/api/stream?t=…` with no
    // `.m3u8` suffix.
    if url_ends_with(url, "m3u8") || content_type.contains("mpegurl") {
        if let Some(seg) = hls_first_segment_bytes(client, ctx, url).await {
            if let Some(frame) = ffmpeg_frame_from_bytes(&seg).await {
                return Ok((data_url("image/jpeg", &frame), "video"));
            }
            fetch_err = Some("could not decode the first HLS segment".to_string());
        } else {
            fetch_err = Some("could not fetch the HLS playlist / a segment".to_string());
        }
    }

    // Last resort (DASH, or HLS the above couldn't handle): ffmpeg opens the URL
    // directly with the detection's headers.
    match ffmpeg_frame_from_url(url, ctx).await {
        Some(frame) => Ok((data_url("image/jpeg", &frame), "video")),
        None => Err(fetch_err.unwrap_or_else(|| "no decodable video frame in this source".to_string())),
    }
}

/// Pull an HLS playlist and its first segment (init segment prepended for fMP4,
/// AES-128 decrypted, TS-disguise stripped) via the emulated client, ready to
/// pipe into ffmpeg.
async fn hls_first_segment_bytes(client: &HttpClient, ctx: &RequestContext, url: &str) -> Option<Vec<u8>> {
    use luedd_media::hls::{parse_master_playlist, parse_media_playlist};

    let text = client.get_text(url, &ctx.to_options(None)).await.ok()?;

    // Resolve a media playlist: follow the lowest-bitrate variant of a master.
    let (media_text, media_url) = if text.contains("#EXT-X-STREAM-INF") {
        let lines: Vec<&str> = text.lines().collect();
        let containers = parse_master_playlist(&lines, url).ok()?;
        let best = containers.iter().min_by_key(|c| {
            c.attributes.get("BANDWIDTH").and_then(|b| b.parse::<u64>().ok()).unwrap_or(u64::MAX)
        })?;
        let variant = best.video_playlist.as_ref().or(best.audio_playlist.as_ref())?.clone();
        let vt = client.get_text(variant.as_str(), &ctx.to_options(None)).await.ok()?;
        (vt, variant.to_string())
    } else {
        (text, url.to_string())
    };

    let lines: Vec<&str> = media_text.lines().collect();
    let playlist = parse_media_playlist(&lines, &media_url).ok()?;

    // Key (if the stream is AES-128 encrypted).
    let mut key_cache: Option<Vec<u8>> = None;
    if let Some(seg) = playlist.media_segments.iter().find(|s| s.key_url.is_some()) {
        let key_url = seg.key_url.clone()?;
        key_cache = client.get_bytes(key_url.as_str(), &ctx.to_options(None)).await.ok();
    }

    let fetch_one = |segment: &luedd_media::hls::HlsMediaSegment| {
        let opts = ctx.to_options(segment.byte_range);
        let url = segment.url.to_string();
        let key = segment.key_url.as_ref().and_then(|_| key_cache.clone());
        let iv = segment.iv;
        async move {
            let raw = client.get_bytes(&url, &opts).await.ok()?;
            let payload = match (key, iv) {
                (Some(k), Some(iv)) => luedd_media::crypto::decrypt_segment(&raw, &k, &iv).ok()?,
                _ => luedd_media::disguise::extract_ts_payload(&raw).to_vec(),
            };
            Some(payload)
        }
    };

    let mut out = Vec::new();
    let mut media_taken = 0;
    for segment in playlist.media_segments.iter() {
        if segment.is_init_segment {
            out.extend_from_slice(&fetch_one(segment).await?);
            continue;
        }
        out.extend_from_slice(&fetch_one(segment).await?);
        media_taken += 1;
        if media_taken >= 1 || out.len() >= MAX_FFMPEG_INPUT_BYTES {
            break;
        }
    }
    (!out.is_empty()).then_some(out)
}

fn data_url(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes))
}

fn is_pdf(content_type: &str, url: &str) -> bool {
    content_type == "application/pdf"
        || (matches!(content_type, "application/octet-stream" | "") && url_ends_with(url, "pdf"))
}

/// Read at most `cap` bytes of a response body. Returns None on a read error.
async fn read_capped(response: &mut luedd_net::wreq::Response, cap: usize) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    while buf.len() < cap {
        match response.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(_) => return None,
        }
    }
    buf.truncate(cap);
    Some(buf)
}

fn url_ends_with(url: &str, ext: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.rsplit('.').next().map(|e| e.eq_ignore_ascii_case(ext)).unwrap_or(false)
}

/// Decode one frame from bytes already in hand (a media-file prefix or an HLS
/// segment). The bytes are written to a temp file rather than piped, because a
/// pipe isn't seekable and many mp4s keep their moov atom at the tail.
async fn ffmpeg_frame_from_bytes(input: &[u8]) -> Option<Vec<u8>> {
    let tmp = std::env::temp_dir().join(format!(
        "luedd-prev-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
    ));
    if tokio::fs::write(&tmp, input).await.is_err() {
        return None;
    }

    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.kill_on_drop(true);
    cmd.args([
        "-hide_banner", "-loglevel", "error", "-nostdin",
        "-i", &tmp.to_string_lossy(),
        "-frames:v", "1",
        "-vf", "scale='min(400,iw)':-2",
        "-f", "image2", "-vcodec", "mjpeg", "-q:v", "4",
        "pipe:1",
    ]);
    cmd.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let out = match cmd.spawn() {
        Ok(child) => tokio::time::timeout(FFMPEG_TIMEOUT, child.wait_with_output()).await.ok().and_then(|r| r.ok()),
        Err(_) => None,
    };
    tokio::fs::remove_file(&tmp).await.ok();

    let out = out?;
    (out.status.success() && !out.stdout.is_empty() && out.stdout.len() <= MAX_FRAME_BYTES).then_some(out.stdout)
}

/// Let ffmpeg open the URL itself (HLS/DASH, or media the prefix decode missed),
/// replaying the detection's headers/cookies plus a browser User-Agent.
async fn ffmpeg_frame_from_url(url: &str, ctx: &RequestContext) -> Option<Vec<u8>> {
    let mut header_blob = String::new();
    let mut have_ua = false;
    for (k, v) in &ctx.headers {
        let lk = k.to_ascii_lowercase();
        if matches!(lk.as_str(), "host" | "accept-encoding" | "connection" | "content-length") {
            continue;
        }
        if lk == "user-agent" {
            have_ua = true;
        }
        header_blob.push_str(k);
        header_blob.push_str(": ");
        header_blob.push_str(v);
        header_blob.push_str("\r\n");
    }
    if let Some(cookie) = &ctx.cookie {
        header_blob.push_str("Cookie: ");
        header_blob.push_str(cookie);
        header_blob.push_str("\r\n");
    }

    // For a playlist (HLS/DASH) seeking means fetching every segment up to the
    // offset, so take frame 0 in a single attempt. For a plain media URL, one
    // seeked attempt (nicer frame) then an unseeked fallback.
    let is_playlist = url_ends_with(url, "m3u8") || url_ends_with(url, "mpd");
    let seeks: &[&str] = if is_playlist { &["0"] } else { &["1", "0"] };

    for seek in seeks {
        let mut cmd = tokio::process::Command::new("ffmpeg");
        cmd.kill_on_drop(true);
        cmd.args(["-hide_banner", "-loglevel", "error", "-nostdin", "-rw_timeout", "10000000"]);
        if is_playlist {
            cmd.args(["-probesize", "800000", "-analyzeduration", "2000000"]);
        }
        if !have_ua {
            cmd.args(["-user_agent", FALLBACK_UA]);
        }
        if !header_blob.is_empty() {
            cmd.args(["-headers", &header_blob]);
        }
        cmd.args([
            "-ss", seek,
            "-i", url,
            "-frames:v", "1",
            "-vf", "scale='min(400,iw)':-2",
            "-f", "image2",
            "-vcodec", "mjpeg",
            "-q:v", "4",
            "pipe:1",
        ]);
        cmd.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null());
        #[cfg(windows)]
        {
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        let Ok(child) = cmd.spawn() else { return None };
        let out = match tokio::time::timeout(FFMPEG_TIMEOUT, child.wait_with_output()).await {
            Ok(Ok(out)) => out,
            _ => continue,
        };
        if out.status.success() && !out.stdout.is_empty() && out.stdout.len() <= MAX_FRAME_BYTES {
            return Some(out.stdout);
        }
    }
    None
}

/// Minimal [`DownloadReq`] for a probe-only call (no dest, no progress).
fn probe_req(
    url: String,
    ctx: RequestContext,
    config: luedd_core::backend::BackendConfig,
) -> luedd_core::backend::DownloadReq {
    luedd_core::backend::DownloadReq {
        url,
        dest_dir: std::path::PathBuf::from("."),
        filename_hint: None,
        ctx,
        quality: None,
        extras: Default::default(),
        concurrency: 1,
        config,
    }
}

async fn set_monitoring(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    #[derive(Deserialize)]
    struct Req {
        enabled: bool,
    }
    if let Ok(req) = serde_json::from_slice::<Req>(&body) {
        MONITORING.store(req.enabled, Ordering::Relaxed);
        tracing::info!(enabled = req.enabled, "browser monitoring toggled from the detection window");
    }
    Json(default_sync_response(video_list(&state).await))
}

async fn clear(State(state): State<Arc<AppState>>, _body: Bytes) -> Json<SyncResponse> {
    state.detected.lock().await.clear();
    state.preview_cache.lock().await.clear();
    state.quality_cache.lock().await.clear();
    Json(default_sync_response(video_list(&state).await))
}

#[allow(clippy::too_many_arguments)]
async fn queue_url(
    state: &AppState,
    url: String,
    filename_hint: Option<String>,
    title_hint: Option<String>,
    headers: HashMap<String, String>,
    cookie: Option<String>,
    quality: Option<String>,
    extras: std::collections::BTreeMap<String, String>,
    preview: Option<(String, String)>,
) -> Option<String> {
    let ctx = RequestContext { headers: headers.clone(), cookie: cookie.clone() };

    let settings = state.config.settings.get().await;
    let backend = state.registry.resolve(&url, &ctx, &settings.backends).await;
    let backend_id = backend.id().to_string();
    let kind = luedd_core::backend::kind_for_backend_id(&backend_id);

    // Only the plain-HTTP backend needs a network round-trip to sniff the real
    // file extension. A plugin backend (Instagram, yt-dlp — all of which map to
    // the Http *kind*) names its own outputs, so skip the sniff: it was adding
    // 1-2 s to every viewer "download" click.
    let detected_ext = if backend_id == "http"
        && matches!(DownloadKind::guess_from_url(&url), DownloadKind::Http)
    {
        luedd_core::naming::resolve_real_extension(&state.manager.http_client(), &url, &ctx).await
    } else {
        None
    };

    let filename = filename_hint.filter(|f| !f.is_empty()).unwrap_or_else(|| {
        luedd_core::naming::suggest_filename(title_hint.as_deref(), &url, detected_ext.as_deref())
    });
    let dest = luedd_core::naming::dest_path(&settings.download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let dest = luedd_core::jobs::sanitize_dest_for_kind(&dest, kind);

    // Cheap pre-run grouping metadata for the per-plugin views.
    let meta = {
        let req = luedd_core::backend::DownloadReq {
            url: url.clone(),
            dest_dir: dest.parent().map(std::path::PathBuf::from).unwrap_or_default(),
            filename_hint: dest.file_name().map(|s| s.to_string_lossy().into_owned()),
            ctx: ctx.clone(),
            quality: quality.clone(),
            extras: extras.clone(),
            concurrency: 1,
            config: settings.backends.clone(),
        };
        backend.describe(&req).await
    };

    // A backend that wants its own output folder (Instagram): re-root the dest
    // into it and remember it on the entry so a delete removes the whole folder.
    let dest = if let Some(dir) = &meta.out_dir {
        tokio::fs::create_dir_all(dir).await.ok();
        match dest.file_name() {
            Some(name) => dir.join(name),
            None => dest,
        }
    } else {
        dest
    };

    tracing::info!(%url, dest = %dest.display(), backend = %backend_id, "queued download from browser extension");
    let mut entry = DownloadEntry::new(url, dest, kind)
        .with_backend_id(backend_id)
        .with_request_context(headers, cookie)
        .with_quality(quality)
        .with_extras(extras)
        .with_preview(preview);
    entry.author = meta.author;
    entry.title = meta.title.or(title_hint);
    entry.media_class = meta.media_class;
    entry.out_dir = meta.out_dir;
    let id = entry.id.clone();
    if let Err(e) = state.store.add_entry(entry).await {
        tracing::warn!(error = %e, "failed to persist download entry from extension");
        return None;
    }

    let manager = state.manager.clone();
    let spawn_id = id.clone();
    tokio::spawn(async move {
        if let Err(e) = manager.run_entry_now(&spawn_id).await {
            tracing::warn!(error = %e, id = %spawn_id, "immediate run of extension-queued download failed to start");
        }
    });
    Some(id)
}

fn flatten_headers(headers: HashMap<String, Vec<String>>, user_agent: Option<String>) -> HashMap<String, String> {
    let mut flat: HashMap<String, String> = headers.into_iter().map(|(k, v)| (k, v.join(", "))).collect();
    if let Some(ua) = user_agent {
        flat.retain(|k, _| !k.eq_ignore_ascii_case("user-agent"));
        flat.insert("User-Agent".to_string(), ua);
    }
    flat
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_headers_prefers_captured_user_agent_over_request_headers_capture() {
        let mut headers = HashMap::new();
        headers.insert("Referer".to_string(), vec!["https://page.example/".to_string()]);
        headers.insert("user-agent".to_string(), vec!["stale-from-webrequest".to_string()]);

        let flat = flatten_headers(headers, Some("real-navigator-user-agent".to_string()));

        let uas: Vec<_> = flat.iter().filter(|(k, _)| k.eq_ignore_ascii_case("user-agent")).collect();
        assert_eq!(uas.len(), 1, "must not keep both a webRequest-captured and a navigator-supplied User-Agent");
        assert_eq!(flat.get("User-Agent").unwrap(), "real-navigator-user-agent");
        assert_eq!(flat.get("Referer").unwrap(), "https://page.example/");
    }

    #[test]
    fn video_list_item_shows_a_filename_not_the_raw_link_in_info() {
        let item = to_video_list_item(
            "v1",
            "https://cdn.example/abc123.mp4?token=secret",
            None,
            Some("My Cool Video"),
            Some("https://site.example/watch?v=1"),
            false,
            false,
            "Lüdd",
            None,
        );
        assert_eq!(item.info, "My Cool Video.mp4");
        assert_eq!(item.url, "https://cdn.example/abc123.mp4?token=secret");
        assert_eq!(item.page_url.as_deref(), Some("https://site.example/watch?v=1"));
    }

    #[test]
    fn video_list_item_falls_back_to_url_derived_name_without_a_title() {
        let item =
            to_video_list_item("v1", "https://cdn.example/movie.mkv", None, None, None, false, false, "Lüdd", None);
        assert_eq!(item.info, "movie.mkv");
        assert_eq!(item.page_url, None);
    }

    #[test]
    fn kind_from_content_type_recognises_extensionless_streams() {
        assert_eq!(kind_from_content_type(Some("application/vnd.apple.mpegurl")), Some("hls"));
        assert_eq!(kind_from_content_type(Some("application/x-mpegURL; charset=utf-8")), Some("hls"));
        assert_eq!(kind_from_content_type(Some("application/dash+xml")), Some("dash"));
        assert_eq!(kind_from_content_type(Some("video/mp4")), Some("video"));
        assert_eq!(kind_from_content_type(Some("audio/mpeg")), Some("audio"));
        assert_eq!(kind_from_content_type(Some("image/webp")), Some("image"));
        assert_eq!(kind_from_content_type(Some("text/html")), None);
        assert_eq!(kind_from_content_type(None), None);
    }

    #[test]
    fn looks_like_image_uses_content_type_then_url_extension() {
        assert!(looks_like_image(Some("image/jpeg"), "https://cdn.example/x"));
        assert!(looks_like_image(Some("IMAGE/PNG; charset=binary"), "https://cdn.example/x"));
        assert!(looks_like_image(None, "https://cdn.example/photo.WEBP?v=2"));
        assert!(!looks_like_image(Some("video/mp4"), "https://cdn.example/clip.mp4"));
        assert!(!looks_like_image(None, "https://cdn.example/stream.m3u8"));
    }

    #[test]
    fn default_media_exts_cover_hls_and_dash_manifests() {
        assert!(DEFAULT_MEDIA_EXTS.contains(&".M3U8"));
        assert!(DEFAULT_MEDIA_EXTS.contains(&".MPD"));
    }

    #[test]
    fn default_media_exts_and_types_cover_documents_and_images() {
        assert!(DEFAULT_MEDIA_EXTS.contains(&".PDF"));
        assert!(DEFAULT_MEDIA_EXTS.contains(&".JPG"));
        assert!(DEFAULT_MEDIA_EXTS.contains(&".PNG"));
        assert!(DEFAULT_MEDIA_TYPES.contains(&"application/pdf"));
        assert!(DEFAULT_MEDIA_TYPES.contains(&"image/"));
    }

    #[test]
    fn yt_caught_of_only_matches_watchable_urls() {
        let id = |u: &str| yt_caught_of(u, None).map(|(_, c)| c.id);

        // watchable
        assert_eq!(id("https://www.youtube.com/watch?v=dQw4w9WgXcQ").as_deref(), Some("dQw4w9WgXcQ"));
        assert_eq!(id("https://youtu.be/dQw4w9WgXcQ").as_deref(), Some("dQw4w9WgXcQ"));
        assert_eq!(id("https://www.youtube.com/shorts/abcdefg").as_deref(), Some("abcdefg"));
        assert_eq!(id("https://www.twitch.tv/videos/123456789").as_deref(), Some("v123456789"));
        assert_eq!(id("https://x.com/someone/status/1790000000000000000").as_deref(), Some("1790000000000000000"));
        assert_eq!(id("https://vimeo.com/76979871").as_deref(), Some("76979871"));

        // NOT watchable
        assert!(yt_caught_of("https://www.youtube.com/feed/subscriptions", None).is_none());
        assert!(yt_caught_of("https://www.youtube.com/", None).is_none());
        assert!(yt_caught_of("https://www.youtube.com/results?search_query=cats", None).is_none());
        assert!(yt_caught_of("https://www.youtube.com/@SomeChannel", None).is_none());
        assert!(yt_caught_of("https://x.com/someone", None).is_none());
        assert!(yt_caught_of("https://www.twitch.tv/someone", None).is_none());
    }

    #[test]
    fn yt_caught_of_canonicalises_youtube_playlist_variants() {
        let u = |s: &str| yt_caught_of(s, None).map(|(_, c)| c.url);
        let want = Some("https://www.youtube.com/watch?v=CUtNDBBLFpI".to_string());
        assert_eq!(u("https://www.youtube.com/watch?v=CUtNDBBLFpI&list=RDx&index=2"), want);
        assert_eq!(u("https://www.youtube.com/watch?v=CUtNDBBLFpI&list=RDx&index=3"), want);
        assert_eq!(u("https://youtu.be/CUtNDBBLFpI?si=abcd"), want);
        assert_eq!(
            u("https://www.youtube.com/shorts/abcdefg?feature=share"),
            Some("https://www.youtube.com/shorts/abcdefg".to_string())
        );
    }
}
