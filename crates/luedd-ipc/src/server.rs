
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

use luedd_core::jobs::DownloadKind;
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
        blocked_hosts: Vec::new(),
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

pub async fn serve(
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    registry: Arc<luedd_core::backend::BackendRegistry>,
    config: ServerConfig,
    listener: std::net::TcpListener,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        store,
        manager,
        registry,
        config,
        detected: Mutex::new(Vec::new()),
        next_id: AtomicU64::new(1),
        preview_cache: Mutex::new(HashMap::new()),
        quality_cache: Mutex::new(HashMap::new()),
        ffmpeg_slots: Arc::new(tokio::sync::Semaphore::new(6)),
    });

    let cors = CorsLayer::new().allow_methods([Method::GET, Method::POST]).allow_origin(Any);

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
) -> VideoListItem {
    let text = tab_url.or(page_title).unwrap_or(url).to_string();
    let info = luedd_core::naming::suggest_filename(page_title, url, None);
    VideoListItem {
        id: id.to_string(),
        text,
        info,
        url: url.to_string(),
        page_url: page_url.map(str::to_string),
        is_image,
        kind: is_page.then(|| "video".to_string()),
        provider: provider.to_string(),
    }
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

/// The extension saw the user land on a page whose host a backend claims
/// (a yt-dlp watch page, an Instagram post…). Record it as a detection so it
/// shows in the panel; `/vid` then routes it to the right backend.
async fn page(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    if !MONITORING.load(Ordering::Relaxed) {
        return Json(default_sync_response(video_list(&state).await));
    }
    let mut new_item = None;
    if let Ok(req) = serde_json::from_slice::<PageRequest>(&body) {
        let cfg = state.config.settings.get().await.backends;
        let provider = luedd_core::backend::provider_label(state.registry.quick_id(&req.url, &cfg)).to_string();
        let mut detected = state.detected.lock().await;
        if !detected.iter().any(|m| m.url == req.url) {
            let id = format!("v{}", state.next_id.fetch_add(1, Ordering::Relaxed));
            tracing::info!(url = %req.url, %id, provider = %provider, "page detection from browser extension");
            let item = to_video_list_item(
                &id,
                &req.url,
                Some(&req.url),
                req.title.as_deref(),
                Some(&req.url),
                false,
                true,
                &provider,
            );
            if let Some(tx) = &state.config.on_new_detection {
                let _ = tx.send(item.clone());
            }
            new_item = Some(item);
            detected.push(DetectedMedia {
                id,
                url: req.url.clone(),
                tab_url: Some(req.url.clone()),
                page_title: req.title,
                page_url: Some(req.url),
                request_headers: HashMap::new(),
                cookie: req.cookie,
                user_agent: None,
                is_image: false,
                is_page: true,
                provider,
            });
        }
    } else {
        tracing::warn!("malformed /page payload from extension");
    }
    Json(sync_response(video_list(&state).await, new_item))
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
    let _ = queue_url(&state, body.url, body.filename, None, flatten_headers(body.request_headers, body.user_agent), body.cookie, None, None).await;
    Json(default_sync_response(video_list(&state).await))
}

async fn media(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    if !MONITORING.load(Ordering::Relaxed) {
        return Json(default_sync_response(video_list(&state).await));
    }
    let mut new_item = None;
    match serde_json::from_slice::<MediaRequest>(&body) {
        Ok(req) => {
            let cfg = state.config.settings.get().await.backends;
            let provider = luedd_core::backend::provider_label(state.registry.quick_id(&req.url, &cfg)).to_string();
            let mut detected = state.detected.lock().await;
            if !detected.iter().any(|m| m.url == req.url) {
                let id = format!("v{}", state.next_id.fetch_add(1, Ordering::Relaxed));
                tracing::info!(url = %req.url, %id, "detected media from browser extension");
                let is_image = looks_like_image(first_header(&req.response_headers, "content-type"), &req.url);
                new_item = Some(to_video_list_item(
                    &id,
                    &req.url,
                    req.tab_url.as_deref(),
                    req.file.as_deref(),
                    req.tab_url.as_deref(),
                    is_image,
                    false,
                    &provider,
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
                            if let Some((data_url, kind)) = build_preview(&state, &client, &media.url, &ctx).await {
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
            Ok(Some(thumb_url)) => fetch_image_data_url(&client, &thumb_url).await,
            _ => None,
        };
        state.preview_cache.lock().await.insert(media.url.clone(), result.clone());
        return Json(preview_response(video_list(&state).await, result));
    }

    let result = build_preview(&state, &client, &media.url, &ctx).await;
    if result.is_none() {
        tracing::warn!(url = %media.url, "no preview available");
    }
    state.preview_cache.lock().await.insert(media.url.clone(), result.clone());
    Json(preview_response(video_list(&state).await, result))
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

async fn build_preview(state: &AppState, client: &HttpClient, url: &str, ctx: &RequestContext) -> Option<Preview> {
    // One request through the Chrome-emulated client. Its headers classify the
    // target; its body feeds whichever path applies.
    let mut response = match client.get_response(url, &ctx.to_options(Some((0, MAX_PDF_BYTES)))).await {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!(%url, error = %e, "preview: initial request failed");
            None
        }
    };
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
            response = None;
        }
    }

    // Images and PDFs render directly as a data URL.
    if let Some(resp) = response.as_mut() {
        if content_type.starts_with("image/") {
            return read_capped(resp, MAX_IMAGE_BYTES as usize)
                .await
                .filter(|b| !b.is_empty())
                .map(|b| (data_url(&content_type, &b), "image"));
        }
        if is_pdf(&content_type, url) {
            return read_capped(resp, MAX_PDF_BYTES as usize)
                .await
                .filter(|b| !b.is_empty())
                .map(|b| (data_url("application/pdf", &b), "pdf"));
        }
    }

    let _slot = state.ffmpeg_slots.acquire().await.ok()?;

    // Progressive video/audio: pull a prefix ourselves (past the CDN) and decode
    // it. If that yields nothing (e.g. a non-faststart mp4 with a trailing moov),
    // fall through to letting ffmpeg fetch the whole URL.
    let progressive = content_type.starts_with("video/")
        || content_type.starts_with("audio/")
        || PROGRESSIVE_EXTS.iter().any(|e| url_ends_with(url, e));
    if progressive {
        if let Some(resp) = response.as_mut() {
            match read_capped(resp, MAX_FFMPEG_INPUT_BYTES).await {
                Some(head) if !head.is_empty() => {
                    if let Some(frame) = ffmpeg_frame_from_bytes(&head).await {
                        return Some((data_url("image/jpeg", &frame), "video"));
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

    // HLS: fetch the playlist and one segment ourselves, through the
    // Chrome-emulated client, so Cloudflare-gated hosts (which reject ffmpeg's
    // TLS fingerprint even with the cf_clearance cookie) still preview.
    if url_ends_with(url, "m3u8") {
        if let Some(seg) = hls_first_segment_bytes(client, ctx, url).await {
            if let Some(frame) = ffmpeg_frame_from_bytes(&seg).await {
                return Some((data_url("image/jpeg", &frame), "video"));
            }
        }
    }

    // Last resort (DASH, or HLS the above couldn't handle): ffmpeg opens the URL
    // directly with the detection's headers.
    let frame = ffmpeg_frame_from_url(url, ctx).await?;
    Some((data_url("image/jpeg", &frame), "video"))
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

async fn queue_url(
    state: &AppState,
    url: String,
    filename_hint: Option<String>,
    title_hint: Option<String>,
    headers: HashMap<String, String>,
    cookie: Option<String>,
    quality: Option<String>,
    preview: Option<(String, String)>,
) -> Option<String> {
    let ctx = RequestContext { headers: headers.clone(), cookie: cookie.clone() };

    let detected_ext = if matches!(DownloadKind::guess_from_url(&url), DownloadKind::Http) {
        luedd_core::naming::resolve_real_extension(&state.manager.http_client(), &url, &ctx).await
    } else {
        None
    };

    let settings = state.config.settings.get().await;
    let backend = state.registry.resolve(&url, &ctx, &settings.backends).await;
    let backend_id = backend.id().to_string();
    let kind = luedd_core::backend::kind_for_backend_id(&backend_id);

    let filename = filename_hint.filter(|f| !f.is_empty()).unwrap_or_else(|| {
        luedd_core::naming::suggest_filename(title_hint.as_deref(), &url, detected_ext.as_deref())
    });
    let dest = luedd_core::naming::dest_path(&settings.download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let dest = luedd_core::jobs::sanitize_dest_for_kind(&dest, kind);

    tracing::info!(%url, dest = %dest.display(), backend = %backend_id, "queued download from browser extension");
    let entry = DownloadEntry::new(url, dest, kind)
        .with_backend_id(backend_id)
        .with_request_context(headers, cookie)
        .with_quality(quality)
        .with_preview(preview);
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
        );
        assert_eq!(item.info, "My Cool Video.mp4");
        assert_eq!(item.url, "https://cdn.example/abc123.mp4?token=secret");
        assert_eq!(item.page_url.as_deref(), Some("https://site.example/watch?v=1"));
    }

    #[test]
    fn video_list_item_falls_back_to_url_derived_name_without_a_title() {
        let item = to_video_list_item("v1", "https://cdn.example/movie.mkv", None, None, None, false, false, "Lüdd");
        assert_eq!(item.info, "movie.mkv");
        assert_eq!(item.page_url, None);
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
}
