//! Local HTTP server the browser extension's `connector.js` talks to
//! (`http://127.0.0.1:8597`), matching its *existing* wire protocol exactly:
//! `GET /sync` (polled every ~1 min via `chrome.alarms`) returns a config/state
//! blob, and `POST /download` (plus `/media`, `/tab-update`, `/vid`, `/clear`)
//! post JSON payloads and expect the same config/state blob back - the
//! extension code (`app.js`/`connector.js`) needed no protocol changes to
//! work against this server, only a backend to talk to.
//!
//! Two things here are load-bearing, not cosmetic: the detection lists in
//! `/sync` (`mediaExts`/`mediaTypes`/...) are what `request-watcher.js` uses to
//! decide whether *any* network request looks like media at all - empty lists
//! mean nothing is ever detected, on any site. And `/media` has to actually
//! remember what gets reported so `/sync`'s `videoList` (shown in the popup)
//! isn't permanently empty.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::Method;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use tidm_core::jobs::DownloadKind;
use tidm_core::queue::{DownloadEntry, DownloadManager, DownloadStore, SettingsStore};
use tidm_net::RequestContext;

/// `download_dir` is read from `settings` fresh on every request rather than
/// cached at server startup - the GUI's Settings panel can change it any time
/// after this server is already running, and a browser-extension download
/// should land wherever the user *currently* has it configured, not wherever
/// it happened to be when the app launched.
pub struct ServerConfig {
    pub settings: Arc<SettingsStore>,
}

/// One piece of media `request-watcher.js` flagged, reported via `POST /media`.
#[derive(Debug, Clone)]
struct DetectedMedia {
    id: String,
    url: String,
    /// Display label fallback: the page address if known, else the page
    /// title - used only for the popup's bold button text (`text` below),
    /// which doesn't distinguish the two. `page_url` is the unambiguous one.
    tab_url: Option<String>,
    /// The page's `document.title` at detection time (`MediaRequest.file`,
    /// despite the name - see its doc comment) - used for XDM-style
    /// title-based filenames rather than the raw CDN URL.
    page_title: Option<String>,
    /// The page's actual address (`tab.url`), strictly - for the popup's
    /// "Details" toggle, which needs the real site link rather than whatever
    /// `tab_url` above fell back to.
    page_url: Option<String>,
    request_headers: HashMap<String, Vec<String>>,
    cookie: Option<String>,
    /// `navigator.userAgent` from the page at detection time - some sites
    /// (Cloudflare-protected ones especially) issue a clearance cookie tied to
    /// the exact User-Agent that solved the challenge; replaying that cookie
    /// with a different UA (our own default) can get the download blocked
    /// even though the cookie itself is valid.
    user_agent: Option<String>,
}

struct AppState {
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    config: ServerConfig,
    detected: Mutex<Vec<DetectedMedia>>,
    next_id: AtomicU64,
}

/// Shape returned by `/sync` and echoed back from every POST endpoint, matching
/// what `app.js`'s `onMessage` expects (`msg.enabled`, `msg.fileExts`, ...).
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
    /// Set only on the specific `/media` response that just added a genuinely
    /// new item (not a duplicate URL already known) - lets `app.js` fire a
    /// system notification exactly once per new detection instead of on every
    /// request the page happens to make, or worse, on every `/sync` poll.
    /// `None`/absent everywhere else; extra fields are otherwise ignored by
    /// `onMessage`, so this needed no protocol version bump.
    #[serde(rename = "newDetection", skip_serializing_if = "Option::is_none")]
    new_detection: Option<VideoListItem>,
    /// Set only on the `/vid` response, so the popup can tell a click actually
    /// queued in the GUI (`Some(true)`) from one that hit an unknown/expired
    /// id (`Some(false)`) - `None`/absent everywhere else.
    #[serde(rename = "vidQueued", skip_serializing_if = "Option::is_none")]
    vid_queued: Option<bool>,
    /// Set only on the `/probe-quality` response. Every response round-trips
    /// this same `SyncResponse` shape (the whole-blob convention documented
    /// at the top of this file) rather than a bespoke response type, since
    /// `connector.js` feeds every response through `App.onMessage` - a
    /// response missing `enabled`/`videoList`/etc. would wipe the extension's
    /// state on every quality probe.
    #[serde(rename = "qualityVariants", skip_serializing_if = "Option::is_none")]
    quality_variants: Option<Vec<tidm_media::quality::QualityOption>>,
}

/// Shape `popup.js`'s `renderList` expects: `listItem.id` becomes a DOM node id
/// (must be a valid HTML id, so it's prefixed rather than a bare UUID/number),
/// `text` is the button label, `info` the subtitle line underneath it (the
/// filename it would be saved as, not the raw source link - a `googlevideo`-
/// style token URL tells the user nothing useful). `url` is the raw source
/// link, kept separately so the popup's "Preview" action can still open it.
#[derive(Debug, Serialize, Clone)]
struct VideoListItem {
    id: String,
    text: String,
    info: String,
    url: String,
    /// The page's actual address, for the popup's "Details" toggle - `None`
    /// when the extension couldn't read the tab's URL.
    #[serde(rename = "pageUrl")]
    page_url: Option<String>,
}

/// Extensions a manifest/segment/progressive download is likely to end in -
/// this is what actually lets `request-watcher.js`'s `isMatchingRequest` flag
/// anything as media; XDM shipped its own list here, this one is a reasonable
/// equivalent covering HLS/DASH manifests plus common progressive containers.
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

fn default_sync_response(video_list: Vec<VideoListItem>) -> SyncResponse {
    sync_response(video_list, None)
}

fn sync_response(video_list: Vec<VideoListItem>, new_detection: Option<VideoListItem>) -> SyncResponse {
    SyncResponse {
        enabled: true,
        file_exts: Vec::new(),
        blocked_hosts: Vec::new(),
        tabs_watcher: Vec::new(),
        video_list,
        request_file_exts: DEFAULT_MEDIA_EXTS.iter().map(|s| s.to_string()).collect(),
        matching_hosts: Vec::new(),
        media_types: DEFAULT_MEDIA_TYPES.iter().map(|s| s.to_string()).collect(),
        new_detection,
        vid_queued: None,
        quality_variants: None,
    }
}

/// Matches the `data` object built in `app.js`'s `onDeterminingFilename`
/// (`connector.postMessage("/download", data)`).
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

/// Matches `RequestWatcher.createRequestData`'s payload, posted to `/media`.
/// Despite the name, `file` is the *page's* `document.title` (passed in as
/// `tab.title` in `createRequestData`), not a filename - kept as-is to match
/// the wire format the extension already sends.
#[derive(Debug, Deserialize)]
struct MediaRequest {
    url: String,
    file: Option<String>,
    #[serde(rename = "tabUrl")]
    tab_url: Option<String>,
    #[serde(default, rename = "requestHeaders")]
    request_headers: HashMap<String, Vec<String>>,
    cookie: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: Option<String>,
}

/// Matches `app.js`'s `postMessage("/vid", { vid })` sent when a popup entry is clicked.
/// `quality` is set only when the user expanded the row's details panel and
/// picked a specific variant there (`popup.js`'s quality picker) - absent for
/// the fast one-click path, which keeps auto-picking the best variant.
#[derive(Debug, Deserialize)]
struct VidRequest {
    vid: String,
    #[serde(default)]
    quality: Option<String>,
}

/// Matches `popup.js`'s `postMessage("/probe-quality", { vid })`, sent when a
/// row's details panel is expanded, to fetch that item's selectable HLS/DASH
/// variants before the user commits to one.
#[derive(Debug, Deserialize)]
struct ProbeQualityRequest {
    vid: String,
}

/// Starts the server and runs until the process exits. Binds to loopback only -
/// this is a local IPC channel, never meant to be reachable off-host.
pub async fn serve(
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    config: ServerConfig,
    port: u16,
) -> anyhow::Result<()> {
    let state =
        Arc::new(AppState { store, manager, config, detected: Mutex::new(Vec::new()), next_id: AtomicU64::new(1) });

    let cors = CorsLayer::new().allow_methods([Method::GET, Method::POST]).allow_origin(Any);

    let app = Router::new()
        .route("/sync", get(sync))
        .route("/download", post(download))
        .route("/media", post(media))
        .route("/tab-update", post(ignored))
        .route("/vid", post(vid))
        .route("/probe-quality", post(probe_quality))
        .route("/clear", post(clear))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!(%addr, "tidm-ipc server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Builds the popup-facing item for one detected/known URL. `info` is a
/// best-guess filename (title + URL, no network round-trip - the real
/// extension-detection request only happens once a download is actually
/// queued) rather than the raw source link, which for most CDNs is an opaque
/// token nobody can read anything useful from.
fn to_video_list_item(
    id: &str,
    url: &str,
    tab_url: Option<&str>,
    page_title: Option<&str>,
    page_url: Option<&str>,
) -> VideoListItem {
    let text = tab_url.or(page_title).unwrap_or(url).to_string();
    let info = tidm_core::naming::suggest_filename(page_title, url, None);
    VideoListItem {
        id: id.to_string(),
        text,
        info,
        url: url.to_string(),
        page_url: page_url.map(str::to_string),
    }
}

async fn video_list(state: &AppState) -> Vec<VideoListItem> {
    state
        .detected
        .lock()
        .await
        .iter()
        .map(|m| to_video_list_item(&m.id, &m.url, m.tab_url.as_deref(), m.page_title.as_deref(), m.page_url.as_deref()))
        .collect()
}

async fn sync(State(state): State<Arc<AppState>>) -> Json<SyncResponse> {
    Json(default_sync_response(video_list(&state).await))
}

async fn ignored(State(state): State<Arc<AppState>>, _body: Bytes) -> Json<SyncResponse> {
    Json(default_sync_response(video_list(&state).await))
}

/// `connector.js`'s `fetch()` never sets a `Content-Type` header, so the
/// browser defaults it to `text/plain` for a JSON string body - `axum::Json`'s
/// extractor requires `application/json` and would reject every real request
/// from the extension, so every POST body below is read as raw bytes and
/// parsed manually rather than via the `Json<T>` extractor.
async fn download(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let body: DownloadRequest = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "malformed /download payload from extension");
            return Json(default_sync_response(video_list(&state).await));
        }
    };
    queue_url(&state, body.url, body.filename, None, flatten_headers(body.request_headers, body.user_agent), body.cookie, None).await;
    Json(default_sync_response(video_list(&state).await))
}

async fn media(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let mut new_item = None;
    match serde_json::from_slice::<MediaRequest>(&body) {
        Ok(req) => {
            let mut detected = state.detected.lock().await;
            if !detected.iter().any(|m| m.url == req.url) {
                let id = format!("v{}", state.next_id.fetch_add(1, Ordering::Relaxed));
                tracing::info!(url = %req.url, %id, "detected media from browser extension");
                new_item = Some(to_video_list_item(
                    &id,
                    &req.url,
                    req.tab_url.as_deref(),
                    req.file.as_deref(),
                    req.tab_url.as_deref(),
                ));
                detected.push(DetectedMedia {
                    id,
                    url: req.url,
                    tab_url: req.tab_url.clone().or_else(|| req.file.clone()),
                    page_title: req.file,
                    page_url: req.tab_url,
                    request_headers: req.request_headers,
                    cookie: req.cookie,
                    user_agent: req.user_agent,
                });
            }
        }
        Err(e) => tracing::warn!(error = %e, "malformed /media payload from extension"),
    }
    Json(sync_response(video_list(&state).await, new_item))
}

/// A popup click on a detected video: look it up by the id `/media` assigned
/// and queue it the same way an explicit `/download` request would be.
async fn vid(State(state): State<Arc<AppState>>, body: Bytes) -> Json<SyncResponse> {
    let queued = if let Ok(req) = serde_json::from_slice::<VidRequest>(&body) {
        let found = {
            let detected = state.detected.lock().await;
            detected.iter().find(|m| m.id == req.vid).cloned()
        };
        match found {
            Some(media) => {
                let headers = flatten_headers(media.request_headers, media.user_agent);
                queue_url(&state, media.url, None, media.page_title, headers, media.cookie, req.quality).await;
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

    let client = state.manager.http_client();
    let ctx = RequestContext {
        headers: flatten_headers(media.request_headers, media.user_agent),
        cookie: media.cookie,
    };
    let guessed_kind = DownloadKind::guess_from_url(&media.url);
    let variants = match guessed_kind {
        DownloadKind::Hls => tidm_media::quality::probe_hls_qualities(&client, &media.url, &ctx).await.unwrap_or_default(),
        DownloadKind::Dash => tidm_media::quality::probe_dash_qualities(&client, &media.url, &ctx).await.unwrap_or_default(),
        // A URL guessed as plain HTTP might still be a disguised manifest
        // (m3u8-guide.txt) - `queue_url` pays for a sniff request to check
        // this before every download, but that cost isn't worth paying just
        // to populate a details panel that's usually plain HTTP anyway; a
        // real HLS/DASH URL under a generic extension just shows no variants
        // here (falls back to auto-best at actual download time, same as
        // today) rather than probing speculatively on every expand.
        DownloadKind::Http => Vec::new(),
    };
    let mut resp = default_sync_response(video_list(&state).await);
    resp.quality_variants = Some(variants);
    Json(resp)
}

async fn clear(State(state): State<Arc<AppState>>, _body: Bytes) -> Json<SyncResponse> {
    state.detected.lock().await.clear();
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
) {
    // Facebook/Instagram post/reel/watch URLs aren't themselves downloadable -
    // they're pages. Resolve to the real direct media URL(s) first (using the
    // page's own captured headers/cookie for private/saved content), then
    // queue each as a plain `Http` download - skips the HLS/DASH-sniffing
    // path below entirely, since a resolved CDN URL is never itself a
    // manifest. A carousel resolves to more than one entry.
    if let Some(site) = tidm_media::social::detect_site(&url) {
        let ctx = RequestContext { headers: headers.clone(), cookie: cookie.clone() };
        match tidm_media::social::extract(site, &state.manager.http_client(), &url, &ctx).await {
            Ok(items) => {
                for item in items {
                    let filename = item
                        .suggested_name
                        .or_else(|| filename_hint.clone())
                        .unwrap_or_else(|| tidm_core::naming::suggest_filename(title_hint.as_deref(), &item.url, None));
                    queue_resolved_http(state, item.url, filename, headers.clone(), cookie.clone(), quality.clone()).await;
                }
            }
            Err(e) => {
                tracing::warn!(%url, ?site, error = %e, "social media extraction failed");
            }
        }
        return;
    }

    let guessed_kind = DownloadKind::guess_from_url(&url);

    // HLS/DASH already get forced to `.mp4` by `sanitize_dest_for_kind` below
    // regardless of what the manifest/segments actually contain, so real-type
    // detection is only useful (and only spends the extra request) for plain
    // HTTP downloads, where the URL's own extension - or lack of one - is the
    // only signal otherwise available.
    let detected_ext = if matches!(guessed_kind, DownloadKind::Http) {
        let ctx = RequestContext { headers: headers.clone(), cookie: cookie.clone() };
        tidm_core::naming::resolve_real_extension(&state.manager.http_client(), &url, &ctx).await
    } else {
        None
    };

    // A guessed-Http URL that actually turned out to be an HLS/DASH manifest
    // (sites disguise these behind generic extensions like `.txt` specifically
    // to dodge naive "is this a video?" detection - `m3u8-guide.txt`) needs to
    // be routed through the real playlist/manifest downloader, not saved
    // as-is: fetching a manifest URL with the plain HTTP downloader would just
    // write the raw manifest text to disk as if it were the video itself.
    let kind = match detected_ext.as_deref() {
        Some("m3u8") => DownloadKind::Hls,
        Some("mpd") => DownloadKind::Dash,
        _ => guessed_kind,
    };

    let filename = filename_hint.filter(|f| !f.is_empty()).unwrap_or_else(|| {
        tidm_core::naming::suggest_filename(title_hint.as_deref(), &url, detected_ext.as_deref())
    });
    let download_dir = state.config.settings.get().await.download_dir;
    let dest = tidm_core::naming::dest_path(&download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let dest = tidm_core::jobs::sanitize_dest_for_kind(&dest, kind);

    tracing::info!(%url, dest = %dest.display(), ?kind, "queued download from browser extension");
    let entry = DownloadEntry::new(url, dest, kind).with_request_context(headers, cookie).with_quality(quality);
    let id = entry.id.clone();
    if let Err(e) = state.store.add_entry(entry).await {
        tracing::warn!(error = %e, "failed to persist download entry from extension");
        return;
    }

    // Run it immediately rather than leaving it `Queued` for a separate "run
    // the queue" step to pick up later: many of these URLs carry a short-lived
    // signed token (observed: an `expires` param only ~8 minutes out) that can
    // lapse in exactly the gap between "extension detected/queued this" and
    // "user remembered to click Run queue" - spawned so the HTTP response to
    // the extension isn't held open for the whole download.
    let manager = state.manager.clone();
    tokio::spawn(async move {
        if let Err(e) = manager.run_entry_now(&id).await {
            tracing::warn!(error = %e, %id, "immediate run of extension-queued download failed to start");
        }
    });
}

/// Tail shared by the normal kind-detection path and the social-extraction
/// path above: build `dest`, persist a `Http`-kind entry, and run it
/// immediately (same short-lived-signed-URL reasoning as `queue_url`'s own
/// doc comment on `run_entry_now` - resolved social CDN URLs expire in hours
/// at best, sometimes far less).
async fn queue_resolved_http(
    state: &AppState,
    url: String,
    filename: String,
    headers: HashMap<String, String>,
    cookie: Option<String>,
    quality: Option<String>,
) {
    let download_dir = state.config.settings.get().await.download_dir;
    let dest = tidm_core::naming::dest_path(&download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    tracing::info!(%url, dest = %dest.display(), "queued download resolved from a social media page");
    let entry =
        DownloadEntry::new(url, dest, DownloadKind::Http).with_request_context(headers, cookie).with_quality(quality);
    let id = entry.id.clone();
    if let Err(e) = state.store.add_entry(entry).await {
        tracing::warn!(error = %e, "failed to persist resolved social media entry");
        return;
    }

    let manager = state.manager.clone();
    tokio::spawn(async move {
        if let Err(e) = manager.run_entry_now(&id).await {
            tracing::warn!(error = %e, %id, "immediate run of resolved social media download failed to start");
        }
    });
}

/// `RequestWatcher.createRequestData` collects possibly-repeated header values
/// (`{name: [v1, v2, ...]}`); flatten to one string per header the same way
/// HTTP itself joins repeated headers, since `RequestContext`/`reqwest` deal in
/// single string values. `navigator.userAgent`, captured separately from the
/// page (not via `webRequest`, whose exposure of the actual User-Agent header
/// sent on the wire is inconsistent across browsers), always wins over
/// whatever `requestHeaders` happened to capture for that key - it's the one
/// value guaranteed to match what the real browser actually used, which
/// matters for sites (Cloudflare-protected ones especially) that bind a
/// clearance cookie to the exact User-Agent that solved their challenge.
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
        );
        assert_eq!(item.info, "My Cool Video.mp4");
        assert_eq!(item.url, "https://cdn.example/abc123.mp4?token=secret");
        assert_eq!(item.page_url.as_deref(), Some("https://site.example/watch?v=1"));
    }

    #[test]
    fn video_list_item_falls_back_to_url_derived_name_without_a_title() {
        let item = to_video_list_item("v1", "https://cdn.example/movie.mkv", None, None, None);
        assert_eq!(item.info, "movie.mkv");
        assert_eq!(item.page_url, None);
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
