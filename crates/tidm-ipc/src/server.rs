
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

pub struct ServerConfig {
    pub settings: Arc<SettingsStore>,
    pub on_new_detection: Option<tokio::sync::mpsc::UnboundedSender<VideoListItem>>,
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
}

struct AppState {
    store: Arc<DownloadStore>,
    manager: Arc<DownloadManager>,
    config: ServerConfig,
    detected: Mutex<Vec<DetectedMedia>>,
    next_id: AtomicU64,
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
    #[serde(rename = "newDetection", skip_serializing_if = "Option::is_none")]
    new_detection: Option<VideoListItem>,
    #[serde(rename = "vidQueued", skip_serializing_if = "Option::is_none")]
    vid_queued: Option<bool>,
    #[serde(rename = "qualityVariants", skip_serializing_if = "Option::is_none")]
    quality_variants: Option<Vec<tidm_media::quality::QualityOption>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct VideoListItem {
    id: String,
    text: String,
    info: String,
    url: String,
    #[serde(rename = "pageUrl")]
    page_url: Option<String>,
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
    cookie: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: Option<String>,
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
    let guessed_kind = DownloadKind::guess_from_url(&url);

    let detected_ext = if matches!(guessed_kind, DownloadKind::Http) {
        let ctx = RequestContext { headers: headers.clone(), cookie: cookie.clone() };
        tidm_core::naming::resolve_real_extension(&state.manager.http_client(), &url, &ctx).await
    } else {
        None
    };

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

    let manager = state.manager.clone();
    tokio::spawn(async move {
        if let Err(e) = manager.run_entry_now(&id).await {
            tracing::warn!(error = %e, %id, "immediate run of extension-queued download failed to start");
        }
    });
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
