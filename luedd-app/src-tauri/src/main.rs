#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use base64::Engine;
use tauri::{Emitter, Manager, State};
use luedd_core::jobs::DownloadKind;
use luedd_core::queue::{
    default_data_dir, default_settings_path, default_store_path, DownloadEntry, DownloadManager, DownloadStore,
    Settings, SettingsStore,
};
use luedd_net::HttpClient;
use tokio::sync::RwLock;

struct AppState {
    store: Arc<DownloadStore>,
    settings: Arc<SettingsStore>,
    manager: RwLock<Arc<DownloadManager>>,
}

fn build_manager(store: Arc<DownloadStore>, settings: &Settings) -> Result<Arc<DownloadManager>, String> {
    let client = HttpClient::new().map_err(|e| e.to_string())?;
    Ok(Arc::new(DownloadManager::new(
        store,
        client,
        settings.max_concurrent_downloads,
        settings.per_download_concurrency,
    )))
}

async fn detect_kind(client: &HttpClient, url: &str) -> (DownloadKind, Option<String>) {
    let guessed_kind = DownloadKind::guess_from_url(url);
    let detected_ext = if matches!(guessed_kind, DownloadKind::Http) {
        luedd_core::naming::resolve_real_extension(client, url, &luedd_net::RequestContext::default()).await
    } else {
        None
    };
    let kind = match detected_ext.as_deref() {
        Some("m3u8") => DownloadKind::Hls,
        Some("mpd") => DownloadKind::Dash,
        _ => guessed_kind,
    };
    (kind, detected_ext)
}

#[tauri::command]
async fn add_download(state: State<'_, AppState>, url: String, filename: Option<String>, quality: Option<String>) -> Result<(), String> {
    let settings = state.settings.get().await;
    let client = state.manager.read().await.http_client();

    let (kind, detected_ext) = detect_kind(&client, &url).await;

    let filename = filename.filter(|f| !f.is_empty()).unwrap_or_else(|| {
        luedd_core::naming::suggest_filename(None, &url, detected_ext.as_deref())
    });
    let dest = luedd_core::naming::dest_path(&settings.download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let dest = luedd_core::jobs::sanitize_dest_for_kind(&dest, kind);

    let entry = DownloadEntry::new(url, dest, kind).with_quality(quality);
    state.store.add_entry(entry).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn probe_qualities(state: State<'_, AppState>, url: String) -> Result<Vec<luedd_media::quality::QualityOption>, String> {
    let client = state.manager.read().await.http_client();
    let (kind, _detected_ext) = detect_kind(&client, &url).await;
    let ctx = luedd_net::RequestContext::default();
    match kind {
        DownloadKind::Http => Ok(Vec::new()),
        DownloadKind::Hls => luedd_media::quality::probe_hls_qualities(&client, &url, &ctx).await.map_err(|e| e.to_string()),
        DownloadKind::Dash => luedd_media::quality::probe_dash_qualities(&client, &url, &ctx).await.map_err(|e| e.to_string()),
    }
}

#[tauri::command]
async fn list_downloads(state: State<'_, AppState>) -> Result<Vec<DownloadEntry>, String> {
    Ok(state.store.list_entries().await)
}

#[tauri::command]
async fn run_queue(state: State<'_, AppState>) -> Result<(), String> {
    let manager = state.manager.read().await.clone();
    manager.run_queued().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn remove_entry(state: State<'_, AppState>, id: String, delete_files: bool) -> Result<bool, String> {
    let manager = state.manager.read().await.clone();
    manager.remove_entry(&id, delete_files).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn retry_entry(state: State<'_, AppState>, id: String) -> Result<bool, String> {
    let manager = state.manager.read().await.clone();
    manager.retry_entry(&id).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn pause_entry(state: State<'_, AppState>, id: String) -> Result<bool, String> {
    let manager = state.manager.read().await.clone();
    manager.pause_entry(&id).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn clear_finished(state: State<'_, AppState>, delete_files: bool) -> Result<usize, String> {
    let manager = state.manager.read().await.clone();
    manager.clear_finished(delete_files).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn pick_folder(app: tauri::AppHandle) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;
    tokio::task::spawn_blocking(move || app.dialog().file().blocking_pick_folder())
        .await
        .map_err(|e| e.to_string())
        .map(|picked| picked.map(|p| p.to_string()))
}

#[tauri::command]
fn open_external_url(app: tauri::AppHandle, url: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    app.opener().open_url(url, None::<&str>).map_err(|e| e.to_string())
}

/// Reads a downloaded image file and returns it as a `data:` URL, so the
/// desktop GUI's Preview button can show it inline (a modal overlay) rather
/// than handing off to whatever the OS's default image viewer happens to
/// be - the plain-`open_file` path used for every other file type. Base64
/// round-tripping through a Tauri command sidesteps needing the asset
/// protocol's own scope/permission config just to load an arbitrary local
/// path into an `<img>` tag.
#[tauri::command]
async fn read_image_data_url(path: String) -> Result<String, String> {
    let dest = std::path::PathBuf::from(&path);
    let ext = dest.extension().and_then(|e| e.to_str()).unwrap_or("png").to_ascii_lowercase();
    let mime = match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        _ => "image/png",
    };
    let bytes = tokio::fs::read(&dest).await.map_err(|e| e.to_string())?;
    Ok(format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)))
}

#[derive(serde::Serialize)]
struct PreviewOut {
    data_url: String,
    kind: String,
}

/// A preview image for a queue entry's file: the image itself for image files,
/// a single grabbed frame for video (also works on the in-progress `.partial`),
/// and an error otherwise so the UI can fall back to a file-type glyph.
#[tauri::command]
async fn read_preview(path: String) -> Result<PreviewOut, String> {
    const IMG: &[&str] = &["jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "avif", "ico"];
    const VID: &[&str] = &["mp4", "mkv", "webm", "mov", "m4v", "avi", "ts", "m2ts", "flv"];

    let p = std::path::PathBuf::from(&path);
    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();

    if IMG.contains(&ext.as_str()) {
        let mime = match ext.as_str() {
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "bmp" => "image/bmp",
            "svg" => "image/svg+xml",
            "avif" => "image/avif",
            "ico" => "image/x-icon",
            _ => "image/png",
        };
        let bytes = tokio::fs::read(&p).await.map_err(|e| e.to_string())?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        return Ok(PreviewOut { data_url: format!("data:{mime};base64,{b64}"), kind: "image".into() });
    }

    let target = if tokio::fs::metadata(&p).await.is_ok() {
        p.clone()
    } else {
        luedd_core::naming::cache_dir_for(&p).join("output.partial")
    };
    if !VID.contains(&ext.as_str()) && tokio::fs::metadata(&target).await.is_err() {
        return Err("no preview for this file type".into());
    }

    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.args(["-v", "error", "-ss", "1", "-i"])
        .arg(&target)
        .args(["-frames:v", "1", "-vf", "scale=480:-2", "-f", "image2pipe", "-vcodec", "mjpeg", "-"]);
    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW: no console window flash when spawned from the GUI app.
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    let out = cmd.output().await.map_err(|e| format!("ffmpeg: {e}"))?;
    if !out.status.success() || out.stdout.is_empty() {
        return Err(format!("ffmpeg: {}", String::from_utf8_lossy(&out.stderr)));
    }
    let b64 = base64::engine::general_purpose::STANDARD.encode(&out.stdout);
    Ok(PreviewOut { data_url: format!("data:image/jpeg;base64,{b64}"), kind: "video".into() })
}

#[tauri::command]
async fn open_file(app: tauri::AppHandle, path: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    let dest = std::path::PathBuf::from(&path);
    let target = if tokio::fs::metadata(&dest).await.is_ok() {
        dest
    } else {
        luedd_core::naming::cache_dir_for(&dest).join("output.partial")
    };
    app.opener().open_path(target.to_string_lossy().to_string(), None::<&str>).map_err(|e| e.to_string())
}

#[tauri::command]
async fn open_containing_folder(app: tauri::AppHandle, state: State<'_, AppState>, path: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    let dest = std::path::PathBuf::from(&path);
    if tokio::fs::metadata(&dest).await.is_ok() {
        return app.opener().reveal_item_in_dir(path).map_err(|e| e.to_string());
    }
    let cache_dir = luedd_core::naming::cache_dir_for(&dest);
    let target = if tokio::fs::metadata(&cache_dir).await.is_ok() {
        cache_dir
    } else {
        state.settings.get().await.download_dir
    };
    app.opener().open_path(target.to_string_lossy().to_string(), None::<&str>).map_err(|e| e.to_string())
}

#[tauri::command]
async fn get_settings(state: State<'_, AppState>) -> Result<Settings, String> {
    Ok(state.settings.get().await)
}

#[tauri::command]
async fn set_settings(state: State<'_, AppState>, settings: Settings) -> Result<(), String> {
    tokio::fs::create_dir_all(&settings.download_dir).await.map_err(|e| e.to_string())?;
    state.settings.set(settings.clone()).await.map_err(|e| e.to_string())?;
    let new_manager = build_manager(state.store.clone(), &settings)?;
    *state.manager.write().await = new_manager;
    Ok(())
}

const IPC_PORT: u16 = 8597;

fn main() {
    tracing_subscriber::fmt::init();

    // The frontend is a handful of embedded HTML files served over an unchanging
    // internal URL; WebView2's HTTP cache otherwise keeps serving a stale build
    // after an upgrade. Disable it - there is nothing here worth caching.
    std::env::set_var("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS", "--disable-http-cache");

    // Single-instance guard: claim the IPC port *before* any window exists. A
    // second copy would otherwise flash a window and then die when the bind
    // fails, and would fight the first for the WebView2 user-data lock.
    let ipc_listener = match std::net::TcpListener::bind(("127.0.0.1", IPC_PORT)) {
        Ok(l) => l,
        Err(_) => {
            // Another instance holds the port. Ask it to surface its window,
            // then exit so there is only ever one running copy.
            if let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", IPC_PORT)) {
                use std::io::{Read, Write};
                let _ = s.write_all(b"GET /focus-main HTTP/1.0\r\nHost: localhost\r\n\r\n");
                let _ = s.flush();
                // Wait for the response so the running instance has actually
                // handled the request before this process exits and drops the
                // socket (Windows can discard unsent data on abrupt exit).
                s.set_read_timeout(Some(std::time::Duration::from_secs(2))).ok();
                let _ = s.read(&mut [0u8; 64]);
            }
            tracing::info!("luedd-app is already running; asked it to focus and exiting");
            return;
        }
    };

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            let data_dir = default_data_dir();
            let store_path = default_store_path(&data_dir);
            let settings_path = default_settings_path(&data_dir);

            let (store, settings) = tauri::async_runtime::block_on(async {
                let store = Arc::new(DownloadStore::open(store_path).await.expect("failed to open download store"));
                let settings =
                    Arc::new(SettingsStore::open(settings_path, &data_dir).await.expect("failed to open settings"));
                (store, settings)
            });

            let initial_settings = tauri::async_runtime::block_on(settings.get());
            let manager = build_manager(store.clone(), &initial_settings).expect("failed to build download manager");

            let server_store = store.clone();
            let server_manager = manager.clone();
            let server_settings = settings.clone();
            let (detection_tx, mut detection_rx) = tokio::sync::mpsc::unbounded_channel();
            let (focus_tx, mut focus_rx) = tokio::sync::mpsc::unbounded_channel();
            tauri::async_runtime::spawn(async move {
                let config = luedd_ipc::server::ServerConfig {
                    settings: server_settings,
                    on_new_detection: Some(detection_tx),
                    on_focus_request: Some(focus_tx),
                };
                if let Err(e) = luedd_ipc::server::serve(server_store, server_manager, config, ipc_listener).await {
                    tracing::warn!(error = %e, "luedd-ipc server exited");
                }
            });

            // Create it up front (hidden) so the header icon and the first
            // detection both hit the reliable "already exists" path.
            if let Err(e) = build_detection_window(&app.handle()) {
                tracing::warn!(error = %e, "failed to pre-create detection window");
            }

            let detection_app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                while detection_rx.recv().await.is_some() {
                    // Never auto-pop the panel - just refresh it if the user
                    // already has it open. It opens only via the header icon.
                    refresh_detection_window(&detection_app_handle);
                }
            });

            // A second launch pings `/focus-main`; bring the existing window up.
            let focus_app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                while focus_rx.recv().await.is_some() {
                    if let Some(win) = focus_app_handle.get_webview_window("main") {
                        let _ = win.unminimize();
                        let _ = win.show();
                        // A background process can't steal the Windows
                        // foreground outright; a brief always-on-top flip pops
                        // the window above everything without that privilege.
                        let _ = win.set_always_on_top(true);
                        let _ = win.set_always_on_top(false);
                        let _ = win.set_focus();
                        let _ = win.request_user_attention(Some(tauri::UserAttentionType::Critical));
                    }
                }
            });

            let scheduler_store = store.clone();
            let scheduler_manager = manager.clone();
            tauri::async_runtime::spawn(luedd_core::queue::run_scheduler_forever(scheduler_store, scheduler_manager));

            if let Some(main_win) = app.get_webview_window("main") {
                let app_handle_for_exit = app.handle().clone();
                main_win.on_window_event(move |event| {
                    if let tauri::WindowEvent::CloseRequested { .. } = event {
                        app_handle_for_exit.exit(0);
                    }
                });
            }

            app.manage(AppState { store, settings, manager: RwLock::new(manager) });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_download,
            probe_qualities,
            list_downloads,
            run_queue,
            remove_entry,
            retry_entry,
            pause_entry,
            clear_finished,
            get_settings,
            set_settings,
            pick_folder,
            open_file,
            read_image_data_url,
            read_preview,
            open_containing_folder,
            open_external_url,
            detection_window_set_pinned,
            detection_window_hide,
            detection_window_show
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

const DETECTION_WINDOW_LABEL: &str = "detection";

/// Build the detection window (hidden). Called once at startup so it always
/// exists by the time the user clicks the header icon or a detection arrives -
/// creating a webview window lazily from a command handler is unreliable.
fn build_detection_window(app: &tauri::AppHandle) -> tauri::Result<tauri::WebviewWindow> {
    let detected_url = format!("detected.html?v={}", env!("LUEDD_ASSET_VER"));
    let win = tauri::WebviewWindowBuilder::new(app, DETECTION_WINDOW_LABEL, tauri::WebviewUrl::App(detected_url.into()))
        .title("Detected downloads")
        .inner_size(420.0, 480.0)
        .resizable(true)
        .always_on_top(true)
        .focused(false)
        .visible(false)
        .background_color(tauri::webview::Color(0x16, 0x17, 0x1a, 0xff))
        .build()?;
    let win_for_close = win.clone();
    win.on_window_event(move |event| {
        if let tauri::WindowEvent::CloseRequested { api, .. } = event {
            api.prevent_close();
            let _ = win_for_close.hide();
        }
    });
    Ok(win)
}

/// Tell the detection panel to reload its list without changing its
/// visibility. A no-op for the user if the panel is hidden.
fn refresh_detection_window(app: &tauri::AppHandle) {
    if let Some(win) = app.get_webview_window(DETECTION_WINDOW_LABEL) {
        let _ = win.emit("detection-updated", ());
    }
}

fn show_or_refresh_detection_window(app: &tauri::AppHandle, focus: bool) {
    let win = match app.get_webview_window(DETECTION_WINDOW_LABEL) {
        Some(win) => win,
        None => match build_detection_window(app) {
            Ok(win) => win,
            Err(e) => {
                tracing::warn!(error = %e, "failed to open detection window");
                return;
            }
        },
    };
    let _ = win.unminimize();
    let _ = win.show();
    if focus {
        let _ = win.set_focus();
    }
    let _ = win.emit("detection-updated", ());
}

#[tauri::command]
fn detection_window_set_pinned(app: tauri::AppHandle, pinned: bool) -> Result<(), String> {
    if let Some(win) = app.get_webview_window(DETECTION_WINDOW_LABEL) {
        win.set_always_on_top(pinned).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[tauri::command]
fn detection_window_hide(app: tauri::AppHandle) -> Result<(), String> {
    if let Some(win) = app.get_webview_window(DETECTION_WINDOW_LABEL) {
        win.hide().map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[tauri::command]
fn detection_window_show(app: tauri::AppHandle) -> Result<(), String> {
    show_or_refresh_detection_window(&app, true);
    Ok(())
}
