#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use tauri::{Emitter, Manager, State};
use tidm_core::jobs::DownloadKind;
use tidm_core::queue::{
    default_data_dir, default_settings_path, default_store_path, DownloadEntry, DownloadManager, DownloadStore,
    Settings, SettingsStore,
};
use tidm_net::HttpClient;
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
        tidm_core::naming::resolve_real_extension(client, url, &tidm_net::RequestContext::default()).await
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
        tidm_core::naming::suggest_filename(None, &url, detected_ext.as_deref())
    });
    let dest = tidm_core::naming::dest_path(&settings.download_dir, &url, &filename);
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let dest = tidm_core::jobs::sanitize_dest_for_kind(&dest, kind);

    let entry = DownloadEntry::new(url, dest, kind).with_quality(quality);
    state.store.add_entry(entry).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn probe_qualities(state: State<'_, AppState>, url: String) -> Result<Vec<tidm_media::quality::QualityOption>, String> {
    let client = state.manager.read().await.http_client();
    let (kind, _detected_ext) = detect_kind(&client, &url).await;
    let ctx = tidm_net::RequestContext::default();
    match kind {
        DownloadKind::Http => Ok(Vec::new()),
        DownloadKind::Hls => tidm_media::quality::probe_hls_qualities(&client, &url, &ctx).await.map_err(|e| e.to_string()),
        DownloadKind::Dash => tidm_media::quality::probe_dash_qualities(&client, &url, &ctx).await.map_err(|e| e.to_string()),
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
async fn open_file(app: tauri::AppHandle, path: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    let dest = std::path::PathBuf::from(&path);
    let target = if tokio::fs::metadata(&dest).await.is_ok() {
        dest
    } else {
        tidm_core::naming::cache_dir_for(&dest).join("output.partial")
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
    let cache_dir = tidm_core::naming::cache_dir_for(&dest);
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

fn main() {
    tracing_subscriber::fmt::init();

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
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
            tauri::async_runtime::spawn(async move {
                let config = tidm_ipc::server::ServerConfig { settings: server_settings, on_new_detection: Some(detection_tx) };
                if let Err(e) = tidm_ipc::server::serve(server_store, server_manager, config, 8597).await {
                    tracing::warn!(error = %e, "tidm-ipc server exited");
                }
            });

            let detection_app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                while detection_rx.recv().await.is_some() {
                    show_or_refresh_detection_window(&detection_app_handle);
                }
            });

            let scheduler_store = store.clone();
            let scheduler_manager = manager.clone();
            tauri::async_runtime::spawn(tidm_core::queue::run_scheduler_forever(scheduler_store, scheduler_manager));

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
            open_containing_folder,
            detection_window_set_pinned,
            detection_window_hide
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

const DETECTION_WINDOW_LABEL: &str = "detection";

fn show_or_refresh_detection_window(app: &tauri::AppHandle) {
    if let Some(win) = app.get_webview_window(DETECTION_WINDOW_LABEL) {
        let _ = win.show();
        let _ = win.set_focus();
        let _ = win.emit("detection-updated", ());
        return;
    }
    let win = tauri::WebviewWindowBuilder::new(app, DETECTION_WINDOW_LABEL, tauri::WebviewUrl::App("detected.html".into()))
        .title("Detected downloads")
        .inner_size(420.0, 480.0)
        .resizable(true)
        .always_on_top(true)
        .focused(true)
        .build();
    match win {
        Ok(win) => {
            let win_for_close = win.clone();
            win.on_window_event(move |event| {
                if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                    api.prevent_close();
                    let _ = win_for_close.hide();
                }
            });
        }
        Err(e) => tracing::warn!(error = %e, "failed to open detection window"),
    }
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
