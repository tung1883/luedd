#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use tauri::{Manager, State};
use tidm_core::jobs::DownloadKind;
use tidm_core::queue::{
    default_data_dir, default_settings_path, default_store_path, DownloadEntry, DownloadManager, DownloadStore,
    Settings, SettingsStore,
};
use tidm_net::HttpClient;
use tokio::sync::RwLock;

/// Shared backend state, the GUI's equivalent of XDM's `ApplicationCore`: one
/// persisted download store, one settings store, and one queue runner (rebuilt
/// whenever settings change, since concurrency limits are fixed at
/// construction) - reused by every Tauri command and by the M4 IPC server this
/// process also hosts.
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

#[tauri::command]
async fn add_download(state: State<'_, AppState>, url: String, filename: Option<String>) -> Result<(), String> {
    let guessed_kind = DownloadKind::guess_from_url(&url);
    let settings = state.settings.get().await;
    tokio::fs::create_dir_all(&settings.download_dir).await.ok();

    let detected_ext = if matches!(guessed_kind, DownloadKind::Http) {
        let client = state.manager.read().await.http_client();
        tidm_core::naming::resolve_real_extension(&client, &url, &tidm_net::RequestContext::default()).await
    } else {
        None
    };
    // A URL that looked like a plain file but is actually a disguised
    // HLS/DASH manifest (see `m3u8-guide.txt`) must go through the real
    // playlist downloader, not be saved as raw manifest text.
    let kind = match detected_ext.as_deref() {
        Some("m3u8") => DownloadKind::Hls,
        Some("mpd") => DownloadKind::Dash,
        _ => guessed_kind,
    };

    let filename = filename.filter(|f| !f.is_empty()).unwrap_or_else(|| {
        tidm_core::naming::suggest_filename(None, &url, detected_ext.as_deref())
    });
    let dest = tidm_core::jobs::sanitize_dest_for_kind(&settings.download_dir.join(filename), kind);

    let entry = DownloadEntry::new(url, dest, kind);
    state.store.add_entry(entry).await.map_err(|e| e.to_string())
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
    // The dialog plugin's own async API takes a callback; running the
    // blocking variant on a dedicated thread keeps this command a plain
    // async fn returning a value, like every other command here.
    tokio::task::spawn_blocking(move || app.dialog().file().blocking_pick_folder())
        .await
        .map_err(|e| e.to_string())
        .map(|picked| picked.map(|p| p.to_string()))
}

/// Opens a downloaded file with the OS's default handler for its type (video
/// player, image viewer, PDF reader, ...) - the "Preview" action per entry.
#[tauri::command]
async fn open_file(app: tauri::AppHandle, path: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    app.opener().open_path(path, None::<&str>).map_err(|e| e.to_string())
}

/// Opens the file's containing folder in the OS file manager with the file
/// itself selected/highlighted - the "Open folder" action per entry.
#[tauri::command]
async fn open_containing_folder(app: tauri::AppHandle, path: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt;
    app.opener().reveal_item_in_dir(path).map_err(|e| e.to_string())
}

#[tauri::command]
async fn get_settings(state: State<'_, AppState>) -> Result<Settings, String> {
    Ok(state.settings.get().await)
}

#[tauri::command]
async fn set_settings(state: State<'_, AppState>, settings: Settings) -> Result<(), String> {
    tokio::fs::create_dir_all(&settings.download_dir).await.map_err(|e| e.to_string())?;
    state.settings.set(settings.clone()).await.map_err(|e| e.to_string())?;
    // Concurrency limits are fixed at `DownloadManager` construction, so a
    // changed setting needs a fresh manager - existing in-flight downloads
    // keep running under the old one since they hold their own `Arc` clone.
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

            // M4: the same local server the browser extension's connector.js
            // talks to runs in-process here, sharing the exact store the GUI
            // reads/writes - a download added from the browser shows up in the
            // GUI's list on its next 2s poll with no extra wiring. It shares
            // the live `SettingsStore` too, so a folder changed in the GUI's
            // Settings panel applies immediately to extension-triggered
            // downloads as well, not just ones added from the GUI.
            let server_store = store.clone();
            let server_manager = manager.clone();
            let server_settings = settings.clone();
            tauri::async_runtime::spawn(async move {
                let config = tidm_ipc::server::ServerConfig { settings: server_settings };
                if let Err(e) = tidm_ipc::server::serve(server_store, server_manager, config, 8597).await {
                    tracing::warn!(error = %e, "tidm-ipc server exited");
                }
            });

            app.manage(AppState { store, settings, manager: RwLock::new(manager) });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_download,
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
            open_containing_folder
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
