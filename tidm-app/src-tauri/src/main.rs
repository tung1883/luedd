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

/// Best-effort detection of what kind a URL really is, beyond its own
/// extension - shared by `add_download` and `probe_qualities` so a URL that
/// looks like a plain file but is actually a disguised HLS/DASH manifest
/// (see `m3u8-guide.txt`) gets routed the same way in both.
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

    // Facebook/Instagram post/reel/watch URLs are pages, not downloadable
    // files - resolve to the real direct media URL(s) first (no cookie here,
    // since a manually-pasted GUI URL has no captured browser session behind
    // it - only public content resolves this way; private/saved content
    // needs the browser-extension flow, which does capture cookies). A
    // carousel resolves to more than one queued entry.
    if let Some(site) = tidm_media::social::detect_site(&url) {
        let ctx = tidm_net::RequestContext::default();
        let items = tidm_media::social::extract(site, &client, &url, &ctx).await.map_err(|e| e.to_string())?;
        for item in items {
            let item_filename = item
                .suggested_name
                .clone()
                .unwrap_or_else(|| tidm_core::naming::suggest_filename(None, &item.url, None));
            let dest = tidm_core::naming::dest_path(&settings.download_dir, &item.url, &item_filename);
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            let entry = DownloadEntry::new(item.url, dest, DownloadKind::Http).with_quality(quality.clone());
            state.store.add_entry(entry).await.map_err(|e| e.to_string())?;
        }
        return Ok(());
    }

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

/// Fetches and parses `url`'s manifest for its selectable quality variants
/// (empty for a plain HTTP URL, or an HLS/DASH URL with only one rendition),
/// so the GUI's "Add" flow can show a picker before actually queuing -
/// called separately from `add_download` rather than folded into it, since
/// probing needs a network round-trip the caller may not want to pay for
/// every add (a plain-HTTP URL skips it entirely, matching `add_download`'s
/// own kind detection).
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
/// The frontend only shows this button once `path` (the entry's `dest`)
/// exists (`Finished`) or for a plain HTTP download mid-transfer, where
/// `progressive::download` writes the growing file inside the per-download
/// cache dir (not `dest` itself - see `naming::cache_dir_for`) until it's
/// complete, so that's the path opened in the latter case.
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

/// Opens the file's containing folder in the OS file manager with the file
/// itself selected/highlighted if it already exists (`Finished`); otherwise
/// falls back to the per-download cache dir (exists once the download has
/// started) and finally to the configured downloads folder itself, so the
/// button always opens something reasonable at any stage.
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

            // Auto-retries `Failed` entries on a backoff and drives any
            // configured schedule window - dead code until this spawn (the
            // loop itself already existed, just unwired). Same "captures the
            // manager Arc at startup, not settings-change-aware" pattern as
            // the tidm-ipc server spawn above, since concurrency limits
            // aren't relevant to what the scheduler itself does (retry
            // bookkeeping + kicking `run_queued`).
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
            open_containing_folder
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
