use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use luedd_net::{HttpClient, RequestContext};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use super::model::{now_unix, DownloadEntry, DownloadProgress, DownloadStatus};
use super::store::DownloadStore;
use crate::backend::{BackendConfig, BackendRegistry, DownloadReq};

type RunningTasks = Arc<StdMutex<HashMap<String, AbortHandle>>>;

pub struct DownloadManager {
    store: Arc<DownloadStore>,
    client: HttpClient,
    semaphore: Arc<Semaphore>,
    per_download_concurrency: usize,
    running: RunningTasks,
    registry: Arc<BackendRegistry>,
    backend_config: BackendConfig,
}

impl DownloadManager {
    pub fn new(store: Arc<DownloadStore>, client: HttpClient, max_concurrent: usize, per_download_concurrency: usize) -> Self {
        Self {
            registry: Arc::new(BackendRegistry::with_builtins(client.clone())),
            backend_config: BackendConfig::default(),
            store,
            client,
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
            per_download_concurrency,
            running: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Swap in a registry with extra backends (yt-dlp, instagram, …) plus the
    /// config snapshot they run against. Called by the app after loading Settings.
    pub fn with_backends(mut self, registry: Arc<BackendRegistry>, config: BackendConfig) -> Self {
        self.registry = registry;
        self.backend_config = config;
        self
    }

    pub fn http_client(&self) -> HttpClient {
        self.client.clone()
    }

    pub async fn run_queued(&self) -> anyhow::Result<()> {
        let queued: Vec<_> = self
            .store
            .list_entries()
            .await
            .into_iter()
            .filter(|e| matches!(e.status, DownloadStatus::Queued))
            .collect();

        let mut tasks = Vec::with_capacity(queued.len());
        for entry in queued {
            let store = self.store.clone();
            let client = self.client.clone();
            let semaphore = self.semaphore.clone();
            let concurrency = self.per_download_concurrency;
            let running = self.running.clone();
            let registry = self.registry.clone();
            let config = self.backend_config.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                run_tracked(store, client, entry, concurrency, running, registry, config).await;
            }));
        }

        for task in tasks {
            task.await.ok();
        }
        Ok(())
    }

    pub async fn run_entry_now(&self, id: &str) -> anyhow::Result<()> {
        let Some(entry) = self.store.get_entry(id).await else {
            anyhow::bail!("no entry with id {id}");
        };
        if !matches!(entry.status, DownloadStatus::Queued) {
            return Ok(());
        }
        run_tracked(
            self.store.clone(),
            self.client.clone(),
            entry,
            self.per_download_concurrency,
            self.running.clone(),
            self.registry.clone(),
            self.backend_config.clone(),
        )
        .await;
        Ok(())
    }

    /// Ids of entries with a live run task right now.
    pub fn running_ids(&self) -> Vec<String> {
        self.running.lock().unwrap().keys().cloned().collect()
    }

    pub async fn pause_entry(&self, id: &str) -> anyhow::Result<bool> {
        let handle = { self.running.lock().unwrap().remove(id) };
        let Some(handle) = handle else {
            return Ok(false);
        };
        handle.abort();
        // The abort stops our run task, but a torrent's transfer lives in a
        // background session — pause it there too.
        if let Some(e) = self.store.get_entry(id).await {
            if let Some(b) = self.registry.get(&e.backend_id) {
                if b.is_torrent() {
                    let _ = b.on_pause(&e.url).await;
                }
            }
        }
        self.store.update_entry(id, |e| e.status = DownloadStatus::Paused).await?;
        Ok(true)
    }

    pub async fn remove_entry(&self, id: &str, delete_files: bool) -> anyhow::Result<bool> {
        let removed = self.store.remove_entry(id).await?;
        if let Some(entry) = &removed {
            // Let a torrent session drop the torrent (and release file handles)
            // BEFORE we try to delete anything on disk.
            if let Some(b) = self.registry.get(&entry.backend_id) {
                if b.is_torrent() {
                    let _ = b.on_remove(&entry.url, delete_files).await;
                }
            }
            if delete_files {
                delete_artifacts(entry).await;
            }
        }
        Ok(removed.is_some())
    }

    pub async fn retry_entry(&self, id: &str) -> anyhow::Result<bool> {
        self.store.retry_entry(id).await
    }

    pub async fn clear_finished(&self, delete_files: bool) -> anyhow::Result<usize> {
        let removed = self.store.clear_finished().await?;
        let count = removed.len();
        if delete_files {
            for entry in &removed {
                delete_artifacts(entry).await;
            }
        }
        Ok(count)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_tracked(
    store: Arc<DownloadStore>,
    client: HttpClient,
    entry: DownloadEntry,
    concurrency: usize,
    running: RunningTasks,
    registry: Arc<BackendRegistry>,
    config: BackendConfig,
) {
    let id = entry.id.clone();
    // Claim it atomically. Loses the race (already Downloading, or two sweeps
    // hit it at once) -> don't start a second transfer for the same entry.
    if !store.try_claim(&id).await.unwrap_or(false) {
        return;
    }
    let handle = tokio::spawn(async move {
        run_single(&store, &client, entry, concurrency, &registry, &config).await;
    });
    running.lock().unwrap().insert(id.clone(), handle.abort_handle());
    let _ = handle.await;
    running.lock().unwrap().remove(&id);
}

async fn run_single(
    store: &Arc<DownloadStore>,
    _client: &HttpClient,
    entry: DownloadEntry,
    concurrency: usize,
    registry: &BackendRegistry,
    config: &BackendConfig,
) {
    // Starting (or re-starting) a run: drop any error from a previous attempt so
    // the row doesn't show a stale failure while it downloads.
    store
        .update_entry(&entry.id, |e| {
            e.status = DownloadStatus::Downloading;
            e.error = None;
            e.next_retry_at = None;
        })
        .await
        .ok();

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress_store = store.clone();
    let progress_id = entry.id.clone();
    // Last time the backend emitted any event (unix seconds). The stall monitor
    // aborts a run that goes quiet for too long — an Instagram fetch on a hung
    // connection would otherwise sit at `Downloading` forever.
    // (last event time, converting?) — the stall monitor ignores runs that have
    // entered the mux phase, which is legitimately long and silent.
    let last_beat = Arc::new(StdMutex::new((now_unix(), false)));
    let beat_w = last_beat.clone();
    let progress_task = tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            {
                let mut b = beat_w.lock().unwrap();
                b.0 = now_unix();
                if matches!(event, luedd_net::JobEvent::Converting) {
                    b.1 = true;
                }
            }
            match event {
                luedd_net::JobEvent::Progress { downloaded_bytes, total_bytes, done_units, total_units, speed_bps } => {
                    // In-memory only; the scheduler flushes it to disk on a
                    // timer. Avoids a full downloads.json rewrite per tick.
                    progress_store
                        .set_progress(
                            &progress_id,
                            DownloadProgress { downloaded_bytes, total_bytes, done_units, total_units, speed_bps },
                        )
                        .await;
                }
                luedd_net::JobEvent::Converting => {
                    progress_store
                        .update_entry(&progress_id, |e| {
                            e.status = DownloadStatus::Converting;
                            e.progress = None;
                        })
                        .await
                        .ok();
                }
            }
        }
    });

    let ctx = RequestContext { headers: entry.headers.clone(), cookie: entry.cookie.clone() };
    let backend = registry.get(&entry.backend_id).unwrap_or_else(|| registry.http());
    let req = DownloadReq {
        url: entry.url.clone(),
        dest_dir: entry
            .out_dir
            .clone()
            .or_else(|| entry.dest.parent().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(".")),
        filename_hint: entry.dest.file_name().map(|s| s.to_string_lossy().into_owned()),
        ctx,
        quality: entry.quality.clone(),
        extras: entry.extras.clone(),
        concurrency,
        config: config.clone(),
    };
    // Abort the run if it emits nothing for STALL_SECS (some backends — a bare
    // Instagram profile probe — legitimately run a while before the first
    // event, so this only kicks in once things go completely silent).
    const STALL_SECS: i64 = 150;
    let run = backend.run(&req, Some(&progress_tx));
    let result = {
        tokio::pin!(run);
        loop {
            tokio::select! {
                r = &mut run => break r,
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    let (beat, converting) = *last_beat.lock().unwrap();
                    if !converting && now_unix() - beat > STALL_SECS {
                        break Err(anyhow::anyhow!("download stalled - no data for {STALL_SECS}s"));
                    }
                }
            }
        }
    };
    drop(progress_tx);
    progress_task.await.ok();

    match result {
        Ok(outcome) => {
            let mut files = outcome.files;
            let final_dest = if files.is_empty() { entry.dest.clone() } else { files.remove(0) };
            let meta = outcome.meta;
            let expected = outcome.expected_units.map(|n| n as u32);
            store
                .update_entry(&entry.id, |e| {
                    e.status = DownloadStatus::Finished;
                    e.error = None;
                    e.next_retry_at = None;
                    e.dest = final_dest.clone();
                    e.extra_files = files.clone();
                    e.expected_files = expected;
                    if meta.author.is_some() {
                        e.author = meta.author.clone();
                    }
                    if meta.title.is_some() {
                        e.title = meta.title.clone();
                    }
                    if meta.media_class.is_some() {
                        e.media_class = meta.media_class.clone();
                    }
                    // A backend that picks its own output folder (torrent, and
                    // some Instagram runs) — record it so delete-cleanup can wipe
                    // the whole folder later.
                    if meta.out_dir.is_some() {
                        e.out_dir = meta.out_dir.clone();
                    }
                })
                .await
                .ok();
        }
        Err(e) => {
            let msg = format!("{e:#}");
            let next_retry_at = super::scheduler::next_auto_retry_at(entry.retry_count);
            store
                .update_entry(&entry.id, |entry| {
                    entry.status = DownloadStatus::Failed;
                    entry.error = Some(msg);
                    entry.next_retry_at = next_retry_at;
                })
                .await
                .ok();
        }
    }
}

async fn delete_artifacts(entry: &DownloadEntry) {
    // A dedicated output folder (Instagram): wipe it whole — covers a
    // half-finished carousel/profile whose files were never tracked on the entry.
    if let Some(dir) = &entry.out_dir {
        if dir.file_name().is_some() {
            tokio::fs::remove_dir_all(dir).await.ok();
        }
    }
    tokio::fs::remove_file(&entry.dest).await.ok();
    tokio::fs::remove_dir_all(crate::naming::cache_dir_for(&entry.dest)).await.ok();
    for f in &entry.extra_files {
        tokio::fs::remove_file(f).await.ok();
        tokio::fs::remove_dir_all(crate::naming::cache_dir_for(f)).await.ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::DownloadKind;
    use crate::queue::model::DownloadEntry;

    #[tokio::test]
    async fn run_entry_now_runs_without_a_separate_run_queued_call() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-runnow-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let entry = DownloadEntry::new(
            "http://127.0.0.1:1/does-not-exist".into(),
            dir.join("out.bin"),
            DownloadKind::Http,
        );
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        manager.run_entry_now(&id).await.unwrap();

        let updated = store.get_entry(&id).await.unwrap();
        assert!(matches!(updated.status, DownloadStatus::Failed), "should have actually run, not stayed Queued");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn run_entry_now_is_a_noop_for_non_queued_entries() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-runnow-noop-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let entry = DownloadEntry::new("http://x/f".into(), dir.join("out.bin"), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();
        store.update_entry(&id, |e| e.status = DownloadStatus::Finished).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        manager.run_entry_now(&id).await.unwrap();

        let unchanged = store.get_entry(&id).await.unwrap();
        assert!(matches!(unchanged.status, DownloadStatus::Finished), "should not re-run an already-finished entry");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn pause_entry_aborts_a_running_download_and_marks_it_paused() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-pause-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    keep_alive.push(socket);
                }
            }
        });

        let entry = DownloadEntry::new(format!("http://{addr}/never-responds"), dir.join("out.bin"), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = Arc::new(DownloadManager::new(store.clone(), client, 2, 2));
        let run_manager = manager.clone();
        let run_id = id.clone();
        tokio::spawn(async move {
            run_manager.run_entry_now(&run_id).await.ok();
        });

        for _ in 0..100 {
            if matches!(store.get_entry(&id).await.unwrap().status, DownloadStatus::Downloading) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(matches!(store.get_entry(&id).await.unwrap().status, DownloadStatus::Downloading), "download never started");

        assert!(manager.pause_entry(&id).await.unwrap());
        let paused = store.get_entry(&id).await.unwrap();
        assert!(matches!(paused.status, DownloadStatus::Paused));

        assert!(!manager.pause_entry(&id).await.unwrap(), "pausing an already-paused entry is a no-op");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn marks_entry_failed_on_bad_url() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let entry = DownloadEntry::new(
            "http://127.0.0.1:1/does-not-exist".into(),
            dir.join("out.bin"),
            DownloadKind::Http,
        );
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        manager.run_queued().await.unwrap();

        let updated = store.get_entry(&id).await.unwrap();
        assert!(matches!(updated.status, DownloadStatus::Failed));
        assert!(updated.error.is_some());

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn clear_finished_with_delete_files_removes_output_and_cache_dir() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-clear-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let dest = dir.join("out.bin");
        tokio::fs::write(&dest, b"content").await.unwrap();
        let cache_dir = crate::naming::cache_dir_for(&dest);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("state.json"), b"{}").await.unwrap();

        let entry = DownloadEntry::new("http://x/f".into(), dest.clone(), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();
        store.update_entry(&id, |e| e.status = DownloadStatus::Finished).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        let removed_count = manager.clear_finished(true).await.unwrap();

        assert_eq!(removed_count, 1);
        assert!(store.list_entries().await.is_empty());
        assert!(tokio::fs::metadata(&dest).await.is_err(), "output file should be deleted");
        assert!(tokio::fs::metadata(&cache_dir).await.is_err(), "cache dir should be deleted");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn remove_entry_without_delete_files_keeps_output_on_disk() {
        let dir = std::env::temp_dir().join(format!("luedd-manager-remove-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let dest = dir.join("out.bin");
        tokio::fs::write(&dest, b"content").await.unwrap();
        let entry = DownloadEntry::new("http://x/f".into(), dest.clone(), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        assert!(manager.remove_entry(&id, false).await.unwrap());
        assert!(store.get_entry(&id).await.is_none());
        assert!(tokio::fs::metadata(&dest).await.is_ok(), "output file should be left alone");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    // --- torrent hook wiring -------------------------------------------------

    use std::sync::atomic::{AtomicBool, Ordering};

    struct StubTorrent {
        paused: Arc<AtomicBool>,
        removed: Arc<AtomicBool>,
        remove_saw_file: Arc<AtomicBool>,
        watch_path: std::path::PathBuf,
        block: bool,
    }

    #[async_trait::async_trait]
    impl crate::backend::DownloadBackend for StubTorrent {
        fn id(&self) -> &'static str {
            "torrent"
        }
        fn can_handle(&self, _u: &str, _s: Option<&crate::backend::Sniff>) -> crate::backend::Confidence {
            crate::backend::Confidence::No
        }
        fn is_torrent(&self) -> bool {
            true
        }
        async fn run(
            &self,
            _req: &crate::backend::DownloadReq,
            _p: Option<&luedd_net::ProgressTx>,
        ) -> anyhow::Result<crate::backend::Outcome> {
            if self.block {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
            Ok(crate::backend::Outcome::default())
        }
        async fn on_pause(&self, _url: &str) -> anyhow::Result<()> {
            self.paused.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn on_remove(&self, _url: &str, _delete: bool) -> anyhow::Result<()> {
            self.removed.store(true, Ordering::SeqCst);
            self.remove_saw_file
                .store(self.watch_path.exists(), Ordering::SeqCst);
            Ok(())
        }
    }

    fn manager_with_stub(
        store: Arc<DownloadStore>,
        stub: Arc<StubTorrent>,
    ) -> DownloadManager {
        let client = HttpClient::new().unwrap();
        let mut reg = BackendRegistry::with_builtins(client.clone());
        reg.register(stub);
        DownloadManager::new(store, client, 2, 2)
            .with_backends(Arc::new(reg), BackendConfig::default())
    }

    #[tokio::test]
    async fn remove_entry_calls_on_remove_before_deleting_files() {
        let dir = std::env::temp_dir().join(format!("luedd-mgr-tor-rm-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let dest = dir.join("movie.mkv");
        tokio::fs::write(&dest, b"data").await.unwrap();

        let stub = Arc::new(StubTorrent {
            paused: Arc::new(AtomicBool::new(false)),
            removed: Arc::new(AtomicBool::new(false)),
            remove_saw_file: Arc::new(AtomicBool::new(false)),
            watch_path: dest.clone(),
            block: false,
        });
        let mut entry = DownloadEntry::new("magnet:?xt=urn:btih:x".into(), dest.clone(), DownloadKind::Http);
        entry.backend_id = "torrent".into();
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = manager_with_stub(store.clone(), stub.clone());
        assert!(manager.remove_entry(&id, true).await.unwrap());
        assert!(stub.removed.load(Ordering::SeqCst), "on_remove not called");
        assert!(stub.remove_saw_file.load(Ordering::SeqCst), "on_remove ran after file was deleted");
        assert!(tokio::fs::metadata(&dest).await.is_err(), "file should be gone afterwards");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn pause_entry_calls_on_pause_for_a_torrent() {
        let dir = std::env::temp_dir().join(format!("luedd-mgr-tor-pause-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());

        let stub = Arc::new(StubTorrent {
            paused: Arc::new(AtomicBool::new(false)),
            removed: Arc::new(AtomicBool::new(false)),
            remove_saw_file: Arc::new(AtomicBool::new(false)),
            watch_path: dir.clone(),
            block: true,
        });
        let mut entry = DownloadEntry::new("magnet:?xt=urn:btih:y".into(), dir.join("o.bin"), DownloadKind::Http);
        entry.backend_id = "torrent".into();
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let manager = Arc::new(manager_with_stub(store.clone(), stub.clone()));
        let m2 = manager.clone();
        let id2 = id.clone();
        tokio::spawn(async move { m2.run_entry_now(&id2).await.ok(); });

        for _ in 0..100 {
            if matches!(store.get_entry(&id).await.unwrap().status, DownloadStatus::Downloading) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(manager.pause_entry(&id).await.unwrap());
        assert!(stub.paused.load(Ordering::SeqCst), "on_pause not called");
        assert!(matches!(store.get_entry(&id).await.unwrap().status, DownloadStatus::Paused));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
