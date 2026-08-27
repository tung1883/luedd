use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tidm_net::{HttpClient, RequestContext};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use super::model::{DownloadEntry, DownloadStatus};
use super::store::DownloadStore;
use crate::jobs;

/// Currently-running entries by id, so `pause_entry` can find and abort the
/// right task. A plain `std::sync::Mutex` is enough - every critical section
/// is a single non-blocking HashMap operation, never held across an `.await`.
type RunningTasks = Arc<StdMutex<HashMap<String, AbortHandle>>>;

/// Runs queued entries with up to `max_concurrent` running at once, the Rust
/// equivalent of `QueueManager`/`AppController` dispatching downloads. Each
/// entry's actual transfer goes through `jobs::run`, the same code path
/// `tidm-cli` and the GUI both use.
pub struct DownloadManager {
    store: Arc<DownloadStore>,
    client: HttpClient,
    semaphore: Arc<Semaphore>,
    per_download_concurrency: usize,
    running: RunningTasks,
}

impl DownloadManager {
    pub fn new(store: Arc<DownloadStore>, client: HttpClient, max_concurrent: usize, per_download_concurrency: usize) -> Self {
        Self {
            store,
            client,
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
            per_download_concurrency,
            running: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Lets callers that need to probe a URL before queuing it (real-extension
    /// detection in `naming::resolve_real_extension`) reuse the manager's own
    /// client rather than building a throwaway one.
    pub fn http_client(&self) -> HttpClient {
        self.client.clone()
    }

    /// Runs every entry currently `Queued`, respecting `max_concurrent`. Returns
    /// once all of them have finished (successfully or not) - callers that want
    /// a persistent background queue should call this from a loop or spawn it.
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

            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                run_tracked(store, client, entry, concurrency, running).await;
            }));
        }

        for task in tasks {
            task.await.ok();
        }
        Ok(())
    }

    /// Runs one entry immediately, bypassing the `max_concurrent` queue-scan
    /// gate entirely (still uses the same `per_download_concurrency` for its
    /// own internal connections). For URLs from short-lived signed links
    /// (common on video CDNs - a token good for only a few minutes), waiting
    /// for a separate "run the queue" step to notice a newly `Queued` entry is
    /// exactly the kind of delay that expires the token before the download
    /// ever starts; the browser extension's `/vid` click uses this instead of
    /// just enqueuing so the download starts the instant it's requested.
    pub async fn run_entry_now(&self, id: &str) -> anyhow::Result<()> {
        let Some(entry) = self.store.get_entry(id).await else {
            anyhow::bail!("no entry with id {id}");
        };
        if !matches!(entry.status, DownloadStatus::Queued) {
            return Ok(()); // already running/finished/failed - nothing to do
        }
        run_tracked(self.store.clone(), self.client.clone(), entry, self.per_download_concurrency, self.running.clone()).await;
        Ok(())
    }

    /// Aborts a currently-running entry's transfer and marks it `Paused`
    /// rather than `Failed`/`Cancelled`, so "Resume" (the same transition as
    /// `retry_entry`) picks it back up later. Returns `false` if the entry
    /// isn't actually running right now (already finished, or never started).
    ///
    /// The abort is a hard cancellation, not a graceful stop - whatever byte
    /// range or segment was in flight is simply dropped. For a plain HTTP
    /// download this is fine: progress is checkpointed periodically and
    /// resuming picks up from the last checkpoint. HLS/DASH have no
    /// segment-level resume yet, so pausing one of those restarts its segment
    /// downloads from scratch on resume - wasteful but not broken.
    pub async fn pause_entry(&self, id: &str) -> anyhow::Result<bool> {
        let handle = { self.running.lock().unwrap().remove(id) };
        let Some(handle) = handle else {
            return Ok(false);
        };
        handle.abort();
        self.store.update_entry(id, |e| e.status = DownloadStatus::Paused).await?;
        Ok(true)
    }

    /// Removes one entry from the list; pass `delete_files: true` to also best-
    /// effort delete its output file and any leftover temp/cache artifacts.
    pub async fn remove_entry(&self, id: &str, delete_files: bool) -> anyhow::Result<bool> {
        let removed = self.store.remove_entry(id).await?;
        if let Some(entry) = &removed {
            if delete_files {
                delete_artifacts(&entry.dest).await;
            }
        }
        Ok(removed.is_some())
    }

    /// Resets a failed/cancelled entry back to `Queued`.
    pub async fn retry_entry(&self, id: &str) -> anyhow::Result<bool> {
        self.store.retry_entry(id).await
    }

    /// Removes every Finished/Failed/Cancelled entry; pass `delete_files: true`
    /// to also best-effort delete each one's output file and any leftover
    /// temp/cache artifacts (assembled-segment temp files, `.segments` dirs,
    /// resumable-download state files).
    pub async fn clear_finished(&self, delete_files: bool) -> anyhow::Result<usize> {
        let removed = self.store.clear_finished().await?;
        let count = removed.len();
        if delete_files {
            for entry in &removed {
                delete_artifacts(&entry.dest).await;
            }
        }
        Ok(count)
    }
}

/// Wraps `run_single` in its own task and registers that task's `AbortHandle`
/// in `running` for the duration, so `DownloadManager::pause_entry` can find
/// and cancel it by id. Insertion happens strictly before removal from this
/// function's own sequential control flow regardless of how fast the inner
/// task actually finishes - `handle.await` is what drives the removal, not
/// the inner task removing itself - so there's no race where a fast-failing
/// download could leave a stale (or missing) map entry.
async fn run_tracked(store: Arc<DownloadStore>, client: HttpClient, entry: DownloadEntry, concurrency: usize, running: RunningTasks) {
    let id = entry.id.clone();
    let handle = tokio::spawn(async move {
        run_single(&store, &client, entry, concurrency).await;
    });
    running.lock().unwrap().insert(id.clone(), handle.abort_handle());
    let _ = handle.await;
    running.lock().unwrap().remove(&id);
}

/// Runs one entry to completion, updating its status/progress/error in the
/// store as it goes. Shared by the batch `run_queued` path and the immediate
/// `run_entry_now` path so both behave identically.
async fn run_single(store: &Arc<DownloadStore>, client: &HttpClient, entry: DownloadEntry, concurrency: usize) {
    store.update_entry(&entry.id, |e| e.status = DownloadStatus::Downloading).await.ok();

    // Forward progress/phase events into the persisted entry so the GUI sees
    // them the same way it already polls status - a separate task rather
    // than an inline await so `jobs::run` never blocks on the store write.
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress_store = store.clone();
    let progress_id = entry.id.clone();
    let progress_task = tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            match event {
                tidm_net::JobEvent::Progress { done, total } => {
                    progress_store.update_entry(&progress_id, |e| e.progress = Some((done, total))).await.ok();
                }
                tidm_net::JobEvent::Converting => {
                    progress_store.update_entry(&progress_id, |e| e.status = DownloadStatus::Converting).await.ok();
                }
            }
        }
    });

    let ctx = RequestContext { headers: entry.headers.clone(), cookie: entry.cookie.clone() };
    let result = jobs::run(client, entry.kind, &entry.url, &entry.dest, concurrency, &ctx, Some(&progress_tx)).await;
    drop(progress_tx);
    progress_task.await.ok();

    match result {
        Ok(()) => {
            store.update_entry(&entry.id, |e| e.status = DownloadStatus::Finished).await.ok();
        }
        Err(e) => {
            // `e.to_string()` (equivalent to `{e}`) shows only the outermost
            // context ("GET {url} failed") and drops every underlying cause -
            // exactly the layer that actually explains *why* (DNS failure,
            // TLS handshake failure, connection refused/reset, timeout, a
            // real HTTP status...). The alternate Display (`{:#}`) chains all
            // of them together, which is what actually gets shown in the GUI.
            let msg = format!("{e:#}");
            store
                .update_entry(&entry.id, |entry| {
                    entry.status = DownloadStatus::Failed;
                    entry.error = Some(msg);
                })
                .await
                .ok();
        }
    }
}

/// Best-effort deletion of `dest` plus every temp/cache artifact a download of
/// any kind might have left behind, keyed off the naming conventions each
/// downloader uses (`tidm_core::progressive`'s `.tidm-state.json` sidecar,
/// `tidm_media`'s `.{filename}.segments` dirs and `.tmp`/`.tmp.ts` intermediate
/// files for muxed/demuxed HLS and DASH). Missing files are silently ignored -
/// this is cleanup, not a correctness-critical operation.
async fn delete_artifacts(dest: &std::path::Path) {
    tokio::fs::remove_file(dest).await.ok();

    let mut state_path = dest.as_os_str().to_owned();
    state_path.push(".tidm-state.json");
    tokio::fs::remove_file(&state_path).await.ok();

    let intermediate_suffixes =
        ["tmp", "tmp.ts", "video.tmp", "audio.tmp", "video.tmp.ts", "audio.tmp.ts"];
    for suffix in intermediate_suffixes {
        let intermediate = dest.with_extension(suffix);
        tokio::fs::remove_file(&intermediate).await.ok();

        if let Some(name) = intermediate.file_name() {
            let segments_dir = intermediate.with_file_name(format!(".{}.segments", name.to_string_lossy()));
            tokio::fs::remove_dir_all(&segments_dir).await.ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::DownloadKind;
    use crate::queue::model::DownloadEntry;

    #[tokio::test]
    async fn run_entry_now_runs_without_a_separate_run_queued_call() {
        let dir = std::env::temp_dir().join(format!("tidm-manager-runnow-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("tidm-manager-runnow-noop-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("tidm-manager-pause-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        // Accepts connections but never writes a response, so the client's
        // request sits waiting indefinitely - a reliable window to pause the
        // download mid-flight without racing a real transfer's completion.
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
        let dir = std::env::temp_dir().join(format!("tidm-manager-test-{}", std::process::id()));
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
    async fn clear_finished_with_delete_files_removes_output_and_state_file() {
        let dir = std::env::temp_dir().join(format!("tidm-manager-clear-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let store = Arc::new(DownloadStore::open(dir.join("downloads.json")).await.unwrap());
        let client = HttpClient::new().unwrap();

        let dest = dir.join("out.bin");
        tokio::fs::write(&dest, b"content").await.unwrap();
        let mut state_path = dest.as_os_str().to_owned();
        state_path.push(".tidm-state.json");
        tokio::fs::write(&state_path, b"{}").await.unwrap();

        let entry = DownloadEntry::new("http://x/f".into(), dest.clone(), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();
        store.update_entry(&id, |e| e.status = DownloadStatus::Finished).await.unwrap();

        let manager = DownloadManager::new(store.clone(), client, 2, 2);
        let removed_count = manager.clear_finished(true).await.unwrap();

        assert_eq!(removed_count, 1);
        assert!(store.list_entries().await.is_empty());
        assert!(tokio::fs::metadata(&dest).await.is_err(), "output file should be deleted");
        assert!(tokio::fs::metadata(&state_path).await.is_err(), "state sidecar should be deleted");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn remove_entry_without_delete_files_keeps_output_on_disk() {
        let dir = std::env::temp_dir().join(format!("tidm-manager-remove-{}", std::process::id()));
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
}
