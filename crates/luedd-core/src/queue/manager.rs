use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use luedd_net::{HttpClient, RequestContext};
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use super::model::{DownloadEntry, DownloadStatus};
use super::store::DownloadStore;
use crate::jobs;

type RunningTasks = Arc<StdMutex<HashMap<String, AbortHandle>>>;

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

    pub async fn run_entry_now(&self, id: &str) -> anyhow::Result<()> {
        let Some(entry) = self.store.get_entry(id).await else {
            anyhow::bail!("no entry with id {id}");
        };
        if !matches!(entry.status, DownloadStatus::Queued) {
            return Ok(());
        }
        run_tracked(self.store.clone(), self.client.clone(), entry, self.per_download_concurrency, self.running.clone()).await;
        Ok(())
    }

    pub async fn pause_entry(&self, id: &str) -> anyhow::Result<bool> {
        let handle = { self.running.lock().unwrap().remove(id) };
        let Some(handle) = handle else {
            return Ok(false);
        };
        handle.abort();
        self.store.update_entry(id, |e| e.status = DownloadStatus::Paused).await?;
        Ok(true)
    }

    pub async fn remove_entry(&self, id: &str, delete_files: bool) -> anyhow::Result<bool> {
        let removed = self.store.remove_entry(id).await?;
        if let Some(entry) = &removed {
            if delete_files {
                delete_artifacts(&entry.dest).await;
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
                delete_artifacts(&entry.dest).await;
            }
        }
        Ok(count)
    }
}

async fn run_tracked(store: Arc<DownloadStore>, client: HttpClient, entry: DownloadEntry, concurrency: usize, running: RunningTasks) {
    let id = entry.id.clone();
    let handle = tokio::spawn(async move {
        run_single(&store, &client, entry, concurrency).await;
    });
    running.lock().unwrap().insert(id.clone(), handle.abort_handle());
    let _ = handle.await;
    running.lock().unwrap().remove(&id);
}

async fn run_single(store: &Arc<DownloadStore>, client: &HttpClient, entry: DownloadEntry, concurrency: usize) {
    store.update_entry(&entry.id, |e| e.status = DownloadStatus::Downloading).await.ok();

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress_store = store.clone();
    let progress_id = entry.id.clone();
    let progress_task = tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            match event {
                luedd_net::JobEvent::Progress { done, total } => {
                    progress_store.update_entry(&progress_id, |e| e.progress = Some((done, total))).await.ok();
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
    let result =
        jobs::run(client, entry.kind, &entry.url, &entry.dest, concurrency, &ctx, Some(&progress_tx), entry.quality.as_deref()).await;
    drop(progress_tx);
    progress_task.await.ok();

    match result {
        Ok(final_dest) => {
            store
                .update_entry(&entry.id, |e| {
                    e.status = DownloadStatus::Finished;
                    e.dest = final_dest;
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

async fn delete_artifacts(dest: &std::path::Path) {
    tokio::fs::remove_file(dest).await.ok();
    tokio::fs::remove_dir_all(crate::naming::cache_dir_for(dest)).await.ok();
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
}
