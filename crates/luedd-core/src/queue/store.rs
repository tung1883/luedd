use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::model::{DownloadEntry, DownloadProgress, DownloadQueueDef, DownloadStatus};

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreData {
    entries: Vec<DownloadEntry>,
    queues: Vec<DownloadQueueDef>,
}

pub struct DownloadStore {
    path: PathBuf,
    data: RwLock<StoreData>,
    /// Set by an in-memory-only change (download progress). A later `save()`
    /// from any write, or `flush()` from the scheduler, persists it.
    dirty: AtomicBool,
}

impl DownloadStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(d) => d,
                Err(e) => {
                    // A torn write (or a hand-edit) left the file unparseable.
                    // Don't refuse to start — move it aside and begin fresh, so
                    // the app is usable again. The queue rebuilds as downloads
                    // are re-added; finished files on disk are untouched.
                    let bak = path.with_extension("json.corrupt");
                    let _ = tokio::fs::rename(&path, &bak).await;
                    tracing::error!(error = %e, backup = %bak.display(), "download store unparseable; started empty");
                    StoreData::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => StoreData::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data), dirty: AtomicBool::new(false) })
    }

    async fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&*self.data.read().await)?;
        let r = crate::atomicfile::write_atomic(&self.path, &json).await;
        if r.is_ok() {
            self.dirty.store(false, Ordering::Relaxed);
        }
        r
    }

    /// Persist only if an in-memory-only change is pending. Called on a timer by
    /// the scheduler so coalesced progress eventually lands on disk.
    pub async fn flush(&self) -> Result<()> {
        if self.dirty.swap(false, Ordering::Relaxed) {
            self.save().await
        } else {
            Ok(())
        }
    }

    /// Update a download's progress in memory WITHOUT writing the file. Progress
    /// is high-frequency and disposable (a crash re-queues `Downloading` rows),
    /// so a full-store serialize per tick is pure overhead — this just marks the
    /// store dirty for the next `flush()`.
    pub async fn set_progress(&self, id: &str, progress: DownloadProgress) {
        {
            let mut data = self.data.write().await;
            if let Some(e) = data.entries.iter_mut().find(|e| e.id == id) {
                e.progress = Some(progress);
            } else {
                return;
            }
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub async fn add_entry(&self, entry: DownloadEntry) -> Result<()> {
        self.data.write().await.entries.push(entry);
        self.save().await
    }

    pub async fn update_entry(&self, id: &str, f: impl FnOnce(&mut DownloadEntry)) -> Result<()> {
        {
            let mut data = self.data.write().await;
            if let Some(entry) = data.entries.iter_mut().find(|e| e.id == id) {
                f(entry);
            }
        }
        self.save().await
    }

    pub async fn list_entries(&self) -> Vec<DownloadEntry> {
        self.data.read().await.entries.clone()
    }

    /// Apply `f` to every entry matching `pred`, then persist ONCE. Returns the
    /// ids touched. Use instead of a loop of `update_entry` calls so a bulk
    /// change (pause a whole profile's downloads) is a single file write.
    pub async fn update_where(
        &self,
        pred: impl Fn(&DownloadEntry) -> bool,
        f: impl Fn(&mut DownloadEntry),
    ) -> Result<Vec<String>> {
        let touched = {
            let mut data = self.data.write().await;
            let mut ids = Vec::new();
            for e in data.entries.iter_mut() {
                if pred(e) {
                    f(e);
                    ids.push(e.id.clone());
                }
            }
            ids
        };
        if !touched.is_empty() {
            self.save().await?;
        }
        Ok(touched)
    }

    /// Remove every entry matching `pred`, persisting ONCE. Returns the removed
    /// entries so the caller can clean up their artifacts if it wants to.
    pub async fn remove_where(
        &self,
        pred: impl Fn(&DownloadEntry) -> bool,
    ) -> Result<Vec<DownloadEntry>> {
        let removed = {
            let mut data = self.data.write().await;
            let (keep, removed): (Vec<_>, Vec<_>) =
                data.entries.drain(..).partition(|e| !pred(e));
            data.entries = keep;
            removed
        };
        if !removed.is_empty() {
            self.save().await?;
        }
        Ok(removed)
    }

    /// Atomically move a still-`Queued` entry to `Downloading`. Returns `false`
    /// when it was already claimed by another runner or is gone — the caller
    /// must then not run it. Guards against two overlapping `run_queued` sweeps
    /// starting the same download twice.
    pub async fn try_claim(&self, id: &str) -> Result<bool> {
        let claimed = {
            let mut data = self.data.write().await;
            match data.entries.iter_mut().find(|e| e.id == id) {
                Some(e) if e.status == DownloadStatus::Queued => {
                    e.status = DownloadStatus::Downloading;
                    e.error = None;
                    e.next_retry_at = None;
                    true
                }
                _ => false,
            }
        };
        if claimed {
            self.save().await?;
        }
        Ok(claimed)
    }

    pub async fn get_entry(&self, id: &str) -> Option<DownloadEntry> {
        self.data.read().await.entries.iter().find(|e| e.id == id).cloned()
    }

    pub async fn add_queue(&self, queue: DownloadQueueDef) -> Result<()> {
        self.data.write().await.queues.push(queue);
        self.save().await
    }

    pub async fn list_queues(&self) -> Vec<DownloadQueueDef> {
        self.data.read().await.queues.clone()
    }

    pub async fn remove_entry(&self, id: &str) -> Result<Option<DownloadEntry>> {
        let removed = {
            let mut data = self.data.write().await;
            let pos = data.entries.iter().position(|e| e.id == id);
            pos.map(|i| data.entries.remove(i))
        };
        self.save().await?;
        Ok(removed)
    }

    pub async fn retry_entry(&self, id: &str) -> Result<bool> {
        let mut retried = false;
        self.update_entry(id, |e| {
            if matches!(
                e.status,
                DownloadStatus::Failed
                    | DownloadStatus::Cancelled
                    | DownloadStatus::Paused
                    | DownloadStatus::Converting
            ) {
                e.status = DownloadStatus::Queued;
                e.error = None;
                e.progress = None;
                e.retry_count = 0;
                e.next_retry_at = None;
                retried = true;
            }
        })
        .await?;
        Ok(retried)
    }

    pub async fn clear_finished(&self) -> Result<Vec<DownloadEntry>> {
        let removed = {
            let mut data = self.data.write().await;
            let (keep, removed): (Vec<_>, Vec<_>) = data.entries.drain(..).partition(|e| {
                matches!(
                    e.status,
                    DownloadStatus::Queued
                        | DownloadStatus::Downloading
                        | DownloadStatus::Converting
                        | DownloadStatus::Paused
                )
            });
            data.entries = keep;
            removed
        };
        self.save().await?;
        Ok(removed)
    }
}

pub fn default_store_path(data_dir: &Path) -> PathBuf {
    data_dir.join("downloads.json")
}

pub fn default_data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("LUEDD_DATA_DIR") {
        return PathBuf::from(dir);
    }
    let exe_dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.to_path_buf()));
    exe_dir.unwrap_or_else(|| PathBuf::from(".")).join("data")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::DownloadKind;
    use crate::queue::model::DownloadStatus;

    #[tokio::test]
    async fn persists_and_reloads_entries() {
        let dir = std::env::temp_dir().join(format!("luedd-store-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("downloads.json");

        {
            let store = DownloadStore::open(&path).await.unwrap();
            let entry = DownloadEntry::new("http://x/f.bin".into(), "/tmp/f.bin".into(), DownloadKind::Http);
            let id = entry.id.clone();
            store.add_entry(entry).await.unwrap();
            store.update_entry(&id, |e| e.status = DownloadStatus::Finished).await.unwrap();
        }

        let reopened = DownloadStore::open(&path).await.unwrap();
        let entries = reopened.list_entries().await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].status, DownloadStatus::Finished));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn opens_empty_when_file_missing() {
        let path = std::env::temp_dir().join(format!("luedd-store-missing-{}.json", std::process::id()));
        let store = DownloadStore::open(&path).await.unwrap();
        assert!(store.list_entries().await.is_empty());
    }

    #[tokio::test]
    async fn remove_entry_deletes_and_returns_it() {
        let dir = std::env::temp_dir().join(format!("luedd-store-remove-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();
        let entry = DownloadEntry::new("http://x/f.bin".into(), "/tmp/f.bin".into(), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let removed = store.remove_entry(&id).await.unwrap();
        assert_eq!(removed.unwrap().id, id);
        assert!(store.list_entries().await.is_empty());
        assert!(store.remove_entry(&id).await.unwrap().is_none());

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn retry_entry_only_resets_failed_or_cancelled() {
        let dir = std::env::temp_dir().join(format!("luedd-store-retry-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();

        let failed = DownloadEntry::new("http://x/a".into(), "/tmp/a".into(), DownloadKind::Http);
        let failed_id = failed.id.clone();
        store.add_entry(failed).await.unwrap();
        store
            .update_entry(&failed_id, |e| {
                e.status = DownloadStatus::Failed;
                e.error = Some("boom".into());
            })
            .await
            .unwrap();

        let running = DownloadEntry::new("http://x/b".into(), "/tmp/b".into(), DownloadKind::Http);
        let running_id = running.id.clone();
        store.add_entry(running).await.unwrap();
        store.update_entry(&running_id, |e| e.status = DownloadStatus::Downloading).await.unwrap();

        assert!(store.retry_entry(&failed_id).await.unwrap());
        let entry = store.get_entry(&failed_id).await.unwrap();
        assert!(matches!(entry.status, DownloadStatus::Queued));
        assert!(entry.error.is_none());

        assert!(!store.retry_entry(&running_id).await.unwrap());
        let entry = store.get_entry(&running_id).await.unwrap();
        assert!(matches!(entry.status, DownloadStatus::Downloading));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn clear_finished_removes_terminal_states_only() {
        let dir = std::env::temp_dir().join(format!("luedd-store-clear-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();

        let mut ids = vec![];
        for status in [DownloadStatus::Queued, DownloadStatus::Downloading, DownloadStatus::Finished, DownloadStatus::Failed] {
            let entry = DownloadEntry::new("http://x/f".into(), "/tmp/f".into(), DownloadKind::Http);
            let id = entry.id.clone();
            store.add_entry(entry).await.unwrap();
            store.update_entry(&id, |e| e.status = status).await.unwrap();
            ids.push((id, status));
        }

        let removed = store.clear_finished().await.unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed.iter().all(|e| matches!(e.status, DownloadStatus::Finished | DownloadStatus::Failed)));

        let remaining = store.list_entries().await;
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|e| matches!(e.status, DownloadStatus::Queued | DownloadStatus::Downloading)));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn set_progress_is_in_memory_until_flush() {
        let dir = std::env::temp_dir().join(format!("luedd-store-prog-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();
        let entry = DownloadEntry::new("http://x/f".into(), "/tmp/f".into(), DownloadKind::Http);
        let id = entry.id.clone();
        store.add_entry(entry).await.unwrap();

        let p = DownloadProgress {
            downloaded_bytes: 10,
            total_bytes: Some(100),
            done_units: 1,
            total_units: 5,
            speed_bps: 1,
        };
        store.set_progress(&id, p).await;
        // visible in memory immediately
        assert_eq!(store.get_entry(&id).await.unwrap().progress.unwrap().downloaded_bytes, 10);
        // not yet on disk
        let cold = DownloadStore::open(dir.join("downloads.json")).await.unwrap();
        assert!(cold.get_entry(&id).await.unwrap().progress.is_none());
        // flush persists it
        store.flush().await.unwrap();
        let warm = DownloadStore::open(dir.join("downloads.json")).await.unwrap();
        assert_eq!(warm.get_entry(&id).await.unwrap().progress.unwrap().downloaded_bytes, 10);

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn update_where_touches_only_matches_and_persists_once() {
        let dir = std::env::temp_dir().join(format!("luedd-store-uw-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();

        for (author, status) in [
            (Some("@a"), DownloadStatus::Queued),
            (Some("@a"), DownloadStatus::Downloading),
            (Some("@b"), DownloadStatus::Queued),
            (None, DownloadStatus::Queued),
        ] {
            let entry = DownloadEntry::new("http://x/f".into(), "/tmp/f".into(), DownloadKind::Http);
            let id = entry.id.clone();
            store.add_entry(entry).await.unwrap();
            store
                .update_entry(&id, |e| {
                    e.status = status;
                    e.author = author.map(str::to_string);
                })
                .await
                .unwrap();
        }

        let touched = store
            .update_where(
                |e| e.author.as_deref() == Some("@a") && e.status == DownloadStatus::Queued,
                |e| e.status = DownloadStatus::Paused,
            )
            .await
            .unwrap();
        assert_eq!(touched.len(), 1);

        let reopened = DownloadStore::open(dir.join("downloads.json")).await.unwrap();
        let paused = reopened
            .list_entries()
            .await
            .into_iter()
            .filter(|e| e.status == DownloadStatus::Paused)
            .count();
        assert_eq!(paused, 1, "only the one @a/Queued entry, and it survived a reload");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn remove_where_drops_matches_keeps_rest() {
        let dir = std::env::temp_dir().join(format!("luedd-store-rw-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();

        for status in [DownloadStatus::Queued, DownloadStatus::Paused, DownloadStatus::Finished] {
            let entry = DownloadEntry::new("http://x/f".into(), "/tmp/f".into(), DownloadKind::Http);
            let id = entry.id.clone();
            store.add_entry(entry).await.unwrap();
            store.update_entry(&id, |e| e.status = status).await.unwrap();
        }

        let removed = store
            .remove_where(|e| !matches!(e.status, DownloadStatus::Finished))
            .await
            .unwrap();
        assert_eq!(removed.len(), 2);

        let remaining = store.list_entries().await;
        assert_eq!(remaining.len(), 1);
        assert!(matches!(remaining[0].status, DownloadStatus::Finished));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
