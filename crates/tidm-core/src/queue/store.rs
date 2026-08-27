use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::model::{DownloadEntry, DownloadQueueDef, DownloadStatus};

/// On-disk persistence for the download list and queues, the Rust equivalent of
/// XDM's `DataAccess.DownloadList`/`QueueManager` (a plain versioned JSON file
/// instead of their hand-rolled binary format - no compatibility requirement
/// with old XDM state, so a human-readable format is strictly better here).
#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreData {
    entries: Vec<DownloadEntry>,
    queues: Vec<DownloadQueueDef>,
}

pub struct DownloadStore {
    path: PathBuf,
    data: RwLock<StoreData>,
}

impl DownloadStore {
    /// Loads the store from `path` if it exists, otherwise starts empty.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).context("corrupt download store file")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => StoreData::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data) })
    }

    async fn save(&self) -> Result<()> {
        let data = self.data.read().await;
        let json = serde_json::to_vec_pretty(&*data)?;
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&self.path, json).await?;
        Ok(())
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

    /// Removes one entry regardless of its status. Callers that also want its
    /// on-disk output/temp files removed (not this store's concern - it only
    /// owns the persisted list) should do so with the returned entry's `dest`.
    pub async fn remove_entry(&self, id: &str) -> Result<Option<DownloadEntry>> {
        let removed = {
            let mut data = self.data.write().await;
            let pos = data.entries.iter().position(|e| e.id == id);
            pos.map(|i| data.entries.remove(i))
        };
        self.save().await?;
        Ok(removed)
    }

    /// Resets a `Failed`, `Cancelled`, or `Paused` entry back to `Queued` so
    /// the next `run_queued` picks it up again (also used as "Resume" for a
    /// paused entry - same transition, the GUI just labels the button
    /// differently). No-op (returns `false`) for any other status. Also
    /// clears `retry_count`/`next_retry_at` - a manual retry always gets a
    /// fresh set of auto-retries if it fails again (the scheduler's own
    /// auto-retry path calls this too, then re-sets `retry_count` itself
    /// right after, since this always resets it to 0 first).
    pub async fn retry_entry(&self, id: &str) -> Result<bool> {
        let mut retried = false;
        self.update_entry(id, |e| {
            if matches!(e.status, DownloadStatus::Failed | DownloadStatus::Cancelled | DownloadStatus::Paused) {
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

    /// Removes every `Finished`/`Failed`/`Cancelled` entry from the list and
    /// returns them, so the caller can optionally delete their output/temp
    /// files - this store only owns the persisted list, not the filesystem.
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

/// Shared default data directory for CLI and GUI alike, so both see the same
/// download list unless `TIDM_DATA_DIR` is set.
///
/// Portable by design: everything (`downloads.json`, `settings.json`, the
/// `downloads/` folder, and per-download temp/state files, since those are all
/// computed relative to a download's own `dest`) lives in a `data` folder next
/// to the running executable, not in the system temp directory - a system temp
/// dir gets periodically wiped and isn't something you can just copy/move the
/// app folder and keep your downloads/settings with it, which is the whole
/// point of "portable." Falls back to the current working directory if the
/// executable's own path can't be resolved (should not happen in practice).
pub fn default_data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("TIDM_DATA_DIR") {
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
        let dir = std::env::temp_dir().join(format!("tidm-store-test-{}", std::process::id()));
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
        let path = std::env::temp_dir().join(format!("tidm-store-missing-{}.json", std::process::id()));
        let store = DownloadStore::open(&path).await.unwrap();
        assert!(store.list_entries().await.is_empty());
    }

    #[tokio::test]
    async fn remove_entry_deletes_and_returns_it() {
        let dir = std::env::temp_dir().join(format!("tidm-store-remove-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("tidm-store-retry-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("tidm-store-clear-{}", std::process::id()));
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
}
