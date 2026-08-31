use std::sync::Arc;
use std::time::Duration;

use chrono::Timelike;

use super::manager::DownloadManager;
use super::model::{now_unix, DownloadStatus};
use super::store::DownloadStore;

const CHECK_INTERVAL: Duration = Duration::from_secs(15);

pub const MAX_AUTO_RETRIES: u32 = 3;

const RETRY_BACKOFF: [Duration; MAX_AUTO_RETRIES as usize] =
    [Duration::from_secs(30), Duration::from_secs(120), Duration::from_secs(600)];

pub fn next_auto_retry_at(retry_count: u32) -> Option<i64> {
    let backoff = RETRY_BACKOFF.get(retry_count as usize)?;
    Some(now_unix() + backoff.as_secs() as i64)
}

pub async fn run_forever(store: Arc<DownloadStore>, manager: Arc<DownloadManager>) -> ! {
    requeue_interrupted(&store).await;
    loop {
        tick(&store, &manager).await;
        tokio::time::sleep(CHECK_INTERVAL).await;
    }
}

/// A `Downloading` entry can only be stale on startup - the task that owned it
/// died with the previous process. Put it back in the queue; the download
/// resumes from its checkpoint (HTTP `state.json`) or its already-fetched
/// segments (HLS/DASH), so no progress is lost.
///
/// `Converting` is left alone: all its segments are already downloaded and only
/// the mux was interrupted, but re-running the job would re-walk the whole
/// download. Better to leave it for the user to retry explicitly than to silently
/// restart a nearly-finished video.
async fn requeue_interrupted(store: &DownloadStore) {
    for entry in store.list_entries().await {
        if matches!(entry.status, DownloadStatus::Downloading) {
            tracing::info!(id = %entry.id, "re-queuing a download interrupted by a previous shutdown");
            store
                .update_entry(&entry.id, |e| {
                    e.status = DownloadStatus::Queued;
                    e.retry_count = 0;
                    e.next_retry_at = None;
                })
                .await
                .ok();
        }
    }
}

async fn tick(store: &DownloadStore, manager: &DownloadManager) {
    auto_retry_due_entries(store, manager).await;

    let now = chrono::Local::now();
    let minutes_since_midnight = now.hour() * 60 + now.minute();

    let queues = store.list_queues().await;
    let any_active = queues.iter().any(|q| match &q.schedule {
        Some(s) => s.is_active_at(minutes_since_midnight),
        None => true,
    });

    if any_active || queues.is_empty() {
        manager.run_queued().await.ok();
    }
}

async fn auto_retry_due_entries(store: &DownloadStore, manager: &DownloadManager) {
    let now = now_unix();
    for entry in store.list_entries().await {
        if !matches!(entry.status, DownloadStatus::Failed) {
            continue;
        }
        let Some(next_retry_at) = entry.next_retry_at else { continue };
        if now < next_retry_at || entry.retry_count >= MAX_AUTO_RETRIES {
            continue;
        }
        let new_count = entry.retry_count + 1;
        if manager.retry_entry(&entry.id).await.unwrap_or(false) {
            store.update_entry(&entry.id, |e| e.retry_count = new_count).await.ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::DownloadSchedule;
    use super::*;

    #[tokio::test]
    async fn requeue_interrupted_revives_only_downloading() {
        use crate::jobs::DownloadKind;
        use crate::queue::model::DownloadEntry;

        let dir = std::env::temp_dir().join(format!("luedd-sched-requeue-{}", std::process::id()));
        let store = DownloadStore::open(dir.join("downloads.json")).await.unwrap();

        let mut ids = Vec::new();
        for status in [
            DownloadStatus::Downloading,
            DownloadStatus::Converting,
            DownloadStatus::Paused,
            DownloadStatus::Finished,
            DownloadStatus::Failed,
        ] {
            let entry = DownloadEntry::new("http://x/f".into(), "/tmp/f".into(), DownloadKind::Http);
            let id = entry.id.clone();
            store.add_entry(entry).await.unwrap();
            store
                .update_entry(&id, |e| {
                    e.status = status;
                    e.retry_count = 2;
                })
                .await
                .unwrap();
            ids.push((id, status));
        }

        requeue_interrupted(&store).await;

        for (id, original) in ids {
            let now = store.get_entry(&id).await.unwrap();
            if original == DownloadStatus::Downloading {
                assert_eq!(now.status, DownloadStatus::Queued, "Downloading should be re-queued");
                assert_eq!(now.retry_count, 0);
            } else {
                assert_eq!(now.status, original, "{original:?} should be untouched (esp. Converting)");
            }
        }

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[test]
    fn schedule_gates_correctly_at_boundaries() {
        let business_hours = DownloadSchedule { start_minutes: 9 * 60, end_minutes: 18 * 60 };
        assert!(!business_hours.is_active_at(0));
        assert!(business_hours.is_active_at(9 * 60));
        assert!(!business_hours.is_active_at(18 * 60));
    }

    #[test]
    fn next_auto_retry_at_grows_with_each_attempt() {
        let first = next_auto_retry_at(0).unwrap();
        let second = next_auto_retry_at(1).unwrap();
        let third = next_auto_retry_at(2).unwrap();
        let now = now_unix();
        assert!(first - now <= 30 && first - now > 0);
        assert!(second - first >= 60, "second backoff should be much longer than the first");
        assert!(third - second >= 300, "third backoff should be much longer than the second");
    }

    #[test]
    fn next_auto_retry_at_stops_once_max_retries_exhausted() {
        assert!(next_auto_retry_at(MAX_AUTO_RETRIES).is_none());
        assert!(next_auto_retry_at(MAX_AUTO_RETRIES + 1).is_none());
    }
}
