use std::sync::Arc;
use std::time::Duration;

use chrono::Timelike;

use super::manager::DownloadManager;
use super::model::{now_unix, DownloadStatus};
use super::store::DownloadStore;

const CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// How many times a `Failed` entry is automatically retried before being left
/// alone for the user to retry manually. Indexes `RETRY_BACKOFF` below.
pub const MAX_AUTO_RETRIES: u32 = 3;

/// Delay before each successive auto-retry attempt, indexed by the entry's
/// `retry_count` *before* that attempt (so the first auto-retry after a fresh
/// failure waits `RETRY_BACKOFF[0]`). Mirrors the growing-delay shape of
/// `tidm_net::retry`'s per-request backoff, just at the whole-download level
/// and persisted (so it survives an app restart) rather than an in-memory loop.
const RETRY_BACKOFF: [Duration; MAX_AUTO_RETRIES as usize] =
    [Duration::from_secs(30), Duration::from_secs(120), Duration::from_secs(600)];

/// How long after a failure a `Failed` entry becomes eligible for its next
/// auto-retry, or `None` once `MAX_AUTO_RETRIES` auto-attempts are exhausted
/// (only a manual retry - which resets `retry_count` - schedules another one).
pub fn next_auto_retry_at(retry_count: u32) -> Option<i64> {
    let backoff = RETRY_BACKOFF.get(retry_count as usize)?;
    Some(now_unix() + backoff.as_secs() as i64)
}

/// Periodically checks each queue's schedule window and runs it when active,
/// the Rust equivalent of `Scheduler`'s 60-second `System.Threading.Timer` tick,
/// and auto-retries any `Failed` entry whose backoff has elapsed. A queue with
/// no schedule is treated as always-active (matches `DownloadQueue` with
/// `Schedule == null` in the original: it runs whenever asked).
pub async fn run_forever(store: Arc<DownloadStore>, manager: Arc<DownloadManager>) -> ! {
    loop {
        tick(&store, &manager).await;
        tokio::time::sleep(CHECK_INTERVAL).await;
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

/// Re-queues every `Failed` entry whose `next_retry_at` has passed and hasn't
/// exhausted `MAX_AUTO_RETRIES` yet. `retry_entry` resets `retry_count` to 0
/// as part of the same `Queued` transition manual retries use, so this bumps
/// it back up right after to reflect the auto-attempt actually consumed -
/// the next failure (if any) reads that new count to schedule the next
/// backoff step, or stop entirely once `MAX_AUTO_RETRIES` is reached.
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
        // Growing delay: each successive backoff step schedules further out.
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
