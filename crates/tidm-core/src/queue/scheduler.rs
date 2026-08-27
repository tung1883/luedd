use std::sync::Arc;
use std::time::Duration;

use chrono::Timelike;

use super::manager::DownloadManager;
use super::store::DownloadStore;

const CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Periodically checks each queue's schedule window and runs it when active,
/// the Rust equivalent of `Scheduler`'s 60-second `System.Threading.Timer` tick.
/// A queue with no schedule is treated as always-active (matches `DownloadQueue`
/// with `Schedule == null` in the original: it runs whenever asked).
pub async fn run_forever(store: Arc<DownloadStore>, manager: Arc<DownloadManager>) -> ! {
    loop {
        tick(&store, &manager).await;
        tokio::time::sleep(CHECK_INTERVAL).await;
    }
}

async fn tick(store: &DownloadStore, manager: &DownloadManager) {
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

#[cfg(test)]
mod tests {
    use super::super::model::DownloadSchedule;

    #[test]
    fn schedule_gates_correctly_at_boundaries() {
        let business_hours = DownloadSchedule { start_minutes: 9 * 60, end_minutes: 18 * 60 };
        assert!(!business_hours.is_active_at(0));
        assert!(business_hours.is_active_at(9 * 60));
        assert!(!business_hours.is_active_at(18 * 60));
    }
}
