use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::jobs::DownloadKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadStatus {
    Queued,
    /// Renamed from `Running` for clarity now that there's a second, distinct
    /// in-progress phase (`Converting`) - the alias keeps any already-
    /// persisted `downloads.json` entry stuck in the old state deserializing.
    #[serde(alias = "Running")]
    Downloading,
    /// Segments/pieces are all fetched; ffmpeg is muxing them into the final
    /// output. A separate phase from `Downloading` because it's CPU-bound
    /// rather than network-bound and reports no further byte progress -
    /// `jobs::run_hls`/`run_dash` signal entry into this phase via
    /// `tidm_net::report_converting` right before invoking ffmpeg.
    Converting,
    /// User-paused mid-transfer (`DownloadManager::pause_entry`) - distinct
    /// from `Queued` (never started) so the GUI can tell the two apart, even
    /// though resuming a paused entry goes through the same `Queued` path.
    Paused,
    Finished,
    Failed,
    Cancelled,
}

/// One download record, the Rust equivalent of XDM's `DownloadEntries`/
/// `DataAccess.DownloadList` rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEntry {
    pub id: String,
    pub url: String,
    pub dest: PathBuf,
    pub kind: DownloadKind,
    pub status: DownloadStatus,
    pub error: Option<String>,
    /// Unix timestamp (seconds) the entry was added.
    pub created_at: i64,
    /// Request headers (Referer/Origin/User-Agent/etc.) captured at detection
    /// time, to be replayed on the actual download. Without these, a URL that
    /// only works in the context of the page it was found on (hotlink
    /// protection, session-bound tokens) fails when fetched standalone even
    /// though the URL itself is otherwise fine. `#[serde(default)]` so older
    /// persisted `downloads.json` entries without this field still deserialize.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub cookie: Option<String>,
    /// (done, total) - segments for Hls/Dash, bytes for Http. `None` until the
    /// first progress update arrives (or if the job finishes too fast to ever
    /// report one).
    #[serde(default)]
    pub progress: Option<(u64, u64)>,
}

impl DownloadEntry {
    pub fn new(url: String, dest: PathBuf, kind: DownloadKind) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            url,
            dest,
            kind,
            status: DownloadStatus::Queued,
            error: None,
            created_at: now_unix(),
            headers: HashMap::new(),
            cookie: None,
            progress: None,
        }
    }

    /// Attaches request context (headers/cookie) captured when the URL was
    /// detected, so the actual download replays them instead of fetching bare.
    pub fn with_request_context(mut self, headers: HashMap<String, String>, cookie: Option<String>) -> Self {
        self.headers = headers;
        self.cookie = cookie;
        self
    }
}

/// A time-of-day window (minutes since midnight, local time), the Rust
/// equivalent of `DownloadSchedule`'s start/end fields. `end < start` means the
/// window wraps past midnight (e.g. 23:00-06:00).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DownloadSchedule {
    pub start_minutes: u32,
    pub end_minutes: u32,
}

impl DownloadSchedule {
    /// Whether `minutes_since_midnight` (0..1440) falls inside this window.
    pub fn is_active_at(&self, minutes_since_midnight: u32) -> bool {
        let m = minutes_since_midnight % 1440;
        if self.start_minutes <= self.end_minutes {
            m >= self.start_minutes && m < self.end_minutes
        } else {
            m >= self.start_minutes || m < self.end_minutes
        }
    }
}

/// An ordered group of downloads with an optional schedule window, the Rust
/// equivalent of `DownloadQueue`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadQueueDef {
    pub id: String,
    pub name: String,
    pub entry_ids: Vec<String>,
    pub schedule: Option<DownloadSchedule>,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_window_same_day() {
        let s = DownloadSchedule { start_minutes: 8 * 60, end_minutes: 17 * 60 };
        assert!(!s.is_active_at(7 * 60 + 59));
        assert!(s.is_active_at(8 * 60));
        assert!(s.is_active_at(16 * 60 + 59));
        assert!(!s.is_active_at(17 * 60));
    }

    #[test]
    fn schedule_window_wraps_midnight() {
        let s = DownloadSchedule { start_minutes: 23 * 60, end_minutes: 6 * 60 };
        assert!(s.is_active_at(23 * 60 + 30));
        assert!(s.is_active_at(0));
        assert!(s.is_active_at(5 * 60 + 59));
        assert!(!s.is_active_at(6 * 60));
        assert!(!s.is_active_at(12 * 60));
    }

    #[test]
    fn generated_ids_are_unique_and_well_formed() {
        let a = DownloadEntry::new("u".into(), "d".into(), DownloadKind::Http);
        let b = DownloadEntry::new("u".into(), "d".into(), DownloadKind::Http);
        assert_ne!(a.id, b.id);
        assert_eq!(a.id.len(), 36);
    }
}
