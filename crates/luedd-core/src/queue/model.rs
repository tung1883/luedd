use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::jobs::DownloadKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadStatus {
    Queued,
    #[serde(alias = "Running")]
    Downloading,
    Converting,
    Paused,
    Finished,
    Failed,
    Cancelled,
}

/// Live progress snapshot for a running download, written by the manager's
/// progress task and read by the UI.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DownloadProgress {
    /// Bytes transferred so far.
    pub downloaded_bytes: u64,
    /// Total bytes, when the size is known (HTTP with Content-Length).
    pub total_bytes: Option<u64>,
    /// Completed segments (HLS/DASH); 0 for byte-only downloads.
    pub done_units: u64,
    /// Total segments; 0 when there is no segment count.
    pub total_units: u64,
    /// Throughput over a recent sliding window, bytes per second.
    pub speed_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEntry {
    pub id: String,
    pub url: String,
    pub dest: PathBuf,
    pub kind: DownloadKind,
    pub status: DownloadStatus,
    pub error: Option<String>,
    pub created_at: i64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub cookie: Option<String>,
    #[serde(default)]
    pub progress: Option<DownloadProgress>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub next_retry_at: Option<i64>,
    #[serde(default)]
    pub quality: Option<String>,
    /// Inline thumbnail (data URL) carried over from the browser detection
    /// panel, so the list can show a preview before a single byte is on disk.
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub preview_kind: Option<String>,
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
            retry_count: 0,
            next_retry_at: None,
            quality: None,
            preview: None,
            preview_kind: None,
        }
    }

    pub fn with_preview(mut self, preview: Option<(String, String)>) -> Self {
        if let Some((data_url, kind)) = preview {
            self.preview = Some(data_url);
            self.preview_kind = Some(kind);
        }
        self
    }

    pub fn with_quality(mut self, quality: Option<String>) -> Self {
        self.quality = quality;
        self
    }

    pub fn with_request_context(mut self, headers: HashMap<String, String>, cookie: Option<String>) -> Self {
        self.headers = headers;
        self.cookie = cookie;
        self
    }

}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DownloadSchedule {
    pub start_minutes: u32,
    pub end_minutes: u32,
}

impl DownloadSchedule {
    pub fn is_active_at(&self, minutes_since_midnight: u32) -> bool {
        let m = minutes_since_midnight % 1440;
        if self.start_minutes <= self.end_minutes {
            m >= self.start_minutes && m < self.end_minutes
        } else {
            m >= self.start_minutes || m < self.end_minutes
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadQueueDef {
    pub id: String,
    pub name: String,
    pub entry_ids: Vec<String>,
    pub schedule: Option<DownloadSchedule>,
}

pub(crate) fn now_unix() -> i64 {
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
