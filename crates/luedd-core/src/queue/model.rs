use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::backend::{backend_id_for_kind, kind_for_backend_id};
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
#[serde(from = "DownloadEntryRepr")]
pub struct DownloadEntry {
    pub id: String,
    pub url: String,
    pub dest: PathBuf,
    /// Kept in sync with `backend_id` for the three built-ins; still written so
    /// an older build can still load the file. Nothing reads it after the
    /// backend registry landed — `backend_id` is authoritative.
    pub kind: DownloadKind,
    /// Which [`crate::backend::DownloadBackend`] runs this entry.
    pub backend_id: String,
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
    /// Backend-defined per-download flags (yt-dlp: `subs` / `thumbnail` /
    /// `chapters`). Empty for most downloads.
    #[serde(default)]
    pub extras: std::collections::BTreeMap<String, String>,
    /// Inline thumbnail (data URL) carried over from the browser detection
    /// panel, so the list can show a preview before a single byte is on disk.
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub preview_kind: Option<String>,
    /// Extra output files beyond `dest` (carousels, torrents).
    #[serde(default)]
    pub extra_files: Vec<PathBuf>,
    /// Grouping key for the per-plugin views: a yt-dlp channel/uploader, or an
    /// Instagram `@account`. `None` = ungrouped.
    #[serde(default)]
    pub author: Option<String>,
    /// Human title for the plugin views (yt-dlp video title). `None` falls back
    /// to the filename.
    #[serde(default)]
    pub title: Option<String>,
    /// Sub-group within an author for the Instagram view:
    /// `"post" | "reel" | "profile" | "story" | "highlight"`.
    #[serde(default)]
    pub media_class: Option<String>,
    /// A dedicated output folder (Instagram carousel / profile / story). When
    /// set, deleting the entry removes this whole folder, partial files and all.
    #[serde(default)]
    pub out_dir: Option<PathBuf>,
}

/// Deserialization shim. Files written before the backend registry have
/// `kind` but no `backend_id`; newer files have both. Either loads.
#[derive(Deserialize)]
struct DownloadEntryRepr {
    id: String,
    url: String,
    dest: PathBuf,
    #[serde(default)]
    kind: Option<DownloadKind>,
    #[serde(default)]
    backend_id: Option<String>,
    status: DownloadStatus,
    #[serde(default)]
    error: Option<String>,
    created_at: i64,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    cookie: Option<String>,
    #[serde(default)]
    progress: Option<DownloadProgress>,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    next_retry_at: Option<i64>,
    #[serde(default)]
    quality: Option<String>,
    #[serde(default)]
    extras: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    preview: Option<String>,
    #[serde(default)]
    preview_kind: Option<String>,
    #[serde(default)]
    extra_files: Vec<PathBuf>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    media_class: Option<String>,
    #[serde(default)]
    out_dir: Option<PathBuf>,
}

impl From<DownloadEntryRepr> for DownloadEntry {
    fn from(r: DownloadEntryRepr) -> Self {
        let backend_id = r
            .backend_id
            .filter(|s| !s.is_empty())
            .or_else(|| r.kind.map(|k| backend_id_for_kind(k).to_string()))
            .unwrap_or_else(|| "http".to_string());
        let kind = r.kind.unwrap_or_else(|| kind_for_backend_id(&backend_id));
        DownloadEntry {
            id: r.id,
            url: r.url,
            dest: r.dest,
            kind,
            backend_id,
            status: r.status,
            error: r.error,
            created_at: r.created_at,
            headers: r.headers,
            cookie: r.cookie,
            progress: r.progress,
            retry_count: r.retry_count,
            next_retry_at: r.next_retry_at,
            quality: r.quality,
            extras: r.extras,
            preview: r.preview,
            preview_kind: r.preview_kind,
            extra_files: r.extra_files,
            author: r.author,
            title: r.title,
            media_class: r.media_class,
            out_dir: r.out_dir,
        }
    }
}

impl DownloadEntry {
    pub fn new(url: String, dest: PathBuf, kind: DownloadKind) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            url,
            dest,
            kind,
            backend_id: backend_id_for_kind(kind).to_string(),
            status: DownloadStatus::Queued,
            error: None,
            created_at: now_unix(),
            headers: HashMap::new(),
            cookie: None,
            progress: None,
            retry_count: 0,
            next_retry_at: None,
            quality: None,
            extras: std::collections::BTreeMap::new(),
            preview: None,
            preview_kind: None,
            extra_files: Vec::new(),
            author: None,
            title: None,
            media_class: None,
            out_dir: None,
        }
    }

    /// Route this entry to a non-transport backend (yt-dlp, instagram, torrent…).
    /// Keeps `kind` best-effort in sync for downgrade compatibility.
    pub fn with_backend_id(mut self, id: impl Into<String>) -> Self {
        self.backend_id = id.into();
        self.kind = kind_for_backend_id(&self.backend_id);
        self
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

    pub fn with_extras(mut self, extras: std::collections::BTreeMap<String, String>) -> Self {
        self.extras = extras;
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

    #[test]
    fn new_entry_derives_backend_id_from_kind() {
        assert_eq!(DownloadEntry::new("u".into(), "d".into(), DownloadKind::Hls).backend_id, "hls");
        assert_eq!(DownloadEntry::new("u".into(), "d".into(), DownloadKind::Dash).backend_id, "dash");
        assert_eq!(DownloadEntry::new("u".into(), "d".into(), DownloadKind::Http).backend_id, "http");
    }

    #[test]
    fn loads_pre_registry_json_without_backend_id() {
        let old = r#"{
            "id": "abc", "url": "https://x/y.m3u8", "dest": "out.mp4",
            "kind": "Hls", "status": "Queued", "error": null, "created_at": 1
        }"#;
        let e: DownloadEntry = serde_json::from_str(old).unwrap();
        assert_eq!(e.backend_id, "hls");
        assert_eq!(e.kind, DownloadKind::Hls);
        assert!(e.extra_files.is_empty());

        // and the value it writes back round-trips
        let round: DownloadEntry = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(round.backend_id, "hls");
    }

    #[test]
    fn loads_new_json_with_backend_id_only() {
        let new = r#"{
            "id": "abc", "url": "magnet:?xt=x", "dest": "out",
            "backend_id": "torrent", "status": "Queued", "error": null, "created_at": 1
        }"#;
        let e: DownloadEntry = serde_json::from_str(new).unwrap();
        assert_eq!(e.backend_id, "torrent");
        assert_eq!(e.kind, DownloadKind::Http);
    }
}
