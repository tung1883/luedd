//! Persistent list of the JSON documents Lüdd-JSON has seen: ones the browser
//! extension caught on a page and ones the user scanned by hand. The Lüdd-JSON
//! viewer shows them as cards so a source can be re-opened later. Same small
//! whole-file JSON store pattern as [`crate::yt_library`].

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Oldest sources fall off past this.
const MAX_SOURCES: usize = 500;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonLibrary {
    #[serde(default)]
    pub sources: Vec<JsonSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSource {
    /// The JSON's URL (the identity), or `file:<name>` for a local file.
    pub url: String,
    /// Display name: the file name without `.json`, else the host.
    pub name: String,
    pub host: String,
    pub first_seen: i64,
    pub last_seen: i64,
    /// Asset count from the most recent scan; `None` before the first one.
    #[serde(default)]
    pub assets: Option<u32>,
    #[serde(default)]
    pub last_scan: Option<i64>,
    /// Entries queued from this source so far.
    #[serde(default)]
    pub queued: u32,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `https://cdn.x.com/api/gallery.json?a=1` -> ("gallery", "cdn.x.com").
pub fn name_and_host(url: &str) -> (String, String) {
    if let Some(name) = url.strip_prefix("file:") {
        return (name.trim_end_matches(".json").to_string(), "local file".to_string());
    }
    let rest = url.split_once("://").map(|x| x.1).unwrap_or(url);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or("").split(':').next().unwrap_or("").to_ascii_lowercase();
    let path = rest[authority.len()..].split(['?', '#']).next().unwrap_or("");
    let last = path.split('/').filter(|s| !s.is_empty()).next_back().unwrap_or("");
    let name = last.strip_suffix(".json").unwrap_or(last);
    let name = if name.is_empty() { host.clone() } else { name.to_string() };
    (name, host)
}

pub struct JsonLibraryStore {
    path: PathBuf,
    data: RwLock<JsonLibrary>,
}

impl JsonLibraryStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(e) => {
                    let bak = path.with_extension("json.corrupt");
                    let _ = tokio::fs::rename(&path, &bak).await;
                    tracing::error!(error = %e, backup = %bak.display(), "corrupt json_library.json; starting fresh");
                    JsonLibrary::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => JsonLibrary::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data) })
    }

    /// Newest first.
    pub async fn snapshot(&self) -> JsonLibrary {
        let mut lib = self.data.read().await.clone();
        lib.sources.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
        lib
    }

    pub async fn len(&self) -> usize {
        self.data.read().await.sources.len()
    }

    /// Insert a source or touch an existing one.
    pub async fn record(&self, url: &str) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let t = now();
            if let Some(s) = lib.sources.iter_mut().find(|s| s.url == url) {
                s.last_seen = t;
            } else {
                let (name, host) = name_and_host(url);
                lib.sources.push(JsonSource {
                    url: url.to_string(),
                    name,
                    host,
                    first_seen: t,
                    last_seen: t,
                    assets: None,
                    last_scan: None,
                    queued: 0,
                });
                if lib.sources.len() > MAX_SOURCES {
                    lib.sources.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
                    lib.sources.truncate(MAX_SOURCES);
                }
            }
        }
        self.save().await
    }

    /// Store the asset count of a finished scan (records the source if new).
    pub async fn scanned(&self, url: &str, assets: u32) -> Result<()> {
        self.record(url).await?;
        {
            let mut lib = self.data.write().await;
            if let Some(s) = lib.sources.iter_mut().find(|s| s.url == url) {
                s.assets = Some(assets);
                s.last_scan = Some(now());
            }
        }
        self.save().await
    }

    pub async fn add_queued(&self, url: &str, n: u32) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            if let Some(s) = lib.sources.iter_mut().find(|s| s.url == url) {
                s.queued = s.queued.saturating_add(n);
            }
        }
        self.save().await
    }

    pub async fn forget(&self, url: &str) -> Result<bool> {
        let removed = {
            let mut lib = self.data.write().await;
            let before = lib.sources.len();
            lib.sources.retain(|s| s.url != url);
            lib.sources.len() != before
        };
        if removed {
            self.save().await?;
        }
        Ok(removed)
    }

    async fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&*self.data.read().await)?;
        crate::atomicfile::write_atomic(&self.path, &json).await
    }
}

pub fn default_json_library_path(data_dir: &Path) -> PathBuf {
    data_dir.join("json_library.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_a_source_from_its_url() {
        assert_eq!(name_and_host("https://cdn.x.com/api/gallery.json?a=1"), ("gallery".into(), "cdn.x.com".into()));
        assert_eq!(name_and_host("https://x.com/"), ("x.com".into(), "x.com".into()));
        assert_eq!(name_and_host("file:data.json"), ("data".into(), "local file".into()));
    }

    #[tokio::test]
    async fn records_dedupes_scans_and_forgets() {
        let dir = std::env::temp_dir().join(format!("luedd-jsonlib-{}", uuid::Uuid::new_v4()));
        let path = default_json_library_path(&dir);
        let store = JsonLibraryStore::open(&path).await.unwrap();
        store.record("https://x.com/a.json").await.unwrap();
        store.record("https://x.com/a.json").await.unwrap();
        store.scanned("https://x.com/b.json", 12).await.unwrap();
        store.add_queued("https://x.com/b.json", 5).await.unwrap();
        let again = JsonLibraryStore::open(&path).await.unwrap();
        let snap = again.snapshot().await;
        assert_eq!(snap.sources.len(), 2);
        let b = snap.sources.iter().find(|s| s.url.ends_with("b.json")).unwrap();
        assert_eq!((b.assets, b.queued), (Some(12), 5));
        assert!(again.forget("https://x.com/a.json").await.unwrap());
        assert_eq!(again.len().await, 1);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
