//! Persistent index of the Instagram accounts Lüdd has caught.
//!
//! Browser detections (`DetectedMedia` in luedd-ipc) are in-memory only and die
//! with the process. The profile viewer needs a list that grows over time, so
//! every Instagram page detection is also recorded here — a small whole-file
//! JSON store next to `settings.json` / `downloads.json`, same pattern as
//! [`crate::queue::SettingsStore`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Bucket key for a caught item whose owning account isn't known yet
/// (`/p/<code>`, `/stories/highlights/<id>` — no username in the URL). A
/// background resolver moves these to the real account.
pub const UNRESOLVED: &str = "__unresolved__";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IgLibrary {
    /// key = lowercase username, or [`UNRESOLVED`].
    #[serde(default)]
    pub accounts: BTreeMap<String, IgAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgAccount {
    pub username: String,
    pub first_seen: i64,
    pub last_seen: i64,
    #[serde(default)]
    pub caught: Vec<IgCaught>,
    /// Last resolved profile-picture URL — cached here so the accounts grid
    /// renders from disk instead of hitting IG's heavily-throttled
    /// `web_profile_info` once per card on every open.
    #[serde(default)]
    pub avatar_url: Option<String>,
    /// When `avatar_url` was last refreshed (unix seconds).
    #[serde(default)]
    pub avatar_at: i64,
    /// A running / paused / finished "download the whole profile" job. Kept
    /// here so a restart can offer to resume it.
    #[serde(default)]
    pub archive: Option<ArchiveJob>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveState {
    /// The walker is (or should be) adding more pages.
    Running,
    /// Stopped by the user or a restart; `cursor` says where to pick up.
    Paused,
    /// Walked to the end — every page has been queued at least once.
    Done,
}

/// Progress of a profile-archive walk. One per account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveJob {
    pub state: ArchiveState,
    /// `/ig/posts` pages walked so far.
    #[serde(default)]
    pub page: u32,
    /// Cursor to resume from (`None` = start at the newest page).
    #[serde(default)]
    pub cursor: Option<String>,
    /// `post_count` from the profile header when the job started — an estimate,
    /// the profile may change under us.
    #[serde(default)]
    pub total: u64,
    /// How many post URLs this job has handed to the queue.
    #[serde(default)]
    pub queued: u64,
    /// `all` | `recent` — what the last kick asked for.
    #[serde(default)]
    pub scope: String,
    /// Bumped every time a new kick (`all` / `recent` / `resume`) creates the
    /// job. A walker captures this at spawn and stops the moment it changes, so
    /// a stale walker can never write bookkeeping onto a freshly-started job.
    #[serde(default)]
    pub epoch: u64,
    pub updated: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgCaught {
    /// `post` | `reel` | `igtv` | `story` | `highlight` | `profile`.
    pub kind: String,
    /// shortcode (post/reel/igtv), highlight id, or `""` (story/profile).
    pub key: String,
    /// canonical page URL — what `/ig/queue` re-downloads.
    pub url: String,
    pub seen: i64,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub struct IgLibraryStore {
    path: PathBuf,
    data: RwLock<IgLibrary>,
}

impl IgLibraryStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(e) => {
                    let bak = path.with_extension("json.corrupt");
                    let _ = tokio::fs::rename(&path, &bak).await;
                    tracing::error!(error = %e, backup = %bak.display(), "corrupt ig_library.json; starting fresh");
                    IgLibrary::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => IgLibrary::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data) })
    }

    pub async fn snapshot(&self) -> IgLibrary {
        self.data.read().await.clone()
    }

    /// Record a caught item. `account` is `None` for account-less URLs (they go
    /// to the [`UNRESOLVED`] bucket keyed by url until [`set_account`] runs).
    pub async fn record(&self, account: Option<&str>, c: IgCaught) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let key = account.map(|a| a.to_ascii_lowercase()).unwrap_or_else(|| UNRESOLVED.to_string());
            let acct = lib.accounts.entry(key.clone()).or_insert_with(|| IgAccount {
                username: account.map(str::to_string).unwrap_or_default(),
                first_seen: now(),
                last_seen: now(),
                caught: Vec::new(),
                avatar_url: None,
                avatar_at: 0,
                archive: None,
            });
            acct.last_seen = now();
            if !acct.caught.iter().any(|x| x.kind == c.kind && x.key == c.key && x.url == c.url) {
                acct.caught.push(c);
            }
        }
        self.save().await
    }

    /// Move every unresolved item at `url` (and any exact-url dupes elsewhere)
    /// under the now-known `account`.
    pub async fn set_account(&self, url: &str, account: &str) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let moving: Vec<IgCaught> = lib
                .accounts
                .get_mut(UNRESOLVED)
                .map(|u| {
                    let (mine, rest): (Vec<_>, Vec<_>) = u.caught.drain(..).partition(|c| c.url == url);
                    u.caught = rest;
                    mine
                })
                .unwrap_or_default();
            if let Some(u) = lib.accounts.get(UNRESOLVED) {
                if u.caught.is_empty() {
                    lib.accounts.remove(UNRESOLVED);
                }
            }
            if moving.is_empty() {
                return Ok(());
            }
            let key = account.to_ascii_lowercase();
            let acct = lib.accounts.entry(key).or_insert_with(|| IgAccount {
                username: account.to_string(),
                first_seen: now(),
                last_seen: now(),
                caught: Vec::new(),
                avatar_url: None,
                avatar_at: 0,
                archive: None,
            });
            if acct.username.is_empty() {
                acct.username = account.to_string();
            }
            for c in moving {
                if !acct.caught.iter().any(|x| x.kind == c.kind && x.key == c.key && x.url == c.url) {
                    acct.caught.push(c);
                }
            }
        }
        self.save().await
    }

    /// Drop unresolved caught items that no longer look valid (e.g. an
    /// `instagram.com/reel/audio` music page that was mis-parsed as a reel).
    /// Returns how many were removed.
    pub async fn prune_unresolved(&self, keep: impl Fn(&IgCaught) -> bool) -> Result<usize> {
        let removed = {
            let mut lib = self.data.write().await;
            let Some(u) = lib.accounts.get_mut(UNRESOLVED) else { return Ok(0) };
            let before = u.caught.len();
            u.caught.retain(&keep);
            let removed = before - u.caught.len();
            if u.caught.is_empty() {
                lib.accounts.remove(UNRESOLVED);
            }
            removed
        };
        if removed > 0 {
            self.save().await?;
        }
        Ok(removed)
    }

    /// Cache the resolved profile picture for an account (no-op for an unknown
    /// account or an unchanged URL, so it rarely writes).
    pub async fn set_avatar(&self, account: &str, url: &str) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let Some(acct) = lib.accounts.get_mut(&account.to_ascii_lowercase()) else {
                return Ok(());
            };
            if acct.avatar_url.as_deref() == Some(url) {
                acct.avatar_at = now();
                return Ok(());
            }
            acct.avatar_url = Some(url.to_string());
            acct.avatar_at = now();
        }
        self.save().await
    }

    /// The archive job for an account, if any.
    pub async fn archive_of(&self, account: &str) -> Option<ArchiveJob> {
        self.data.read().await.accounts.get(&account.to_ascii_lowercase()).and_then(|a| a.archive.clone())
    }

    /// Store (or clear, with `None`) an account's archive job. Creates the
    /// account row if the profile was never caught before.
    pub async fn set_archive(&self, account: &str, job: Option<ArchiveJob>) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let key = account.to_ascii_lowercase();
            let acct = lib.accounts.entry(key).or_insert_with(|| IgAccount {
                username: account.to_string(),
                first_seen: now(),
                last_seen: now(),
                caught: Vec::new(),
                avatar_url: None,
                avatar_at: 0,
                archive: None,
            });
            if acct.username.is_empty() {
                acct.username = account.to_string();
            }
            acct.archive = job;
        }
        self.save().await
    }

    async fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&*self.data.read().await)?;
        crate::atomicfile::write_atomic(&self.path, &json).await
    }
}

pub fn default_ig_library_path(data_dir: &Path) -> PathBuf {
    data_dir.join("ig_library.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_dedupes_and_persists() {
        let dir = std::env::temp_dir().join(format!("luedd-iglib-{}", uuid::Uuid::new_v4()));
        let path = default_ig_library_path(&dir);

        let store = IgLibraryStore::open(&path).await.unwrap();
        let post = IgCaught {
            kind: "post".into(),
            key: "ABC".into(),
            url: "https://www.instagram.com/p/ABC".into(),
            seen: 1,
        };
        store.record(Some("quynhingx"), post.clone()).await.unwrap();
        store.record(Some("quynhingx"), post.clone()).await.unwrap(); // dupe
        store
            .record(
                Some("QuynhIngx"), // case-insensitive → same bucket
                IgCaught { kind: "story".into(), key: "".into(), url: "https://www.instagram.com/stories/quynhingx".into(), seen: 2 },
            )
            .await
            .unwrap();

        // reopen from disk
        let store = IgLibraryStore::open(&path).await.unwrap();
        let lib = store.snapshot().await;
        assert_eq!(lib.accounts.len(), 1);
        assert_eq!(lib.accounts["quynhingx"].caught.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn set_account_moves_unresolved() {
        let dir = std::env::temp_dir().join(format!("luedd-iglib-{}", uuid::Uuid::new_v4()));
        let path = default_ig_library_path(&dir);
        let store = IgLibraryStore::open(&path).await.unwrap();

        let url = "https://www.instagram.com/p/XYZ".to_string();
        store
            .record(None, IgCaught { kind: "post".into(), key: "XYZ".into(), url: url.clone(), seen: 1 })
            .await
            .unwrap();
        assert!(store.snapshot().await.accounts.contains_key(UNRESOLVED));

        store.set_account(&url, "someone").await.unwrap();
        let lib = store.snapshot().await;
        assert!(!lib.accounts.contains_key(UNRESOLVED));
        assert_eq!(lib.accounts["someone"].caught.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }
}
