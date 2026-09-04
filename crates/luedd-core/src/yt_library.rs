//! Persistent index of the channels / uploaders yt-dlp has caught.
//!
//! Browser detections (`DetectedMedia` in luedd-ipc) are in-memory only and die
//! with the process. The yt-dlp viewer needs a list that grows over time, so
//! every yt-dlp page detection is also recorded here — a small whole-file JSON
//! store next to `settings.json` / `ig_library.json`, the same pattern as
//! [`crate::ig_library`].
//!
//! A caught video's owning channel is not in the watch-page URL, so it lands in
//! the [`UNRESOLVED`] bucket keyed by url until a background `yt-dlp -J` fills in
//! the channel + metadata and [`YtLibraryStore::resolve`] moves it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Bucket for a caught video whose channel isn't known yet.
pub const UNRESOLVED: &str = "__unresolved__";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YtLibrary {
    /// key = `channel_id` / a url-derived slug, or [`UNRESOLVED`].
    #[serde(default)]
    pub channels: BTreeMap<String, YtChannel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YtChannel {
    /// `channel_id` or a slug; matches the map key.
    pub key: String,
    /// Display name — empty until a `-J` resolves it.
    #[serde(default)]
    pub name: String,
    /// Host the videos came from: `youtube.com` | `twitch.tv` | …
    #[serde(default)]
    pub site: String,
    #[serde(default)]
    pub channel_url: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(default)]
    pub avatar_at: i64,
    pub first_seen: i64,
    pub last_seen: i64,
    #[serde(default)]
    pub caught: Vec<YtCaught>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YtCaught {
    /// yt-dlp `id` (video id) or, before resolution, a slug of the url.
    pub id: String,
    /// Canonical watch-page URL — what `/yt/queue` re-downloads.
    pub url: String,
    /// Page title on catch, upgraded from `-J`.
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub thumbnail: Option<String>,
    /// Seconds.
    #[serde(default)]
    pub duration: Option<u64>,
    #[serde(default)]
    pub uploader: Option<String>,
    /// `YYYYMMDD`.
    #[serde(default)]
    pub upload_date: Option<String>,
    #[serde(default)]
    pub view_count: Option<u64>,
    /// Has `-J` metadata been merged in?
    #[serde(default)]
    pub resolved: bool,
    pub seen: i64,
}

/// The channel + per-video fields a `-J` resolve produces.
#[derive(Debug, Clone, Default)]
pub struct YtResolve {
    pub channel_key: String,
    pub channel_name: String,
    pub channel_url: Option<String>,
    pub avatar_url: Option<String>,
    pub video_id: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub duration: Option<u64>,
    pub uploader: Option<String>,
    pub upload_date: Option<String>,
    pub view_count: Option<u64>,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub struct YtLibraryStore {
    path: PathBuf,
    data: RwLock<YtLibrary>,
}

impl YtLibraryStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                // A torn file (killed mid-write on an older build) must not brick
                // startup — set it aside and start fresh. The library rebuilds
                // itself from browser detections.
                Err(e) => {
                    let bak = path.with_extension("json.corrupt");
                    let _ = tokio::fs::rename(&path, &bak).await;
                    tracing::error!(error = %e, backup = %bak.display(), "corrupt yt_library.json; starting fresh");
                    YtLibrary::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => YtLibrary::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data) })
    }

    pub async fn snapshot(&self) -> YtLibrary {
        self.data.read().await.clone()
    }

    /// Record a caught video under `channel_key` (or [`UNRESOLVED`]). Deduped on
    /// `url`.
    pub async fn record(&self, channel_key: &str, site: &str, c: YtCaught) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            // Already known (resolved into a real channel, or pending)? Just touch
            // it — never re-file it under UNRESOLVED, or an open browser tab would
            // resurrect it every catch and the resolver would spin on it forever.
            if let Some(ch) = lib
                .channels
                .values_mut()
                .find(|ch| ch.caught.iter().any(|x| x.url == c.url))
            {
                ch.last_seen = now();
            } else {
                let ch =
                    lib.channels.entry(channel_key.to_string()).or_insert_with(|| YtChannel {
                        key: channel_key.to_string(),
                        name: String::new(),
                        site: site.to_string(),
                        channel_url: None,
                        avatar_url: None,
                        avatar_at: 0,
                        first_seen: now(),
                        last_seen: now(),
                        caught: Vec::new(),
                    });
                ch.last_seen = now();
                if ch.site.is_empty() {
                    ch.site = site.to_string();
                }
                ch.caught.push(c);
            }
        }
        self.save().await
    }

    /// A `-J` came back: move the caught video at `r.video`/`url` out of
    /// [`UNRESOLVED`] into its real channel and merge the metadata.
    pub async fn resolve(&self, url: &str, r: &YtResolve) -> Result<()> {
        {
            let mut lib = self.data.write().await;

            // pull the caught item from wherever it currently lives
            let mut moving: Option<YtCaught> = None;
            for ch in lib.channels.values_mut() {
                if let Some(pos) = ch.caught.iter().position(|c| c.url == url) {
                    moving = Some(ch.caught.remove(pos));
                    break;
                }
            }
            lib.channels.retain(|k, ch| k != UNRESOLVED || !ch.caught.is_empty());

            let Some(mut item) = moving else { return Ok(()) };
            item.id = if r.video_id.is_empty() { item.id } else { r.video_id.clone() };
            if !r.title.is_empty() {
                item.title = r.title.clone();
            }
            if r.thumbnail.is_some() {
                item.thumbnail = r.thumbnail.clone();
            }
            item.duration = r.duration.or(item.duration);
            item.uploader = r.uploader.clone().or(item.uploader);
            item.upload_date = r.upload_date.clone().or(item.upload_date);
            item.view_count = r.view_count.or(item.view_count);
            item.resolved = true;

            let key = if r.channel_key.is_empty() { UNRESOLVED.to_string() } else { r.channel_key.clone() };
            let site = channel_site_hint(&r.channel_url).unwrap_or_default();
            let ch = lib.channels.entry(key.clone()).or_insert_with(|| YtChannel {
                key,
                name: String::new(),
                site,
                channel_url: None,
                avatar_url: None,
                avatar_at: 0,
                first_seen: now(),
                last_seen: now(),
                caught: Vec::new(),
            });
            ch.last_seen = now();
            if !r.channel_name.is_empty() {
                ch.name = r.channel_name.clone();
            }
            if r.channel_url.is_some() {
                ch.channel_url = r.channel_url.clone();
            }
            if let Some(a) = &r.avatar_url {
                ch.avatar_url = Some(a.clone());
                ch.avatar_at = now();
            }
            // Dedupe on canonical URL *and* on the resolved video id — the same
            // video caught via two URL variants lands here twice otherwise.
            let dup = ch.caught.iter().any(|x| {
                x.url == item.url || (!item.id.is_empty() && x.id == item.id)
            });
            if !dup {
                ch.caught.push(item);
            }
        }
        self.save().await
    }

    /// Drop a caught video entirely (e.g. its `-J` failed — private / deleted /
    /// not actually a video page). Removes any now-empty channel.
    pub async fn forget(&self, url: &str) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            for ch in lib.channels.values_mut() {
                ch.caught.retain(|c| c.url != url);
            }
            lib.channels.retain(|_, ch| !ch.caught.is_empty());
        }
        self.save().await
    }

    /// Cache a channel's avatar (no-op for an unknown channel / unchanged URL).
    pub async fn set_channel_avatar(&self, key: &str, url: &str) -> Result<()> {
        {
            let mut lib = self.data.write().await;
            let Some(ch) = lib.channels.get_mut(key) else { return Ok(()) };
            if ch.avatar_url.as_deref() == Some(url) {
                ch.avatar_at = now();
                return Ok(());
            }
            ch.avatar_url = Some(url.to_string());
            ch.avatar_at = now();
        }
        self.save().await
    }

    async fn save(&self) -> Result<()> {
        let json = serde_json::to_vec_pretty(&*self.data.read().await)?;
        crate::atomicfile::write_atomic(&self.path, &json).await
    }
}

/// Cheap host from a channel URL, e.g. `https://www.youtube.com/@x` -> `youtube.com`.
fn channel_site_hint(url: &Option<String>) -> Option<String> {
    let u = url.as_deref()?;
    let host = u.split("://").nth(1)?.split(['/', '?']).next()?;
    Some(host.strip_prefix("www.").unwrap_or(host).to_ascii_lowercase())
}

pub fn default_yt_library_path(data_dir: &Path) -> PathBuf {
    data_dir.join("yt_library.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caught(id: &str, url: &str) -> YtCaught {
        YtCaught {
            id: id.into(),
            url: url.into(),
            title: "t".into(),
            thumbnail: None,
            duration: None,
            uploader: None,
            upload_date: None,
            view_count: None,
            resolved: false,
            seen: 1,
        }
    }

    #[tokio::test]
    async fn record_dedupes_and_persists() {
        let dir = std::env::temp_dir().join(format!("luedd-ytlib-{}", uuid::Uuid::new_v4()));
        let path = default_yt_library_path(&dir);

        let store = YtLibraryStore::open(&path).await.unwrap();
        let url = "https://www.youtube.com/watch?v=abc";
        store.record(UNRESOLVED, "youtube.com", caught("abc", url)).await.unwrap();
        store.record(UNRESOLVED, "youtube.com", caught("abc", url)).await.unwrap(); // dupe
        store
            .record(UNRESOLVED, "youtube.com", caught("xyz", "https://www.youtube.com/watch?v=xyz"))
            .await
            .unwrap();

        let store = YtLibraryStore::open(&path).await.unwrap();
        let lib = store.snapshot().await;
        assert_eq!(lib.channels[UNRESOLVED].caught.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn record_does_not_resurrect_a_resolved_video() {
        let dir = std::env::temp_dir().join(format!("luedd-ytlib-{}", uuid::Uuid::new_v4()));
        let path = default_yt_library_path(&dir);
        let store = YtLibraryStore::open(&path).await.unwrap();

        let url = "https://www.youtube.com/watch?v=abc";
        store.record(UNRESOLVED, "youtube.com", caught("abc", url)).await.unwrap();
        store
            .resolve(
                url,
                &YtResolve {
                    channel_key: "UC1".into(),
                    channel_name: "Chan".into(),
                    video_id: "abc".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // an open browser tab keeps catching the same watch page
        store.record(UNRESOLVED, "youtube.com", caught("abc", url)).await.unwrap();

        let lib = store.snapshot().await;
        assert!(!lib.channels.contains_key(UNRESOLVED), "must not re-file a known video");
        assert_eq!(lib.channels["UC1"].caught.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resolve_moves_unresolved() {
        let dir = std::env::temp_dir().join(format!("luedd-ytlib-{}", uuid::Uuid::new_v4()));
        let path = default_yt_library_path(&dir);
        let store = YtLibraryStore::open(&path).await.unwrap();

        let url = "https://www.youtube.com/watch?v=abc";
        store.record(UNRESOLVED, "youtube.com", caught("abc", url)).await.unwrap();
        assert!(store.snapshot().await.channels.contains_key(UNRESOLVED));

        store
            .resolve(
                url,
                &YtResolve {
                    channel_key: "UC123".into(),
                    channel_name: "Kurzgesagt".into(),
                    channel_url: Some("https://www.youtube.com/@kurzgesagt".into()),
                    video_id: "abc".into(),
                    title: "Big Star".into(),
                    duration: Some(641),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let lib = store.snapshot().await;
        assert!(!lib.channels.contains_key(UNRESOLVED));
        assert_eq!(lib.channels["UC123"].name, "Kurzgesagt");
        assert_eq!(lib.channels["UC123"].caught.len(), 1);
        assert!(lib.channels["UC123"].caught[0].resolved);
        assert_eq!(lib.channels["UC123"].caught[0].duration, Some(641));

        std::fs::remove_dir_all(&dir).ok();
    }
}
