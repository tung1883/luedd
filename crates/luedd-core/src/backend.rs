//! Pluggable download backends.
//!
//! Every download runs through a [`DownloadBackend`]. The three built-ins
//! (`http`, `hls`, `dash`) wrap the existing [`crate::jobs`] engine unchanged;
//! new sites/protocols (yt-dlp, Instagram, BitTorrent, …) are added by
//! implementing the trait and calling [`BackendRegistry::register`].

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use luedd_media::quality::QualityOption;
use luedd_net::{HttpClient, ProgressTx, RequestContext};

use crate::jobs::DownloadKind;

pub mod builtin;
pub mod instagram;
pub mod instaloader;
pub mod ytdlp;

pub use builtin::{DashBackend, HlsBackend, HttpBackend};
pub use instagram::InstagramBackend;
pub use ytdlp::YtdlpBackend;

/// How strongly a backend claims a URL. Higher wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    No,
    Weak,
    Strong,
    Certain,
}

/// A content-type / extension sniff, offered to `can_handle` for a second
/// opinion when the URL alone is ambiguous.
#[derive(Debug, Clone, Default)]
pub struct Sniff {
    pub real_ext: Option<String>,
    pub content_type: Option<String>,
}

/// Everything a backend needs to run one download.
#[derive(Clone)]
pub struct DownloadReq {
    pub url: String,
    /// Directory the output goes in; the backend picks the final file name(s).
    pub dest_dir: PathBuf,
    pub filename_hint: Option<String>,
    pub ctx: RequestContext,
    /// Opaque, backend-defined variant key (from `probe_qualities`).
    pub quality: Option<String>,
    pub concurrency: usize,
    pub config: BackendConfig,
}

/// What a completed download produced. Plural for torrents / IG carousels.
#[derive(Debug, Clone, Default)]
pub struct Outcome {
    pub files: Vec<PathBuf>,
    /// Grouping metadata for the per-plugin views, discovered during the run
    /// (yt-dlp channel, Instagram account…). Written onto the entry on success.
    pub meta: EntryMeta,
}

impl Outcome {
    pub fn single(path: PathBuf) -> Self {
        Self { files: vec![path], meta: EntryMeta::default() }
    }
}

/// Optional grouping fields the plugin views key off. Every field independently
/// optional so a backend fills only what it knows.
#[derive(Debug, Clone, Default)]
pub struct EntryMeta {
    /// yt-dlp channel/uploader, or an Instagram `@account`.
    pub author: Option<String>,
    /// A human title (yt-dlp video title).
    pub title: Option<String>,
    /// Instagram sub-group: `post | reel | profile | story | highlight`.
    pub media_class: Option<String>,
    /// A dedicated folder this entry's output lives in (Instagram writes a whole
    /// carousel / profile / story here). Set at `describe` time so deleting the
    /// entry can remove the folder wholesale - partial downloads included.
    pub out_dir: Option<PathBuf>,
}

impl EntryMeta {
    pub fn is_empty(&self) -> bool {
        self.author.is_none()
            && self.title.is_none()
            && self.media_class.is_none()
            && self.out_dir.is_none()
    }
}

#[async_trait::async_trait]
pub trait DownloadBackend: Send + Sync {
    fn id(&self) -> &'static str;

    fn can_handle(&self, url: &str, sniff: Option<&Sniff>) -> Confidence;

    /// Torrent backends pause/resume through a session handle rather than by
    /// aborting the run task.
    fn is_torrent(&self) -> bool {
        false
    }

    /// Hosts whose *page* URLs this backend downloads (yt-dlp watch pages,
    /// Instagram posts…). The browser extension offers a page on one of these
    /// hosts as a detection directly, instead of trying to catch its media
    /// requests.
    fn page_hosts(&self) -> &'static [&'static str] {
        &[]
    }

    async fn probe_qualities(&self, _req: &DownloadReq) -> Result<Vec<QualityOption>> {
        Ok(Vec::new())
    }

    /// A thumbnail image for a page detection (no media file to decode a frame
    /// from). Returns `(image_url, square)` - `square` is `true` for an
    /// intrinsically 1:1 source (an Instagram profile picture / highlight
    /// cover) so the panel can size its slot to match instead of letterboxing.
    async fn thumbnail(&self, _req: &DownloadReq) -> Result<Option<(String, bool)>> {
        Ok(None)
    }

    /// Cheap, best-effort grouping metadata known *before* the download runs
    /// (e.g. from the URL alone, or a cache). Used to place queued/running
    /// entries in the per-plugin views; `run`'s `Outcome.meta` refines it after.
    async fn describe(&self, _req: &DownloadReq) -> EntryMeta {
        EntryMeta::default()
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome>;
}

// ---------------------------------------------------------------------------
// backend_id <-> DownloadKind (for the three built-ins + downgrade compat)
// ---------------------------------------------------------------------------

pub fn backend_id_for_kind(kind: DownloadKind) -> &'static str {
    match kind {
        DownloadKind::Http => "http",
        DownloadKind::Hls => "hls",
        DownloadKind::Dash => "dash",
    }
}

pub fn kind_for_backend_id(id: &str) -> DownloadKind {
    match id {
        "hls" => DownloadKind::Hls,
        "dash" => DownloadKind::Dash,
        _ => DownloadKind::Http,
    }
}

/// Human-facing provider name. The three transport built-ins are all just
/// "Lüdd" (the engine); plugins show a friendly name.
pub fn provider_label(backend_id: &str) -> &str {
    match backend_id {
        "http" | "hls" | "dash" => "Lüdd",
        "ytdlp" => "yt-dlp",
        "instagram" => "Lüdd-Insta",
        "torrent" => "torrent",
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct BackendRegistry {
    backends: Vec<Arc<dyn DownloadBackend>>,
    client: HttpClient,
}

impl BackendRegistry {
    /// A registry with only the three transport built-ins.
    pub fn with_builtins(client: HttpClient) -> Self {
        Self {
            backends: vec![
                Arc::new(HttpBackend::new(client.clone())),
                Arc::new(HlsBackend::new(client.clone())),
                Arc::new(DashBackend::new(client.clone())),
            ],
            client,
        }
    }

    pub fn register(&mut self, backend: Arc<dyn DownloadBackend>) {
        self.backends.push(backend);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn DownloadBackend>> {
        self.backends.iter().find(|b| b.id() == id).cloned()
    }

    /// The `http` backend, which is always registered and is the fallback.
    pub fn http(&self) -> Arc<dyn DownloadBackend> {
        self.get("http").expect("http backend is always registered")
    }

    /// Cheap, synchronous backend guess for display/grouping (no network sniff):
    /// host-routing override, then the best `can_handle(url, None)`, then `http`.
    pub fn quick_id(&self, url: &str, cfg: &BackendConfig) -> &'static str {
        if let Some(host) = host_of(url) {
            if let Some(route) = cfg.host_routing.iter().find(|r| host_matches(&host, &r.host_suffix)) {
                if let Some(b) = self.get(&route.backend_id) {
                    return b.id();
                }
            }
        }
        let mut best: Option<(Confidence, &'static str)> = None;
        for b in &self.backends {
            let c = b.can_handle(url, None);
            if best.as_ref().map_or(true, |(bc, _)| c > *bc) {
                best = Some((c, b.id()));
            }
        }
        best.map(|(_, id)| id).unwrap_or("http")
    }

    /// Human-facing provider labels for every registered backend, deduped,
    /// registration order (so "Lüdd" - the built-ins - comes first). Sent to
    /// the detection panel so it can list a provider even when it has 0 links.
    pub fn provider_labels(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for b in &self.backends {
            let label = provider_label(b.id()).to_string();
            if !out.contains(&label) {
                out.push(label);
            }
        }
        out
    }

    /// Union of every backend's [`DownloadBackend::page_hosts`], deduped. Sent
    /// to the extension so it knows which page URLs to offer as detections.
    pub fn page_hosts(&self) -> Vec<String> {
        let mut hosts: Vec<String> =
            self.backends.iter().flat_map(|b| b.page_hosts().iter().map(|s| s.to_string())).collect();
        hosts.sort();
        hosts.dedup();
        hosts
    }

    /// Resolve a URL to the backend that should handle it.
    ///
    /// 1. explicit `host_routing` override - exact host-suffix match wins.
    /// 2. `can_handle(url, None)` across all backends - any `Certain` wins.
    /// 3. if still below `Strong` and the URL looks like plain HTTP, sniff the
    ///    real extension once and re-ask with that.
    /// 4. fall back to `http`.
    pub async fn resolve(
        &self,
        url: &str,
        ctx: &RequestContext,
        cfg: &BackendConfig,
    ) -> Arc<dyn DownloadBackend> {
        if let Some(host) = host_of(url) {
            if let Some(route) = cfg.host_routing.iter().find(|r| host_matches(&host, &r.host_suffix)) {
                if let Some(backend) = self.get(&route.backend_id) {
                    return backend;
                }
            }
        }

        let mut best: Option<(Confidence, Arc<dyn DownloadBackend>)> = None;
        for backend in &self.backends {
            let c = backend.can_handle(url, None);
            if c == Confidence::Certain {
                return backend.clone();
            }
            if best.as_ref().map_or(true, |(bc, _)| c > *bc) {
                best = Some((c, backend.clone()));
            }
        }

        if best.as_ref().map_or(true, |(c, _)| *c < Confidence::Strong) {
            let sniff = Sniff {
                real_ext: crate::naming::resolve_real_extension(&self.client, url, ctx).await,
                content_type: None,
            };
            for backend in &self.backends {
                let c = backend.can_handle(url, Some(&sniff));
                if best.as_ref().map_or(true, |(bc, _)| c > *bc) {
                    best = Some((c, backend.clone()));
                }
            }
        }

        best.map(|(_, b)| b).unwrap_or_else(|| self.http())
    }
}

fn host_of(url: &str) -> Option<String> {
    let after = url.split_once("://")?.1;
    let authority = after.split(['/', '?', '#']).next()?;
    let host = authority.rsplit('@').next()?;
    Some(host.split(':').next()?.to_ascii_lowercase())
}

fn host_matches(host: &str, suffix: &str) -> bool {
    let s = suffix.trim().trim_start_matches('.').to_ascii_lowercase();
    !s.is_empty() && (host == s || host.ends_with(&format!(".{s}")))
}

// ---------------------------------------------------------------------------
// Config (embedded in Settings.backends)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct BackendConfig {
    /// Explicit yt-dlp binary; `None` => bundled, then PATH.
    pub ytdlp_path: Option<PathBuf>,
    pub instaloader_path: Option<PathBuf>,
    pub python_path: Option<PathBuf>,
    /// Lüdd-Insta primary download engine: `"custom"` (default, unset) | `"instaloader"`.
    pub instagram_engine_main: Option<String>,
    /// Lüdd-Insta fallback engine, tried when the primary fails at runtime:
    /// `"none"` (default, unset) | `"custom"` | `"instaloader"`.
    pub instagram_engine_fallback: Option<String>,
    /// Browser to pull cookies from for the extraction leg ("chrome", "firefox", …).
    pub cookies_from_browser: Option<String>,
    /// Manual host -> backend overrides.
    pub host_routing: Vec<HostRoute>,
    pub instagram: InstagramConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HostRoute {
    pub host_suffix: String,
    pub backend_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct InstagramConfig {
    /// Fallback session cookie for Add-box URLs. Extension-detected Instagram
    /// pages already carry the browser's cookies, so this is only needed when a
    /// URL is pasted by hand.
    pub session_cookie: Option<String>,
    /// Persisted-query ids. Seeded with the values from `../insta-graphql.md`
    /// so it works out of the box; Instagram rotates these every few weeks, at
    /// which point the user refreshes them in Settings.
    pub doc_id_shortcode: Option<String>,
    pub doc_id_timeline: Option<String>,
    pub query_hash_reels: Option<String>,
    pub app_id: String,
}

impl Default for InstagramConfig {
    fn default() -> Self {
        Self {
            session_cookie: None,
            // insta-graphql.md §4.3 / §4.2 - observed 2025-2026, will drift.
            doc_id_shortcode: Some("24368985919464652".to_string()),
            doc_id_timeline: Some("8759034877476257".to_string()),
            // Stories now use the REST reels_media endpoint, not this hash.
            query_hash_reels: None,
            app_id: "936619743392459".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_strips_scheme_port_and_path() {
        assert_eq!(host_of("https://www.youtube.com:443/watch?v=x").as_deref(), Some("www.youtube.com"));
        assert_eq!(host_of("http://Example.COM/a/b").as_deref(), Some("example.com"));
    }

    #[test]
    fn host_matches_is_suffix_aware() {
        assert!(host_matches("www.youtube.com", "youtube.com"));
        assert!(host_matches("youtube.com", "youtube.com"));
        assert!(host_matches("cdn.instagram.com", ".instagram.com"));
        assert!(!host_matches("notyoutube.com", "youtube.com"));
        assert!(!host_matches("youtube.com.evil.com", "youtube.com"));
    }

    #[tokio::test]
    async fn resolve_picks_hls_for_m3u8_and_falls_back_to_http() {
        let reg = BackendRegistry::with_builtins(HttpClient::new().unwrap());
        let cfg = BackendConfig::default();
        let ctx = RequestContext::default();
        assert_eq!(reg.resolve("https://x/y.m3u8", &ctx, &cfg).await.id(), "hls");
        assert_eq!(reg.resolve("https://x/y.mpd", &ctx, &cfg).await.id(), "dash");
        assert_eq!(reg.resolve("https://x/y.bin?foo=1", &ctx, &cfg).await.id(), "http");
    }

    #[tokio::test]
    async fn host_routing_override_wins() {
        let reg = BackendRegistry::with_builtins(HttpClient::new().unwrap());
        let cfg = BackendConfig {
            host_routing: vec![HostRoute { host_suffix: "example.com".into(), backend_id: "dash".into() }],
            ..Default::default()
        };
        assert_eq!(reg.resolve("https://foo.example.com/x.m3u8", &RequestContext::default(), &cfg).await.id(), "dash");
    }
}
