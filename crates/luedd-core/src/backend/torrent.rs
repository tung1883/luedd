//! Embedded BitTorrent backend (librqbit).
//!
//! A single lazily-created [`librqbit::Session`] (its state JSON-persisted under
//! `<data_dir>/torrent/session`, so torrents resume on restart) backs every
//! magnet / `.torrent` download. Unlike the other backends, a torrent's transfer
//! lives inside that session's own tasks — aborting the [`run`] future stops our
//! progress polling but not the download — so the queue calls [`on_pause`] /
//! [`on_remove`] to reach through to the session.
//!
//! Per-file selection: [`preview`] lists the files (fetching metadata for a
//! magnet), the chosen indices ride along as the `torrent_files` extra, and
//! [`set_files`] changes the selection on a running torrent.

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use librqbit::limits::LimitsConfig;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ListenerOptions, ManagedTorrent,
    ManagedTorrentState, Session, SessionOptions, SessionPersistenceConfig,
};
use librqbit_core::magnet::Magnet;
use librqbit_core::Id20;
use luedd_net::{JobEvent, ProgressTx};
use tokio::sync::{Mutex, OnceCell};

use super::{BackendConfig, Confidence, DownloadBackend, DownloadReq, EntryMeta, Outcome, Sniff};

const DEFAULT_PORT: u16 = 4240;
/// A magnet gets this long to pull its metadata (find a peer that has it) before
/// the download is failed with a clear message rather than sitting forever.
const METADATA_TIMEOUT: Duration = Duration::from_secs(240);
/// The add-flow file preview does its own metadata fetch on a tighter budget.
const PREVIEW_TIMEOUT: Duration = Duration::from_secs(120);
/// Extra key carrying the selected file indices, e.g. `"0,3,4"`. Absent / empty
/// = download every file.
pub const FILES_EXTRA: &str = "torrent_files";

/// One torrent's live stats for the list row (`torrent_stats` command). Byte
/// counts absolute; speeds bytes/second.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentStat {
    pub url: String,
    pub name: String,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub dl_speed: u64,
    pub ul_speed: u64,
    pub uploaded_bytes: u64,
    /// Connected (live) peers. librqbit exposes no separate swarm seed count.
    pub peers: u32,
    /// `metadata` | `verifying` | `downloading` | `seeding` | `paused` | `error`.
    pub state: String,
    pub error: Option<String>,
    pub finished: bool,
    pub eta_secs: Option<u64>,
    pub ratio: f64,
}

/// One file inside a torrent, with its live progress and whether it's selected.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentFileStat {
    pub index: usize,
    pub path: String,
    pub size_bytes: u64,
    pub downloaded_bytes: u64,
    pub selected: bool,
}

/// One connected peer (`torrent_detail`). Byte counts are cumulative for the
/// connection — the frontend diffs successive polls for a rate.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentPeer {
    pub addr: String,
    pub client: Option<String>,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
}

/// Everything the expanded detail panel shows for one torrent.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentDetail {
    pub info_hash: String,
    pub save_path: String,
    pub total_pieces: u32,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub uploaded_bytes: u64,
    pub ratio: f64,
    pub files: Vec<TorrentFileStat>,
    pub peers: Vec<TorrentPeer>,
}

/// File list shown in the add dialog before the download starts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentPreview {
    pub name: String,
    pub total_bytes: u64,
    pub files: Vec<TorrentPreviewFile>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentPreviewFile {
    pub index: usize,
    pub path: String,
    pub size_bytes: u64,
}

pub struct TorrentBackend {
    session: OnceCell<Arc<Session>>,
    /// `<app data dir>/torrent`. Session persistence lives in `session/` under it.
    data_dir: PathBuf,
    handles: Mutex<HashMap<Id20, Arc<ManagedTorrent>>>,
    /// Entry URL -> info-hash, so pause/remove/detail can find the torrent by URL.
    url_index: Mutex<HashMap<String, Id20>>,
}

/// Aborts the spawned progress-emit task when the `run` future is dropped
/// (including on the queue's `handle.abort()` for a pause).
struct DropGuard(tokio::task::JoinHandle<()>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl TorrentBackend {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            session: OnceCell::new(),
            data_dir,
            handles: Mutex::new(HashMap::new()),
            url_index: Mutex::new(HashMap::new()),
        }
    }

    /// The shared session, created on first use from `cfg.torrent`. The first
    /// call also auto-resumes every persisted torrent.
    async fn session(&self, cfg: &BackendConfig) -> Result<Arc<Session>> {
        let sess = self
            .session
            .get_or_try_init(|| async {
                let t = &cfg.torrent;
                let port = t.listen_port.unwrap_or(DEFAULT_PORT);
                let mut opts = SessionOptions {
                    fastresume: true,
                    persistence: Some(SessionPersistenceConfig::Json {
                        folder: Some(self.data_dir.join("session")),
                    }),
                    ratelimits: LimitsConfig {
                        download_bps: kbps_to_bps(t.download_limit_kbps),
                        upload_bps: kbps_to_bps(t.upload_limit_kbps),
                    },
                    ..Default::default()
                };
                if t.enable_dht == Some(false) {
                    opts.dht = None;
                }
                // Bind IPv4 — the BitTorrent swarm is overwhelmingly v4 and a
                // `[::]` bind on Windows is v6-only, which strands the download
                // with zero reachable peers.
                opts.listen = Some(ListenerOptions {
                    listen_addr: (Ipv4Addr::UNSPECIFIED, port).into(),
                    enable_upnp_port_forwarding: t.enable_upnp != Some(false),
                    ..Default::default()
                });
                if let Some(p) = t.max_peers_per_torrent {
                    opts.peer_limit = Some(p as usize);
                }
                tokio::fs::create_dir_all(&self.data_dir).await.ok();
                Session::new_with_opts(self.data_dir.clone(), opts).await
            })
            .await?;
        Ok(sess.clone())
    }

    /// Eagerly start the session (called at app startup so persisted torrents
    /// resume without waiting for a new download).
    pub async fn ensure_session(&self, cfg: &BackendConfig) -> Result<()> {
        self.session(cfg).await.map(|_| ())
    }

    /// Live stats for the given entry URLs (skips URLs with no live torrent).
    /// The `handle.stats()` reads take librqbit's internal blocking locks, which
    /// contend with the transfer / metadata-fetch hot path — run them off the
    /// async worker so the UI stays responsive.
    pub async fn stats(&self, urls: &[String]) -> Vec<TorrentStat> {
        let pairs: Vec<(String, Arc<ManagedTorrent>)> = {
            let idx = self.url_index.lock().await;
            let handles = self.handles.lock().await;
            urls.iter()
                .filter_map(|u| Some((u.clone(), handles.get(idx.get(u)?)?.clone())))
                .collect()
        };
        if pairs.is_empty() {
            return Vec::new();
        }
        tokio::task::spawn_blocking(move || {
            pairs.iter().map(|(u, h)| to_stat(u.clone(), h)).collect()
        })
        .await
        .unwrap_or_default()
    }

    /// Full detail for one torrent's expanded panel (files + connected peers).
    /// Also off the async worker — the peer snapshot walks a live DashMap.
    pub async fn detail(&self, url: &str) -> Option<TorrentDetail> {
        let hash = *self.url_index.lock().await.get(url)?;
        let handle = self.handles.lock().await.get(&hash).cloned()?;
        tokio::task::spawn_blocking(move || build_torrent_detail(hash, &handle))
            .await
            .ok()
            .flatten()
    }

    /// List the files in a torrent without downloading it — the add dialog's
    /// picker. For a magnet this fetches metadata (network), bounded by
    /// [`PREVIEW_TIMEOUT`].
    pub async fn preview(&self, url: &str, cfg: &BackendConfig) -> Result<TorrentPreview> {
        // Already added? Read the file list straight off the handle.
        if let Some(d) = self.detail(url).await {
            return Ok(TorrentPreview {
                name: display_name(url),
                total_bytes: d.total_bytes,
                files: d
                    .files
                    .into_iter()
                    .map(|f| TorrentPreviewFile {
                        index: f.index,
                        path: f.path,
                        size_bytes: f.size_bytes,
                    })
                    .collect(),
            });
        }
        let sess = self.session(cfg).await?;
        let resp = tokio::time::timeout(
            PREVIEW_TIMEOUT,
            sess.add_torrent(
                add_torrent_for(url)?,
                Some(AddTorrentOptions { list_only: true, ..Default::default() }),
            ),
        )
        .await
        .context("timed out fetching torrent metadata — no reachable seeds/peers?")??;

        let lo = match resp {
            AddTorrentResponse::ListOnly(lo) => lo,
            _ => bail!("torrent is already in the list — open it there to change files"),
        };
        let name = lo
            .info
            .name()
            .map(|c| c.into_owned())
            .unwrap_or_else(|| display_name(url));
        let mut files = Vec::new();
        let mut total = 0u64;
        for (i, fd) in lo.info.iter_file_details().enumerate() {
            total += fd.len;
            files.push(TorrentPreviewFile {
                index: i,
                path: fd.filename.to_string(),
                size_bytes: fd.len,
            });
        }
        Ok(TorrentPreview { name, total_bytes: total, files })
    }

    /// Change which files a running torrent downloads.
    pub async fn set_files(&self, url: &str, indices: Vec<usize>) -> Result<()> {
        let sess = self.session.get().context("torrent session not started")?;
        let hash = self
            .url_index
            .lock()
            .await
            .get(url)
            .copied()
            .context("torrent is not active")?;
        let handle = self
            .handles
            .lock()
            .await
            .get(&hash)
            .cloned()
            .context("torrent is not active")?;
        let set: HashSet<usize> = indices.into_iter().collect();
        sess.update_only_files(&handle, &set).await
    }
}

#[async_trait]
impl DownloadBackend for TorrentBackend {
    fn id(&self) -> &'static str {
        "torrent"
    }

    fn is_torrent(&self) -> bool {
        true
    }

    fn can_handle(&self, url: &str, _sniff: Option<&Sniff>) -> Confidence {
        if url.starts_with("magnet:") {
            return Confidence::Certain;
        }
        let path = url.split(['?', '#']).next().unwrap_or(url).to_ascii_lowercase();
        if path.ends_with(".torrent") {
            return Confidence::Strong;
        }
        Confidence::No
    }

    async fn describe(&self, req: &DownloadReq) -> EntryMeta {
        let name = display_name(&req.url);
        let base = req
            .config
            .torrent
            .download_dir
            .clone()
            .unwrap_or_else(|| req.dest_dir.clone());
        EntryMeta {
            title: Some(name.clone()),
            media_class: Some("torrent".to_string()),
            out_dir: Some(base.join(sanitize(&name))),
            ..Default::default()
        }
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let sess = self.session(&req.config).await?;
        let tx = progress.cloned();
        emit_wait(&tx);

        let name = display_name(&req.url);
        let base = req
            .config
            .torrent
            .download_dir
            .clone()
            .unwrap_or_else(|| req.dest_dir.clone());
        let out_dir = base.join(sanitize(&name));
        tokio::fs::create_dir_all(&out_dir).await.ok();

        let only_files = parse_file_selection(req.extras.get(FILES_EXTRA));
        let opts = AddTorrentOptions {
            output_folder: Some(out_dir.to_string_lossy().into_owned()),
            overwrite: true,
            paused: false,
            only_files,
            ..Default::default()
        };

        // `add_torrent` returns almost immediately for a magnet (metadata comes
        // later) — the 5s heartbeat only covers a slow `.torrent` fetch.
        let add_fut = sess.add_torrent(add_torrent_for(&req.url)?, Some(opts));
        tokio::pin!(add_fut);
        let resp = {
            let hb = tx.clone();
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.tick().await;
            tokio::time::timeout(Duration::from_secs(120), async move {
                loop {
                    tokio::select! {
                        r = &mut add_fut => break r,
                        _ = ticker.tick() => emit_wait(&hb),
                    }
                }
            })
            .await
            .context("timed out adding torrent")??
        };

        let handle = resp.into_handle().context("torrent add returned no handle")?;
        let hash = handle.info_hash();
        // Covers a re-added torrent the session had persisted as paused (Resume).
        let _ = sess.unpause(&handle).await;
        self.handles.lock().await.insert(hash, handle.clone());
        self.url_index.lock().await.insert(req.url.clone(), hash);

        let _guard = DropGuard(tokio::spawn({
            let h = handle.clone();
            let tx = tx.clone();
            async move {
                let mut iv = tokio::time::interval(Duration::from_secs(1));
                loop {
                    iv.tick().await;
                    let s = h.stats();
                    let speed = s
                        .live
                        .as_ref()
                        .map(|l| l.download_speed.as_bytes())
                        .unwrap_or(0);
                    if let Some(tx) = &tx {
                        let _ = tx.send(JobEvent::Progress {
                            downloaded_bytes: s.progress_bytes,
                            total_bytes: (s.total_bytes > 0).then_some(s.total_bytes),
                            done_units: 0,
                            total_units: 0,
                            speed_bps: speed,
                        });
                    }
                }
            }
        }));

        // Bounded wait for metadata — a dead magnet with no seeds would
        // otherwise leave the entry "downloading" forever at 0 bytes.
        match tokio::time::timeout(METADATA_TIMEOUT, wait_for_metadata(&handle)).await {
            Err(_) => bail!(
                "couldn't fetch torrent metadata in {}s — no reachable seeds or peers \
                 (check your connection / firewall / VPN; the torrent may also be dead)",
                METADATA_TIMEOUT.as_secs()
            ),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(())) => {}
        }

        handle
            .wait_until_completed()
            .await
            .context("torrent download failed")?;
        drop(_guard);

        // A final 100% tick so the stored progress isn't frozen just short of
        // the end (the emit loop is polled, not exact).
        if let Some(tx) = &tx {
            let done = handle.stats().total_bytes;
            let _ = tx.send(JobEvent::Progress {
                downloaded_bytes: done,
                total_bytes: Some(done),
                done_units: 0,
                total_units: 0,
                speed_bps: 0,
            });
        }

        let folder = handle.output_folder().to_path_buf();
        let only = handle.only_files();
        let files: Vec<PathBuf> = handle
            .with_metadata(|m| {
                m.file_infos
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| only.as_ref().map_or(true, |o| o.contains(i)))
                    .map(|(_, fi)| folder.join(&fi.relative_filename))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let meta = EntryMeta {
            title: handle.name().or(Some(name)),
            media_class: Some("torrent".to_string()),
            out_dir: Some(folder),
            ..Default::default()
        };

        // Seed policy. Default: drop the torrent from the session at 100% (files
        // stay on disk). `seed_forever`: leave it running.
        if req.config.torrent.seed_forever != Some(true) {
            let _ = sess.delete(hash.into(), false).await;
            self.handles.lock().await.remove(&hash);
            self.url_index.lock().await.remove(&req.url);
        }

        let n = files.len() as u64;
        Ok(Outcome { files, meta, expected_units: Some(n) })
    }

    async fn on_pause(&self, url: &str) -> Result<()> {
        let Some(sess) = self.session.get() else { return Ok(()) };
        let hash = self.url_index.lock().await.get(url).copied();
        if let Some(hash) = hash {
            let handle = self.handles.lock().await.get(&hash).cloned();
            if let Some(handle) = handle {
                sess.pause(&handle).await?;
            }
        }
        Ok(())
    }

    async fn on_remove(&self, url: &str, delete_files: bool) -> Result<()> {
        let hash = self.url_index.lock().await.remove(url);
        if let Some(hash) = hash {
            self.handles.lock().await.remove(&hash);
            if let Some(sess) = self.session.get() {
                let _ = sess.delete(hash.into(), delete_files).await;
            }
        }
        Ok(())
    }
}

/// Wait until the torrent knows its size (metadata resolved) or errors.
async fn wait_for_metadata(h: &Arc<ManagedTorrent>) -> Result<()> {
    loop {
        let s = h.stats();
        if let Some(e) = s.error {
            bail!("{e}");
        }
        if s.total_bytes > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn add_torrent_for(url: &str) -> Result<AddTorrent<'static>> {
    if url.starts_with("magnet:") || url.starts_with("http://") || url.starts_with("https://") {
        Ok(AddTorrent::from_url(url.to_string()))
    } else {
        AddTorrent::from_local_filename(url)
            .with_context(|| format!("reading .torrent file {url:?}"))
    }
}

fn parse_file_selection(raw: Option<&String>) -> Option<Vec<usize>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let v: Vec<usize> = raw.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    (!v.is_empty()).then_some(v)
}

fn emit_wait(tx: &Option<ProgressTx>) {
    if let Some(tx) = tx {
        let _ = tx.send(JobEvent::Progress {
            downloaded_bytes: 0,
            total_bytes: None,
            done_units: 0,
            total_units: 0,
            speed_bps: 0,
        });
    }
}

fn kbps_to_bps(kbps: Option<u32>) -> Option<NonZeroU32> {
    kbps.filter(|v| *v > 0)
        .and_then(|v| NonZeroU32::new(v.saturating_mul(1024)))
}

fn ratio_of(s: &librqbit::TorrentStats) -> f64 {
    if s.total_bytes > 0 {
        s.uploaded_bytes as f64 / s.total_bytes as f64
    } else {
        0.0
    }
}

/// The blocking half of [`TorrentBackend::detail`] — runs on a blocking thread.
fn build_torrent_detail(hash: Id20, handle: &Arc<ManagedTorrent>) -> Option<TorrentDetail> {
    let s = handle.stats();
    let only = handle.only_files();
    let total_pieces = handle
        .with_metadata(|m| m.info.lengths().total_pieces())
        .unwrap_or(0);
    let file_meta: Vec<(String, u64)> = handle
        .with_metadata(|m| {
            m.file_infos
                .iter()
                .map(|fi| (fi.relative_filename.to_string_lossy().into_owned(), fi.len))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let files = file_meta
        .iter()
        .enumerate()
        .map(|(i, (p, sz))| TorrentFileStat {
            index: i,
            path: p.clone(),
            size_bytes: *sz,
            downloaded_bytes: s.file_progress.get(i).copied().unwrap_or(0),
            selected: only.as_ref().map_or(true, |o| o.contains(&i)),
        })
        .collect();
    let peers = handle.with_state(|st| match st {
        ManagedTorrentState::Live(l) => l
            .per_peer_stats_snapshot(Default::default())
            .peers
            .into_iter()
            .map(|(addr, ps)| TorrentPeer {
                addr,
                client: ps.client_name,
                downloaded_bytes: ps.counters.fetched_bytes,
                uploaded_bytes: ps.counters.uploaded_bytes,
            })
            .collect(),
        _ => Vec::new(),
    });
    Some(TorrentDetail {
        info_hash: hash.as_string(),
        save_path: handle.output_folder().to_string_lossy().into_owned(),
        total_pieces,
        progress_bytes: s.progress_bytes,
        total_bytes: s.total_bytes,
        uploaded_bytes: s.uploaded_bytes,
        ratio: ratio_of(&s),
        files,
        peers,
    })
}

fn to_stat(url: String, h: &Arc<ManagedTorrent>) -> TorrentStat {
    let s = h.stats();
    let (dl, ul) = s
        .live
        .as_ref()
        .map(|l| (l.download_speed.as_bytes(), l.upload_speed.as_bytes()))
        .unwrap_or((0, 0));
    let peers = s
        .live
        .as_ref()
        .map(|l| l.snapshot.peer_stats.live)
        .unwrap_or(0);
    let eta_secs = if dl > 0 && s.total_bytes > s.progress_bytes {
        Some((s.total_bytes - s.progress_bytes) / dl)
    } else {
        None
    };
    // Map librqbit's raw state to what the row actually shows.
    let raw = s.state.to_string();
    let state = match raw.as_str() {
        "initializing" if s.total_bytes == 0 => "metadata",
        "initializing" => "verifying",
        "live" if s.finished => "seeding",
        "live" => "downloading",
        "paused" => "paused",
        _ => "error",
    }
    .to_string();
    TorrentStat {
        name: h.name().unwrap_or_else(|| display_name(&url)),
        url,
        progress_bytes: s.progress_bytes,
        total_bytes: s.total_bytes,
        dl_speed: dl,
        ul_speed: ul,
        uploaded_bytes: s.uploaded_bytes,
        peers,
        state,
        error: s.error.clone(),
        finished: s.finished,
        eta_secs,
        ratio: ratio_of(&s),
    }
}

/// A human name for the torrent, before it resolves: magnet `dn=`, or the
/// `.torrent` file's base name, else "torrent".
fn display_name(url: &str) -> String {
    if url.starts_with("magnet:") {
        if let Ok(m) = Magnet::parse(url) {
            if let Some(n) = m.name {
                if !n.trim().is_empty() {
                    return n;
                }
            }
        }
        return "torrent".to_string();
    }
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let stem = path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(path)
        .strip_suffix(".torrent")
        .or_else(|| path.rsplit(['/', '\\']).next())
        .unwrap_or("torrent");
    let stem = stem.trim();
    if stem.is_empty() {
        "torrent".to_string()
    } else {
        stem.to_string()
    }
}

/// Filesystem-safe folder name.
fn sanitize(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        "torrent".to_string()
    } else {
        trimmed.chars().take(120).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn be() -> TorrentBackend {
        TorrentBackend::new(PathBuf::from("."))
    }

    #[test]
    fn can_handle_matrix() {
        let b = be();
        assert_eq!(
            b.can_handle("magnet:?xt=urn:btih:abc", None),
            Confidence::Certain
        );
        assert_eq!(
            b.can_handle("https://example.com/foo.torrent", None),
            Confidence::Strong
        );
        assert_eq!(
            b.can_handle("https://example.com/foo.torrent?x=1", None),
            Confidence::Strong
        );
        assert_eq!(b.can_handle("C:\\dl\\Ubuntu.TORRENT", None), Confidence::Strong);
        assert_eq!(
            b.can_handle("https://example.com/video.mp4", None),
            Confidence::No
        );
    }

    #[test]
    fn magnet_display_name() {
        const IH: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        assert_eq!(
            display_name(&format!("magnet:?xt=urn:btih:{IH}&dn=Debian+ISO")),
            "Debian ISO"
        );
        assert_eq!(display_name(&format!("magnet:?xt=urn:btih:{IH}")), "torrent");
    }

    #[test]
    fn torrent_file_display_name() {
        assert_eq!(
            display_name("https://x.org/path/Big-Buck-Bunny.torrent"),
            "Big-Buck-Bunny"
        );
        assert_eq!(display_name("/home/u/My Linux.torrent"), "My Linux");
    }

    #[test]
    fn sanitize_strips_separators() {
        assert_eq!(sanitize("a/b:c*d?"), "a_b_c_d_");
        assert_eq!(sanitize("   "), "torrent");
        assert_eq!(sanitize("..hidden.."), "hidden");
    }

    #[test]
    fn kbps_conversion() {
        assert_eq!(kbps_to_bps(None), None);
        assert_eq!(kbps_to_bps(Some(0)), None);
        assert_eq!(kbps_to_bps(Some(100)).unwrap().get(), 102_400);
    }

    #[test]
    fn file_selection_parsing() {
        assert_eq!(parse_file_selection(None), None);
        assert_eq!(parse_file_selection(Some(&String::new())), None);
        assert_eq!(parse_file_selection(Some(&"  ".to_string())), None);
        assert_eq!(
            parse_file_selection(Some(&"0, 2 ,5".to_string())),
            Some(vec![0, 2, 5])
        );
        assert_eq!(
            parse_file_selection(Some(&"1,bad,3".to_string())),
            Some(vec![1, 3])
        );
    }
}
