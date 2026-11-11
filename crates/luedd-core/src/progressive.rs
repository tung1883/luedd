
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use luedd_net::{HttpClient, ProgressTracker, ProgressTx, RequestContext};

use crate::naming::cache_dir_for;

const STATE_SAVE_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Piece {
    offset: u64,
    length: u64,
    downloaded: u64,
}

impl Piece {
    fn is_finished(&self) -> bool {
        self.downloaded >= self.length
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DownloadState {
    url: String,
    total_size: u64,
    pieces: Vec<Piece>,
}

/// Files up to this size just get one piece per connection.
const SMALL_FILE: u64 = 8 << 20;
/// Bigger files are cut into pieces of this size range that workers pull from a
/// queue, so a slow piece never leaves the other connections idle and a failed
/// piece only redoes a few MB.
const PIECE_MIN: u64 = 4 << 20;
const PIECE_MAX: u64 = 16 << 20;
/// Network chunks are gathered into batches this big before hitting the disk
/// (fewer, larger sequential writes), or flushed after `FLUSH_EVERY` so progress
/// stays live on a slow link.
const WRITE_BATCH: usize = 1 << 20;
const FLUSH_EVERY: Duration = Duration::from_millis(250);

/// Where the unfinished download lives: right next to the final file, so the last
/// step is a same-volume rename (no second copy of the data). An older partial
/// left in the cache dir by a previous version is still picked up and resumed.
pub fn partial_path_for(dest: &Path) -> PathBuf {
    let mut name = dest.file_name().map(|n| n.to_os_string()).unwrap_or_else(|| "download".into());
    name.push(".luedd-part");
    dest.with_file_name(name)
}

fn work_path_for(dest: &Path, cache_dir: &Path) -> PathBuf {
    let legacy = cache_dir.join("output.partial");
    if legacy.exists() {
        legacy
    } else {
        partial_path_for(dest)
    }
}

pub async fn download(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let cache_dir = cache_dir_for(dest);
    tokio::fs::create_dir_all(&cache_dir).await?;
    let state_file = cache_dir.join("state.json");
    let work_path = work_path_for(dest, &cache_dir);

    // resume only if the partial file is really there at the size we planned for
    let existing_state = match load_state(&state_file, url).await {
        Some(s) if partial_matches(&work_path, s.total_size).await => Some(s),
        _ => None,
    };

    let state = match existing_state {
        Some(s) => {
            tracing::info!(
                remaining = s.pieces.iter().filter(|p| !p.is_finished()).count(),
                total_pieces = s.pieces.len(),
                "resuming previous download"
            );
            s
        }
        None => {
            let probe = client.probe(url, &ctx.to_options(None)).await?;
            match (probe.accept_ranges, probe.content_length) {
                (true, Some(total_size)) if total_size > 0 => {
                    let pieces = plan_pieces(total_size, concurrency.max(1));
                    let file = OpenOptions::new().create(true).write(true).truncate(true).open(&work_path).await?;
                    file.set_len(total_size).await?;
                    DownloadState { url: url.to_string(), total_size, pieces }
                }
                _ => {
                    tracing::info!("server does not support byte ranges; downloading as a single stream");
                    return download_single_stream(client, url, dest, &cache_dir, ctx, progress).await;
                }
            }
        }
    };

    download_resumable(client, state, dest, &cache_dir, &work_path, &state_file, concurrency.max(1), ctx, progress).await
}

async fn partial_matches(path: &Path, total_size: u64) -> bool {
    tokio::fs::metadata(path).await.map(|m| m.len() == total_size).unwrap_or(false)
}

/// Small files: one piece per connection. Big files: pieces of 4-16 MB.
fn plan_pieces(total_size: u64, concurrency: usize) -> Vec<Piece> {
    let conc = concurrency.max(1) as u64;
    if total_size <= SMALL_FILE {
        return split_into_pieces(total_size, conc);
    }
    let piece_len = (total_size / (conc * 8)).clamp(PIECE_MIN, PIECE_MAX);
    split_into_pieces(total_size, total_size.div_ceil(piece_len).max(1))
}

fn split_into_pieces(total_size: u64, count: u64) -> Vec<Piece> {
    let base = total_size / count;
    let mut pieces = Vec::with_capacity(count as usize);
    let mut offset = 0u64;
    for i in 0..count {
        let length = if i == count - 1 { total_size - offset } else { base };
        if length == 0 {
            continue;
        }
        pieces.push(Piece { offset, length, downloaded: 0 });
        offset += length;
    }
    pieces
}

async fn load_state(state_file: &Path, url: &str) -> Option<DownloadState> {
    let bytes = tokio::fs::read(state_file).await.ok()?;
    let state: DownloadState = serde_json::from_slice(&bytes).ok()?;
    if state.url != url {
        return None;
    }
    Some(state)
}

async fn save_state(state_file: &Path, state: &DownloadState) -> Result<()> {
    let json = serde_json::to_vec(state)?;
    tokio::fs::write(state_file, json).await?;
    Ok(())
}

/// Per-piece progress: plain atomics, so the write loop takes no lock per chunk.
struct Progress {
    url: String,
    total_size: u64,
    /// `(offset, length)` of every piece
    layout: Vec<(u64, u64)>,
    downloaded: Vec<AtomicU64>,
}

impl Progress {
    fn snapshot(&self) -> DownloadState {
        DownloadState {
            url: self.url.clone(),
            total_size: self.total_size,
            pieces: self
                .layout
                .iter()
                .zip(&self.downloaded)
                .map(|(&(offset, length), d)| Piece { offset, length, downloaded: d.load(Ordering::Relaxed).min(length) })
                .collect(),
        }
    }

    fn unfinished(&self) -> usize {
        self.layout.iter().zip(&self.downloaded).filter(|((_, len), d)| d.load(Ordering::Relaxed) < *len).count()
    }
}

async fn download_resumable(
    client: &HttpClient,
    state: DownloadState,
    dest: &Path,
    cache_dir: &Path,
    work_path: &Path,
    state_file: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let total_size = state.total_size;
    let prog = Arc::new(Progress {
        url: state.url.clone(),
        total_size,
        layout: state.pieces.iter().map(|p| (p.offset, p.length)).collect(),
        downloaded: state.pieces.iter().map(|p| AtomicU64::new(p.downloaded.min(p.length))).collect(),
    });
    // the pieces still to do, handed out one at a time
    let pending: Arc<Vec<usize>> =
        Arc::new(state.pieces.iter().enumerate().filter(|(_, p)| !p.is_finished()).map(|(i, _)| i).collect());
    let cursor = Arc::new(AtomicUsize::new(0));
    let tracker = Arc::new(ProgressTracker::new(progress, (total_size > 0).then_some(total_size), 0));

    // resume info is written on a timer, not from the hot write loop
    let saver = {
        let (prog, state_file) = (prog.clone(), state_file.to_path_buf());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(STATE_SAVE_INTERVAL);
            tick.tick().await;
            loop {
                tick.tick().await;
                save_state(&state_file, &prog.snapshot()).await.ok();
            }
        })
    };

    let workers_n = concurrency.min(pending.len()).max(1);
    let mut workers: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(workers_n);
    for _ in 0..workers_n {
        let client = client.clone();
        let (prog, pending, cursor) = (prog.clone(), pending.clone(), cursor.clone());
        let work_path = work_path.to_path_buf();
        let ctx = ctx.clone();
        let tracker = tracker.clone();

        workers.push(tokio::spawn(async move {
            loop {
                let n = cursor.fetch_add(1, Ordering::Relaxed);
                let Some(&index) = pending.get(n) else { break };
                luedd_net::retry(luedd_net::DEFAULT_RETRY_ATTEMPTS, luedd_net::DEFAULT_RETRY_DELAY, || {
                    download_piece(&client, &work_path, &prog, index, &ctx, &tracker)
                })
                .await
                .with_context(|| format!("piece {index} failed after retries"))?;
            }
            Ok(())
        }));
    }

    let mut outcome: Result<()> = Ok(());
    for worker in workers {
        let r = worker.await.context("download worker panicked").and_then(|r| r);
        if outcome.is_ok() {
            outcome = r;
        }
    }
    saver.abort();
    let unfinished = prog.unfinished();
    if outcome.is_err() || unfinished > 0 {
        // leave an accurate resume point; a finished download writes nothing more
        save_state(state_file, &prog.snapshot()).await.ok();
    }
    outcome?;
    if unfinished > 0 {
        bail!("download incomplete: {unfinished} piece(s) unfinished after all workers exited");
    }
    crate::atomicfile::move_file(work_path, dest).await.context("moving finished download into place")?;
    tokio::fs::remove_dir_all(cache_dir).await.ok();
    tracker.finish();
    tracing::info!(total_size, path = %dest.display(), "download complete");
    Ok(())
}

/// Positional write (no shared cursor): `pwrite` on unix, `seek_write` on Windows.
#[cfg(windows)]
fn write_all_at(f: &std::fs::File, mut buf: &[u8], mut off: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = f.seek_write(buf, off)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        off += n as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(f: &std::fs::File, mut buf: &[u8], mut off: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buf.is_empty() {
        let n = f.write_at(buf, off)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        off += n as u64;
    }
    Ok(())
}

/// Write buffers are recycled across pieces: allocating and first-touching fresh
/// megabytes for every piece shows up as kernel page-fault time.
static BUF_POOL: std::sync::Mutex<Vec<Vec<u8>>> = std::sync::Mutex::new(Vec::new());

fn take_buf() -> Vec<u8> {
    BUF_POOL.lock().ok().and_then(|mut p| p.pop()).unwrap_or_else(|| Vec::with_capacity(WRITE_BATCH))
}

fn give_buf(mut b: Vec<u8>) {
    b.clear();
    if b.capacity() >= WRITE_BATCH {
        if let Ok(mut p) = BUF_POOL.lock() {
            if p.len() < 32 {
                p.push(b);
            }
        }
    }
}

type WriteJob = tokio::task::JoinHandle<std::io::Result<Vec<u8>>>;

#[allow(unused_assignments)] // the buffer swap in the macros reads/writes `spare` on some paths only
async fn download_piece(
    client: &HttpClient,
    work_path: &Path,
    prog: &Progress,
    index: usize,
    ctx: &RequestContext,
    tracker: &ProgressTracker,
) -> Result<()> {
    let (offset, length) = prog.layout[index];
    let done = prog.downloaded[index].load(Ordering::Relaxed);
    if done >= length {
        return Ok(());
    }

    let range_offset = offset + done;
    let range_len = length - done;
    let url = prog.url.as_str();
    let opts = ctx.to_options(Some((range_offset, range_len)));

    let response = client.get_response(url, &opts).await?;
    let status = response.status();
    if !(status.as_u16() == 206 || status.is_success()) {
        bail!("piece {index} GET {url} returned status {status} ({})", luedd_net::describe_sent_headers(&opts));
    }
    // a plain 200 to a ranged request is the whole file from byte 0: writing that
    // at an offset would silently corrupt the download
    if status.as_u16() == 200 && range_offset > 0 {
        bail!("piece {index}: the server ignored the Range header (HTTP 200 instead of 206)");
    }

    let file = Arc::new(
        std::fs::OpenOptions::new().write(true).open(work_path).context("opening destination file for piece write")?,
    );

    // Two buffers: one fills from the network while the other is written on a
    // blocking thread. Progress advances only after a batch is really written, so a
    // resume point never claims data that is not in the file.
    let mut buf: Vec<u8> = take_buf();
    let mut spare: Option<Vec<u8>> = None;
    let mut inflight: Option<(WriteJob, u64)> = None;
    let mut next_off = range_offset; // file offset of the first byte in `buf`
    let mut received: u64 = 0; // bytes taken from the network
    let mut written: u64 = 0; // bytes confirmed on disk
    let mut last_flush = Instant::now();

    macro_rules! finish_inflight {
        () => {
            if let Some((job, n)) = inflight.take() {
                let mut old = job.await.context("disk write task panicked")?.context("writing piece to disk")?;
                old.clear();
                spare = Some(old);
                prog.downloaded[index].fetch_add(n, Ordering::Relaxed);
                tracker.add_bytes(n);
                written += n;
            }
        };
    }
    macro_rules! submit_buf {
        () => {
            if !buf.is_empty() {
                finish_inflight!();
                let out = std::mem::replace(&mut buf, spare.take().unwrap_or_else(take_buf));
                let (off, n) = (next_off, out.len() as u64);
                next_off += n;
                let f = file.clone();
                inflight = Some((
                    tokio::task::spawn_blocking(move || write_all_at(&f, &out, off).map(|_| out)),
                    n,
                ));
                last_flush = Instant::now();
            }
        };
    }

    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error reading response body")?;
        let take = (range_len - received).min(chunk.len() as u64) as usize;
        buf.extend_from_slice(&chunk[..take]);
        received += take as u64;
        if buf.len() >= WRITE_BATCH || last_flush.elapsed() >= FLUSH_EVERY || received >= range_len {
            submit_buf!();
        }
        if received >= range_len {
            break;
        }
    }
    submit_buf!();
    finish_inflight!();
    give_buf(std::mem::take(&mut buf));
    if let Some(b) = spare.take() {
        give_buf(b);
    }

    if written < range_len {
        bail!("piece {index} ended early: got {written} of {range_len} expected bytes");
    }

    Ok(())
}

async fn download_single_stream(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    cache_dir: &Path,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let opts = ctx.to_options(None);
    let response = client.get_response(url, &opts).await?;
    let status = response.status();
    if !status.is_success() {
        bail!("GET {url} returned status {status} ({})", luedd_net::describe_sent_headers(&opts));
    }
    let total_size = response.content_length().unwrap_or(0);
    let tracker = ProgressTracker::new(progress, (total_size > 0).then_some(total_size), 0);

    let work_path = partial_path_for(dest);
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(&work_path).await?;
    let mut writer = tokio::io::BufWriter::with_capacity(WRITE_BATCH, file);
    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error reading response body")?;
        writer.write_all(&chunk).await?;
        total += chunk.len() as u64;
        tracker.add_bytes(chunk.len() as u64);
    }
    writer.flush().await?;
    drop(writer);
    crate::atomicfile::move_file(&work_path, dest).await.context("moving finished download into place")?;
    tokio::fs::remove_dir_all(cache_dir).await.ok();
    tracker.finish();
    tracing::info!(total, path = %dest.display(), "download complete (non-resumable single stream)");
    Ok(())
}

/// Fast path for small files whose size is not known up front (Instagram CDN
/// images): one GET streamed into `<name>.luedd-part`, then renamed. No HEAD
/// probe, no cache dir, no resume state, no preallocation - none of that pays off
/// for a few hundred KB.
pub async fn download_simple(client: &HttpClient, url: &str, dest: &Path, ctx: &RequestContext) -> Result<()> {
    if let Some(parent) = dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let opts = ctx.to_options(None);
    let response = client.get_response(url, &opts).await?;
    let status = response.status();
    if !status.is_success() {
        bail!("GET {url} returned status {status} ({})", luedd_net::describe_sent_headers(&opts));
    }
    let work_path = partial_path_for(dest);
    let result: Result<()> = async {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&work_path).await?;
        let mut writer = tokio::io::BufWriter::with_capacity(WRITE_BATCH, file);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk.context("error reading response body")?).await?;
        }
        writer.flush().await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => crate::atomicfile::move_file(&work_path, dest).await.context("moving finished download into place"),
        Err(e) => {
            tokio::fs::remove_file(&work_path).await.ok();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn splits_evenly_divisible_size() {
        let pieces = split_into_pieces(1000, 4);
        assert_eq!(pieces.len(), 4);
        for p in &pieces {
            assert_eq!(p.length, 250);
        }
        assert_eq!(pieces[3].offset, 750);
    }

    #[test]
    fn last_piece_absorbs_remainder() {
        let pieces = split_into_pieces(1003, 4);
        assert_eq!(pieces.iter().map(|p| p.length).sum::<u64>(), 1003);
        assert_eq!(pieces.last().unwrap().length, 253);
    }

    #[test]
    fn single_piece_when_smaller_than_concurrency() {
        let pieces = split_into_pieces(10, 8);
        assert_eq!(pieces.iter().map(|p| p.length).sum::<u64>(), 10);
        assert!(pieces.len() <= 8);
    }

    #[test]
    fn small_files_get_one_piece_per_connection() {
        assert_eq!(plan_pieces(5 << 20, 4).len(), 4);
    }

    #[test]
    fn big_files_are_cut_into_queue_sized_pieces() {
        for (total, conc) in [(100u64 << 20, 4usize), (3 << 30, 8), (9 << 20, 2)] {
            let pieces = plan_pieces(total, conc);
            assert_eq!(pieces.iter().map(|p| p.length).sum::<u64>(), total);
            // contiguous, no gaps or overlaps
            let mut at = 0;
            for p in &pieces {
                assert_eq!(p.offset, at);
                at += p.length;
            }
            // several pieces per connection, none bigger than ~1.5x the cap
            assert!(pieces.len() >= conc, "{total}: only {} pieces", pieces.len());
            assert!(pieces.iter().all(|p| p.length <= PIECE_MAX + PIECE_MAX / 2), "{total}: piece too big");
        }
    }

    #[test]
    fn partial_file_sits_next_to_the_destination() {
        let dest = Path::new("/videos/clip.mp4");
        let p = partial_path_for(dest);
        assert_eq!(p.parent(), dest.parent());
        assert_eq!(p.file_name().unwrap().to_str().unwrap(), "clip.mp4.luedd-part");
    }

    /// A tiny HTTP/1.1 server with Range support, for a real end-to-end download.
    fn serve(data: Arc<Vec<u8>>, honor_range: bool) -> (String, Arc<AtomicU64>) {
        let served = Arc::new(AtomicU64::new(0));
        let served_for_threads = served.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut s) = conn else { continue };
                let data = data.clone();
                let served = served_for_threads.clone();
                std::thread::spawn(move || {
                    let mut pending = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        let head_end = loop {
                            if let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                                break Some(i);
                            }
                            match s.read(&mut tmp) {
                                Ok(0) | Err(_) => break None,
                                Ok(n) => pending.extend_from_slice(&tmp[..n]),
                            }
                        };
                        let Some(i) = head_end else { return };
                        let head = String::from_utf8_lossy(&pending[..i]).to_string();
                        pending.drain(..i + 4);
                        let method = head.split_whitespace().next().unwrap_or("").to_string();
                        let range = head.lines().find_map(|l| {
                            let l = l.to_ascii_lowercase();
                            l.strip_prefix("range: bytes=").map(str::to_string)
                        });
                        let n = data.len();
                        let (status, body, extra) = match (&range, honor_range) {
                            (Some(r), true) => {
                                let (a, b) = r.split_once('-').unwrap();
                                let a: usize = a.parse().unwrap();
                                let b: usize = b.parse::<usize>().unwrap().min(n - 1);
                                ("206 Partial Content", &data[a..=b], format!("Content-Range: bytes {a}-{b}/{n}\r\n"))
                            }
                            _ => ("200 OK", &data[..], String::new()),
                        };
                        let hdr = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{extra}Connection: keep-alive\r\n\r\n",
                            body.len()
                        );
                        if s.write_all(hdr.as_bytes()).is_err() {
                            return;
                        }
                        if method != "HEAD" {
                            if s.write_all(body).is_err() {
                                return;
                            }
                            served.fetch_add(body.len() as u64, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        (format!("http://{addr}/file.bin"), served)
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n).map(|i| ((i * 31 + i / 251) % 256) as u8).collect()
    }

    #[tokio::test]
    async fn downloads_a_multi_piece_file_intact_with_no_leftovers() {
        let data = Arc::new(pattern(21 * 1024 * 1024 + 123));
        let (url, _served) = serve(data.clone(), true);
        let dir = std::env::temp_dir().join(format!("luedd-prog-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let dest = dir.join("multi.bin");
        let client = HttpClient::new().unwrap();

        download(&client, &url, &dest, 4, &RequestContext::default(), None).await.unwrap();

        assert_eq!(tokio::fs::read(&dest).await.unwrap(), *data);
        assert!(!partial_path_for(&dest).exists(), "partial file must be renamed away");
        assert!(!cache_dir_for(&dest).exists(), "cache dir must be cleaned up");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn resumes_from_a_saved_state_without_redoing_finished_pieces() {
        let data = Arc::new(pattern(20 * 1024 * 1024));
        let (url, served) = serve(data.clone(), true);
        let dir = std::env::temp_dir().join(format!("luedd-prog-resume-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let dest = dir.join("resume.bin");
        let cache_dir = cache_dir_for(&dest);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();

        // an interrupted run: first two 4 MiB pieces done and on disk, the rest empty
        let pieces = plan_pieces(data.len() as u64, 4);
        let done_pieces = 2usize;
        let mut file_bytes = vec![0u8; data.len()];
        let mut state_pieces = pieces.clone();
        for (i, p) in state_pieces.iter_mut().enumerate() {
            if i < done_pieces {
                p.downloaded = p.length;
                let (a, b) = (p.offset as usize, (p.offset + p.length) as usize);
                file_bytes[a..b].copy_from_slice(&data[a..b]);
            }
        }
        tokio::fs::write(partial_path_for(&dest), &file_bytes).await.unwrap();
        let state = DownloadState { url: url.clone(), total_size: data.len() as u64, pieces: state_pieces };
        tokio::fs::write(cache_dir.join("state.json"), serde_json::to_vec(&state).unwrap()).await.unwrap();

        let client = HttpClient::new().unwrap();
        download(&client, &url, &dest, 4, &RequestContext::default(), None).await.unwrap();

        assert_eq!(tokio::fs::read(&dest).await.unwrap(), *data);
        assert!(!partial_path_for(&dest).exists());
        // only the three unfinished 4 MiB pieces were fetched, not the whole file
        let fetched = served.load(Ordering::Relaxed);
        assert!(fetched <= 3 * (4 << 20), "re-downloaded finished pieces: server sent {fetched} bytes");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_partial_of_the_wrong_size_is_not_trusted() {
        let data = Arc::new(pattern(12 * 1024 * 1024));
        let (url, _served) = serve(data.clone(), true);
        let dir = std::env::temp_dir().join(format!("luedd-prog-badpartial-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let dest = dir.join("bad.bin");
        let cache_dir = cache_dir_for(&dest);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();

        // state claims everything is done, but the partial file is truncated
        let pieces: Vec<Piece> = plan_pieces(data.len() as u64, 4).into_iter().map(|mut p| { p.downloaded = p.length; p }).collect();
        tokio::fs::write(partial_path_for(&dest), b"short").await.unwrap();
        let state = DownloadState { url: url.clone(), total_size: data.len() as u64, pieces };
        tokio::fs::write(cache_dir.join("state.json"), serde_json::to_vec(&state).unwrap()).await.unwrap();

        let client = HttpClient::new().unwrap();
        download(&client, &url, &dest, 4, &RequestContext::default(), None).await.unwrap();
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), *data);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn simple_download_writes_the_file_and_leaves_nothing_behind() {
        let data = Arc::new(pattern(300_000));
        let (url, _served) = serve(data.clone(), true);
        let dir = std::env::temp_dir().join(format!("luedd-prog-simple-{}", std::process::id()));
        let dest = dir.join("nested").join("img.jpg"); // parent does not exist yet
        let client = HttpClient::new().unwrap();

        download_simple(&client, &url, &dest, &RequestContext::default()).await.unwrap();

        assert_eq!(tokio::fs::read(&dest).await.unwrap(), *data);
        assert!(!partial_path_for(&dest).exists());
        assert!(!cache_dir_for(&dest).exists(), "the fast path must not create a cache dir");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_server_that_ignores_range_never_corrupts_the_file() {
        let data = Arc::new(pattern(20 * 1024 * 1024));
        let (url, _served) = serve(data.clone(), false);
        let dir = std::env::temp_dir().join(format!("luedd-prog-norange-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let dest = dir.join("norange.bin");
        let client = HttpClient::new().unwrap();

        let r = download(&client, &url, &dest, 4, &RequestContext::default(), None).await;
        // either it fails loudly, or whatever it produced is byte-identical
        if r.is_ok() {
            assert_eq!(tokio::fs::read(&dest).await.unwrap(), *data);
        } else {
            assert!(!dest.exists(), "no half-written file may appear at the destination");
        }
        std::fs::remove_dir_all(&dir).ok();
        tokio::fs::remove_dir_all(cache_dir_for(&dest)).await.ok();
    }
}
