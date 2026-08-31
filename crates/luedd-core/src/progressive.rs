
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

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
    let work_path = cache_dir.join("output.partial");
    let state_file = cache_dir.join("state.json");

    let existing_state = load_state(&state_file, url).await;

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
                    let pieces = split_into_pieces(total_size, concurrency.max(1) as u64);
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

struct Shared {
    state: DownloadState,
    claimed: Vec<bool>,
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
    let url = state.url.clone();
    let claimed = vec![false; state.pieces.len()];
    let shared = Arc::new(Mutex::new(Shared { state, claimed }));
    let last_saved = Arc::new(Mutex::new(Instant::now()));
    let tracker = Arc::new(ProgressTracker::new(progress, (total_size > 0).then_some(total_size), 0));

    let mut workers: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let shared = shared.clone();
        let last_saved = last_saved.clone();
        let work_path = work_path.to_path_buf();
        let state_file = state_file.to_path_buf();
        let url = url.clone();
        let ctx = ctx.clone();
        let tracker = tracker.clone();

        workers.push(tokio::spawn(async move {
            loop {
                let piece_index = {
                    let mut s = shared.lock().await;
                    let idx = find_next_claimable_piece(&s.state.pieces, &s.claimed);
                    if let Some(i) = idx {
                        s.claimed[i] = true;
                    }
                    idx
                };
                let Some(index) = piece_index else { break };

                luedd_net::retry(luedd_net::DEFAULT_RETRY_ATTEMPTS, luedd_net::DEFAULT_RETRY_DELAY, || {
                    download_piece(&client, &url, &work_path, shared.clone(), index, &last_saved, &state_file, &ctx, &tracker)
                })
                .await
                .with_context(|| format!("piece {index} failed after retries"))?;
            }
            Ok(())
        }));
    }

    for worker in workers {
        worker.await.context("download worker panicked")??;
    }

    {
        let s = shared.lock().await;
        let unfinished = s.state.pieces.iter().filter(|p| !p.is_finished()).count();
        if unfinished > 0 {
            bail!("download incomplete: {unfinished} piece(s) unfinished after all workers exited");
        }
    }
    tokio::fs::rename(work_path, dest).await.context("moving finished download into place")?;
    tokio::fs::remove_dir_all(cache_dir).await.ok();
    tracker.finish();
    tracing::info!(total_size, path = %dest.display(), "download complete");
    Ok(())
}

fn find_next_claimable_piece(pieces: &[Piece], claimed: &[bool]) -> Option<usize> {
    pieces.iter().enumerate().position(|(i, p)| !p.is_finished() && !claimed[i])
}

async fn download_piece(
    client: &HttpClient,
    url: &str,
    work_path: &Path,
    shared: Arc<Mutex<Shared>>,
    index: usize,
    last_saved: &Arc<Mutex<Instant>>,
    state_file: &Path,
    ctx: &RequestContext,
    tracker: &ProgressTracker,
) -> Result<()> {
    let (offset, length, downloaded) = {
        let s = shared.lock().await;
        let p = &s.state.pieces[index];
        (p.offset, p.length, p.downloaded)
    };
    if downloaded >= length {
        return Ok(());
    }

    let range_offset = offset + downloaded;
    let range_len = length - downloaded;
    let opts = ctx.to_options(Some((range_offset, range_len)));

    let response = client.get_response(url, &opts).await?;
    let status = response.status();
    if !(status.as_u16() == 206 || status.is_success()) {
        bail!("piece {index} GET {url} returned status {status} ({})", luedd_net::describe_sent_headers(&opts));
    }

    let mut file = OpenOptions::new().write(true).open(work_path).await.context("opening destination file for piece write")?;
    file.seek(std::io::SeekFrom::Start(range_offset)).await?;

    let mut stream = response.bytes_stream();
    let mut written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error reading response body")?;
        file.write_all(&chunk).await?;
        written += chunk.len() as u64;
        tracker.add_bytes(chunk.len() as u64);

        let should_save = {
            let mut s = shared.lock().await;
            s.state.pieces[index].downloaded += chunk.len() as u64;

            let mut ls = last_saved.lock().await;
            if ls.elapsed() >= STATE_SAVE_INTERVAL {
                *ls = Instant::now();
                Some(s.state.clone())
            } else {
                None
            }
        };
        if let Some(state) = should_save {
            save_state(state_file, &state).await.ok();
        }

        if written >= range_len {
            break;
        }
    }
    file.flush().await?;

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

    let work_path = cache_dir.join("output.partial");
    let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&work_path).await?;
    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("error reading response body")?;
        file.write_all(&chunk).await?;
        total += chunk.len() as u64;
        tracker.add_bytes(chunk.len() as u64);
    }
    file.flush().await?;
    drop(file);
    tokio::fs::rename(&work_path, dest).await.context("moving finished download into place")?;
    tokio::fs::remove_dir_all(cache_dir).await.ok();
    tracker.finish();
    tracing::info!(total, path = %dest.display(), "download complete (non-resumable single stream)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn find_next_claimable_skips_finished_and_claimed() {
        let pieces = vec![
            Piece { offset: 0, length: 10, downloaded: 10 },
            Piece { offset: 10, length: 10, downloaded: 0 },
            Piece { offset: 20, length: 10, downloaded: 0 },
        ];
        let claimed = vec![false, true, false];
        assert_eq!(find_next_claimable_piece(&pieces, &claimed), Some(2));
    }

    #[test]
    fn find_next_claimable_none_when_all_done_or_claimed() {
        let pieces = vec![
            Piece { offset: 0, length: 10, downloaded: 10 },
            Piece { offset: 10, length: 10, downloaded: 0 },
        ];
        let claimed = vec![false, true];
        assert_eq!(find_next_claimable_piece(&pieces, &claimed), None);
    }
}
