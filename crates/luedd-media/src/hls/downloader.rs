use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use luedd_net::{HttpClient, ProgressTracker, RequestContext};

use super::model::{HlsMediaSegment, HlsPlaylist};
use crate::crypto::decrypt_segment;
use crate::disguise::extract_ts_payload;

pub async fn download_playlist(
    client: &HttpClient,
    playlist: &HlsPlaylist,
    dest: &Path,
    segments_dir: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    tracker: &ProgressTracker,
) -> Result<()> {
    let total_segments = playlist.media_segments.len();
    let tmp_dir = segments_dir;
    tokio::fs::create_dir_all(tmp_dir).await?;

    let mut key_cache: Option<(url::Url, Vec<u8>)> = None;
    if let Some(seg) = playlist.media_segments.iter().find(|s| s.key_url.is_some()) {
        let key_url = seg.key_url.clone().unwrap();
        let key_bytes = client.get_bytes(key_url.as_str(), &ctx.to_options(None)).await?;
        key_cache = Some((key_url, key_bytes));
    }

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut set = tokio::task::JoinSet::new();

    for (index, segment) in playlist.media_segments.iter().cloned().enumerate() {
        let client = client.clone();
        let sem = semaphore.clone();
        let seg_path = tmp_dir.join(format!("{index:08}.seg"));
        let key = key_cache.clone();
        let ctx = ctx.clone();

        set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let result = download_one_segment(&client, &segment, &key, &seg_path, &ctx).await;
            (index, result)
        });
    }

    // Account for each segment the moment it finishes — in completion order, not
    // playlist order — so one slow early segment can't freeze the whole bar.
    while let Some(joined) = set.join_next().await {
        let (index, result) = joined.context("segment download task panicked")?;
        let segment_bytes = result.with_context(|| format!("failed to download segment {index}"))?;
        tracker.add_unit(segment_bytes);
    }

    let seg_paths: Vec<PathBuf> =
        (0..total_segments).map(|i| tmp_dir.join(format!("{i:08}.seg"))).collect();
    assemble(&seg_paths, dest).await?;
    tokio::fs::remove_dir_all(&tmp_dir).await.ok();
    Ok(())
}

async fn download_one_segment(
    client: &HttpClient,
    segment: &HlsMediaSegment,
    key: &Option<(url::Url, Vec<u8>)>,
    out_path: &Path,
    ctx: &RequestContext,
) -> Result<u64> {
    if let Ok(meta) = tokio::fs::metadata(out_path).await {
        return Ok(meta.len());
    }

    let opts = ctx.to_options(segment.byte_range);
    let raw = luedd_net::retry(luedd_net::DEFAULT_RETRY_ATTEMPTS, luedd_net::DEFAULT_RETRY_DELAY, || {
        client.get_bytes(segment.url.as_str(), &opts)
    })
    .await
    .with_context(|| format!("segment {} failed after retries", segment.url))?;

    let payload = if let (Some(seg_key_url), Some(iv)) = (&segment.key_url, &segment.iv) {
        let (cached_url, key_bytes) = key
            .as_ref()
            .filter(|(u, _)| u == seg_key_url)
            .context("encryption key not pre-fetched for this segment's key URL")?;
        let _ = cached_url;
        decrypt_segment(&raw, key_bytes, iv).context("segment decryption failed")?
    } else {
        extract_ts_payload(&raw).to_vec()
    };

    let len = payload.len() as u64;
    let tmp_path = out_path.with_extension("seg.part");
    tokio::fs::write(&tmp_path, payload).await?;
    tokio::fs::rename(&tmp_path, out_path).await?;
    Ok(len)
}

async fn assemble(seg_paths: &[PathBuf], dest: &Path) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut out = tokio::fs::File::create(dest).await?;
    for path in seg_paths {
        let bytes = tokio::fs::read(path).await?;
        out.write_all(&bytes).await?;
    }
    out.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Minimal HTTP/1.1 server. `GET /seg/N` returns 16 bytes; segment 0 is
    /// delayed so it is the last to finish even though it is first in playlist
    /// order.
    async fn spawn_segment_server(slow_first: Duration) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req.split_whitespace().nth(1).unwrap_or("/");
                    let idx: usize = path.rsplit('/').next().and_then(|s| s.parse().ok()).unwrap_or(0);
                    if idx == 0 {
                        tokio::time::sleep(slow_first).await;
                    }
                    let body = vec![b'x'; 16];
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    sock.write_all(resp.as_bytes()).await.ok();
                    sock.write_all(&body).await.ok();
                    sock.flush().await.ok();
                });
            }
        });
        format!("http://{addr}")
    }

    fn playlist(base: &str, count: usize) -> HlsPlaylist {
        HlsPlaylist {
            media_segments: (0..count)
                .map(|i| HlsMediaSegment {
                    url: url::Url::parse(&format!("{base}/seg/{i}")).unwrap(),
                    byte_range: None,
                    duration: 1.0,
                    key_url: None,
                    iv: None,
                    is_init_segment: false,
                })
                .collect(),
            is_encrypted: false,
            total_duration: 10.0,
            has_byte_range: false,
            is_key_i_frame_only: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_advances_before_a_slow_leading_segment_finishes() {
        let base = spawn_segment_server(Duration::from_millis(1500)).await;
        let pl = playlist(&base, 10);
        let dir = std::env::temp_dir().join(format!("luedd-hls-ooo-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let tracker = ProgressTracker::with_emit_interval(Some(&tx), None, 10, Duration::from_millis(1));

        let client = HttpClient::new().unwrap();
        let dl = tokio::spawn({
            let dest = dir.join("out.ts");
            let segs = dir.join("segs");
            async move { download_playlist(&client, &pl, &dest, &segs, 8, &RequestContext::default(), &tracker).await }
        });

        // Segment 0 sleeps for 1500 ms. Long before then, the other 9 finish and
        // must already be reflected in an emitted event. The old in-playlist-order
        // await produced no event at all until segment 0 returned.
        let mut best = 0u64;
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            while let Ok(ev) = rx.try_recv() {
                if let luedd_net::JobEvent::Progress { done_units, .. } = ev {
                    best = best.max(done_units);
                }
            }
            if best >= 1 {
                break;
            }
        }
        assert!(best >= 1, "progress froze behind slow segment 0 (no event within 600ms)");

        let r = dl.await.unwrap();
        r.unwrap();

        // Every fast segment eventually accounted for, count exact.
        while let Ok(ev) = rx.try_recv() {
            if let luedd_net::JobEvent::Progress { done_units, .. } = ev {
                best = best.max(done_units);
            }
        }
        assert_eq!(best, 10, "final progress must reflect all 10 completed segments");
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
