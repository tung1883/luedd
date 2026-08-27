use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tidm_net::{HttpClient, ProgressTx, RequestContext};

use super::model::{HlsMediaSegment, HlsPlaylist};
use crate::crypto::decrypt_segment;
use crate::disguise::extract_ts_payload;

/// Downloads every segment in `playlist` concurrently (bounded by `concurrency`),
/// strips any disguised-segment header per `m3u8-guide.txt`, decrypts if the
/// playlist is AES-128 encrypted, and appends the segments in playlist order into
/// a single assembled file at `dest`. Equivalent in effect to
/// `MultiSourceHLSDownloader`'s per-chunk workers + `Assemble()`, but as bounded
/// async tasks instead of thread-per-connection. `ctx` carries any
/// Referer/Origin/Cookie/User-Agent captured when the URL was detected, replayed
/// on the manifest's key fetch and every segment fetch. `segments_dir` is the
/// caller-owned scratch folder for this playlist's per-segment files - callers
/// downloading multiple playlists for one logical download (demuxed video +
/// audio) pass distinct `segments_dir`s so they don't collide, but both live
/// inside that download's single shared cache folder.
pub async fn download_playlist(
    client: &HttpClient,
    playlist: &HlsPlaylist,
    dest: &Path,
    segments_dir: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let total_segments = playlist.media_segments.len() as u64;
    let tmp_dir = segments_dir;
    tokio::fs::create_dir_all(tmp_dir).await?;

    let mut key_cache: Option<(url::Url, Vec<u8>)> = None;
    // Fetch the encryption key once up front (all segments in a media playlist
    // typically share one #EXT-X-KEY URI; a change mid-playlist would need
    // per-segment key fetches, kept simple here since HLS VOD streams rarely rotate keys).
    if let Some(seg) = playlist.media_segments.iter().find(|s| s.key_url.is_some()) {
        let key_url = seg.key_url.clone().unwrap();
        let key_bytes = client.get_bytes(key_url.as_str(), &ctx.to_options(None)).await?;
        key_cache = Some((key_url, key_bytes));
    }

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut tasks = Vec::with_capacity(playlist.media_segments.len());

    for (index, segment) in playlist.media_segments.iter().cloned().enumerate() {
        let client = client.clone();
        let sem = semaphore.clone();
        let seg_path = tmp_dir.join(format!("{index:08}.seg"));
        let key = key_cache.clone();
        let ctx = ctx.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            download_one_segment(&client, &segment, &key, &seg_path, &ctx).await
        }));
    }

    let mut seg_paths = Vec::with_capacity(tasks.len());
    let mut bytes_so_far: u64 = 0;
    for (index, task) in tasks.into_iter().enumerate() {
        let segment_bytes = task
            .await
            .context("segment download task panicked")?
            .with_context(|| format!("failed to download segment {index}"))?;
        seg_paths.push(tmp_dir.join(format!("{index:08}.seg")));
        bytes_so_far += segment_bytes;

        // Report actual bytes rather than a bare segment count: there's no
        // single upfront Content-Length for an HLS stream (it's N separate
        // segment requests), so the "total" here is an estimate extrapolated
        // from the average segment size seen so far - close enough for a
        // percentage/speed indicator, and self-corrects as more segments
        // complete. Segments in a VOD stream are normally close to uniform
        // duration/size, so this converges quickly.
        let done = index as u64 + 1;
        let estimated_total = (bytes_so_far / done) * total_segments;
        tidm_net::report_progress(progress, bytes_so_far, estimated_total);
    }

    assemble(&seg_paths, dest).await?;
    tokio::fs::remove_dir_all(&tmp_dir).await.ok();
    Ok(())
}

/// Returns the decrypted/de-disguised payload's byte size, so the caller can
/// track cumulative bytes downloaded for progress/speed reporting.
async fn download_one_segment(
    client: &HttpClient,
    segment: &HlsMediaSegment,
    key: &Option<(url::Url, Vec<u8>)>,
    out_path: &Path,
    ctx: &RequestContext,
) -> Result<u64> {
    // A retry re-runs this whole playlist from segment 0 with the same `dest`
    // (and therefore the same `tmp_dir`), so a prior attempt's already-complete
    // segments are still sitting on disk - skip re-fetching them. Safe because
    // `out_path` is only ever created by the atomic rename below, so its mere
    // existence means a full, uncorrupted write already happened.
    if let Ok(meta) = tokio::fs::metadata(out_path).await {
        return Ok(meta.len());
    }

    let opts = ctx.to_options(segment.byte_range);
    // Segment fetches over real CDNs fail transiently often enough in practice
    // (observed: truncated bodies, mid-transfer timeouts) that one-shot fetches
    // are unreliable for a 100+ segment playlist - a single flaky segment
    // otherwise aborts the entire download.
    let raw = tidm_net::retry(tidm_net::DEFAULT_RETRY_ATTEMPTS, tidm_net::DEFAULT_RETRY_DELAY, || {
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
        // Not encrypted: still check for a disguised-TS wrapper regardless of
        // the segment's claimed extension, per m3u8-guide.txt.
        extract_ts_payload(&raw).to_vec()
    };

    let len = payload.len() as u64;
    // Write to a sibling temp path and rename into place, rather than writing
    // `out_path` directly - a rename is effectively atomic, so a segment that
    // gets interrupted mid-write (process killed, crash) never leaves a
    // truncated file under the final name for the skip check above to
    // mistake for complete.
    let tmp_path = out_path.with_extension("seg.part");
    tokio::fs::write(&tmp_path, payload).await?;
    tokio::fs::rename(&tmp_path, out_path).await?;
    Ok(len)
}

/// Concatenates segment files (already decrypted/de-disguised, in playlist order)
/// into one assembled file.
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
