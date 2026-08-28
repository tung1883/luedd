use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tidm_net::{HttpClient, ProgressTx, RequestContext};

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
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let total_segments = playlist.media_segments.len() as u64;
    let tmp_dir = segments_dir;
    tokio::fs::create_dir_all(tmp_dir).await?;

    let mut key_cache: Option<(url::Url, Vec<u8>)> = None;
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

        let done = index as u64 + 1;
        let estimated_total = (bytes_so_far / done) * total_segments;
        tidm_net::report_progress(progress, bytes_so_far, estimated_total);
    }

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
