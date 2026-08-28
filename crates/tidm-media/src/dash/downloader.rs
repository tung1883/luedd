use std::path::Path;

use anyhow::{Context, Result};
use tidm_net::{HttpClient, ProgressTx, RequestContext};

use super::model::Representation;

pub async fn download_representation(
    client: &HttpClient,
    representation: &Representation,
    dest: &Path,
    segments_dir: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let total_segments = representation.segments.len() as u64;
    let tmp_dir = segments_dir;
    tokio::fs::create_dir_all(tmp_dir).await?;

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut tasks = Vec::with_capacity(representation.segments.len());

    for (index, url) in representation.segments.iter().cloned().enumerate() {
        let client = client.clone();
        let sem = semaphore.clone();
        let seg_path = tmp_dir.join(format!("{index:08}.seg"));
        let ctx = ctx.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");

            if let Ok(meta) = tokio::fs::metadata(&seg_path).await {
                return Ok::<_, anyhow::Error>(meta.len());
            }

            let opts = ctx.to_options(None);
            let bytes = tidm_net::retry(tidm_net::DEFAULT_RETRY_ATTEMPTS, tidm_net::DEFAULT_RETRY_DELAY, || {
                client.get_bytes(url.as_str(), &opts)
            })
            .await
            .with_context(|| format!("segment {url} failed after retries"))?;
            let len = bytes.len() as u64;
            let tmp_path = seg_path.with_extension("seg.part");
            tokio::fs::write(&tmp_path, bytes).await?;
            tokio::fs::rename(&tmp_path, &seg_path).await?;
            Ok::<_, anyhow::Error>(len)
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

    use tokio::io::AsyncWriteExt;
    let mut out = tokio::fs::File::create(dest).await?;
    for path in &seg_paths {
        let bytes = tokio::fs::read(path).await?;
        out.write_all(&bytes).await?;
    }
    out.flush().await?;

    tokio::fs::remove_dir_all(&tmp_dir).await.ok();
    Ok(())
}
