use std::path::Path;

use anyhow::{Context, Result};
use luedd_net::{HttpClient, ProgressTracker, RequestContext};

use super::model::Representation;

pub async fn download_representation(
    client: &HttpClient,
    representation: &Representation,
    dest: &Path,
    segments_dir: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    tracker: &ProgressTracker,
) -> Result<()> {
    let total_segments = representation.segments.len();
    let tmp_dir = segments_dir;
    tokio::fs::create_dir_all(tmp_dir).await?;

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut set = tokio::task::JoinSet::new();

    for (index, url) in representation.segments.iter().cloned().enumerate() {
        let client = client.clone();
        let sem = semaphore.clone();
        let seg_path = tmp_dir.join(format!("{index:08}.seg"));
        let ctx = ctx.clone();

        set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");

            let result: Result<u64> = async {
                if let Ok(meta) = tokio::fs::metadata(&seg_path).await {
                    return Ok(meta.len());
                }

                let opts = ctx.to_options(None);
                let bytes = luedd_net::retry(luedd_net::DEFAULT_RETRY_ATTEMPTS, luedd_net::DEFAULT_RETRY_DELAY, || {
                    client.get_bytes(url.as_str(), &opts)
                })
                .await
                .with_context(|| format!("segment {url} failed after retries"))?;
                let len = bytes.len() as u64;
                let tmp_path = seg_path.with_extension("seg.part");
                tokio::fs::write(&tmp_path, bytes).await?;
                tokio::fs::rename(&tmp_path, &seg_path).await?;
                Ok(len)
            }
            .await;
            (index, result)
        });
    }

    // Completion-order accounting: progress advances as any segment lands.
    while let Some(joined) = set.join_next().await {
        let (index, result) = joined.context("segment download task panicked")?;
        let segment_bytes = result.with_context(|| format!("failed to download segment {index}"))?;
        tracker.add_unit(segment_bytes);
    }

    let seg_paths: Vec<std::path::PathBuf> =
        (0..total_segments).map(|i| tmp_dir.join(format!("{i:08}.seg"))).collect();

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
