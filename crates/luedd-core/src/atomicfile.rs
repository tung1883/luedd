//! Crash-safe whole-file writes for the small JSON stores.
//!
//! `tokio::fs::write` truncates then streams the new bytes, so a process kill
//! (or power loss) mid-write leaves a torn file — which then fails to parse on
//! the next start. Writing to a sibling temp file and renaming it over the
//! target makes the swap atomic: a reader sees either the whole old file or the
//! whole new one, never a half.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Write `bytes` to `path` atomically (temp file in the same directory, then
/// rename). Creates the parent directory if needed.
pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| format!("creating {}", parent.display()))?;
    }
    // Unique per call: two concurrent writers to the same target must not share
    // a temp path, or their bytes interleave and the rename publishes garbage.
    let tmp = path.with_extension(format!(
        "{}tmp-{}-{}",
        path.extension().and_then(|e| e.to_str()).map(|e| format!("{e}.")).unwrap_or_default(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    tokio::fs::write(&tmp, bytes).await.with_context(|| format!("writing {}", tmp.display()))?;
    match tokio::fs::rename(&tmp, path).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            Err(e).with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))
        }
    }
}
