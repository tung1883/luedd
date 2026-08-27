//! Runs a single download job to completion (HTTP, HLS, or DASH). This is the
//! shared execution path used by both `tidm-cli` and the Tauri GUI (M5) so a
//! download behaves identically regardless of which frontend started it -
//! previously this logic lived duplicated inline in the CLI.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tidm_media::dash::download_representation;
use tidm_media::hls::{download_playlist, parse_master_playlist, parse_media_playlist};
use tidm_media::mux::{mux_demuxed, mux_single};
use tidm_net::{HttpClient, ProgressTx, RequestContext};

use crate::progressive;

/// What kind of downloader a URL needs. `Http` covers any plain file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DownloadKind {
    Http,
    Hls,
    Dash,
}

impl DownloadKind {
    /// Best-effort sniff from the URL's extension, matching the CLI's original
    /// heuristic (checked against the manifest body for Hls/Dash once fetched).
    pub fn guess_from_url(url: &str) -> DownloadKind {
        let path = url.split(['?', '#']).next().unwrap_or(url);
        if path.ends_with(".m3u8") {
            DownloadKind::Hls
        } else if path.ends_with(".mpd") {
            DownloadKind::Dash
        } else {
            DownloadKind::Http
        }
    }
}

/// Callers that don't know the real output container up front (a URL detected
/// by the browser extension with no explicit filename, say) default to naming
/// the destination after the *source manifest's own* last path segment - for
/// an HLS/DASH URL that means a `.m3u8`/`.mpd` (or extension-less) `dest`. If
/// that literal path is handed to `ffmpeg` as a mux target, ffmpeg picks its
/// muxer from the extension: a `.m3u8` destination makes it write a *new* HLS
/// playlist plus a pile of freshly-numbered `.ts` segments next to it instead
/// of one video file - the exact "downloaded as hundreds of files, never
/// merged" bug this guards against. Real user-chosen extensions (`.mkv`, an
/// explicit `.mp4`, ...) are left alone, and `Http` downloads are never
/// touched (an extension-less or oddly-named plain file is normal for those).
///
/// Called both where a `DownloadEntry` is first created (so the persisted
/// `dest` - used later for cleanup/display - is correct from the start) and
/// defensively again at the top of `run_hls`/`run_dash` in case some other
/// caller ever constructs an entry without going through that path.
pub fn sanitize_dest_for_kind(dest: &Path, kind: DownloadKind) -> PathBuf {
    if matches!(kind, DownloadKind::Http) {
        return dest.to_path_buf();
    }
    let ext = dest.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "m3u8" | "mpd" | "" => dest.with_extension("mp4"),
        _ => dest.to_path_buf(),
    }
}

pub async fn run_http(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    progressive::download(client, url, dest, concurrency, ctx, progress).await
}

pub async fn run_hls(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let dest = &sanitize_dest_for_kind(dest, DownloadKind::Hls);
    let text = client.get_text(url, &ctx.to_options(None)).await?;
    let lines: Vec<&str> = text.lines().collect();

    if text.contains("#EXT-X-STREAM-INF") {
        let containers = parse_master_playlist(&lines, url)?;
        let best = containers
            .iter()
            .max_by_key(|c| {
                c.attributes.get("BANDWIDTH").and_then(|b| b.parse::<u64>().ok()).unwrap_or(0)
            })
            .context("master playlist had no variants")?;

        let video_url = best.video_playlist.as_ref().context("variant missing video playlist")?;
        let video_text = client.get_text(video_url.as_str(), &ctx.to_options(None)).await?;
        let video_lines: Vec<&str> = video_text.lines().collect();
        let video_playlist = parse_media_playlist(&video_lines, video_url.as_str())?;

        match &best.audio_playlist {
            Some(audio_url) => {
                let audio_text = client.get_text(audio_url.as_str(), &ctx.to_options(None)).await?;
                let audio_lines: Vec<&str> = audio_text.lines().collect();
                let audio_playlist = parse_media_playlist(&audio_lines, audio_url.as_str())?;

                let video_tmp = dest.with_extension("video.tmp.ts");
                let audio_tmp = dest.with_extension("audio.tmp.ts");
                // Only the (usually larger, slower) video track drives progress -
                // reporting both would need combining two independent totals into
                // one number, not worth the complexity for a progress indicator.
                download_playlist(client, &video_playlist, &video_tmp, concurrency, ctx, progress).await?;
                download_playlist(client, &audio_playlist, &audio_tmp, concurrency, ctx, None).await?;
                tidm_net::report_converting(progress);
                mux_demuxed(&video_tmp, &audio_tmp, dest).await?;
                tokio::fs::remove_file(&video_tmp).await.ok();
                tokio::fs::remove_file(&audio_tmp).await.ok();
            }
            None => {
                let assembled_tmp = dest.with_extension("tmp.ts");
                download_playlist(client, &video_playlist, &assembled_tmp, concurrency, ctx, progress).await?;
                tidm_net::report_converting(progress);
                mux_single(&assembled_tmp, dest).await?;
                tokio::fs::remove_file(&assembled_tmp).await.ok();
            }
        }
    } else if text.contains("#EXTINF") {
        let playlist = parse_media_playlist(&lines, url)?;
        let assembled_tmp = dest.with_extension("tmp.ts");
        download_playlist(client, &playlist, &assembled_tmp, concurrency, ctx, progress).await?;
        tidm_net::report_converting(progress);
        mux_single(&assembled_tmp, dest).await?;
        tokio::fs::remove_file(&assembled_tmp).await.ok();
    } else {
        // A 200 response that's neither a master nor media playlist is almost
        // always an auth/bot-protection page (login wall, Cloudflare
        // challenge, error page) served with a success status instead of a
        // real 4xx - the earlier status-code check never catches it. Include
        // a snippet and what was actually sent so this is diagnosable instead
        // of a bare "not HLS" with no further clue.
        let snippet: String = text.chars().take(200).collect();
        bail!(
            "URL did not look like a master or media HLS playlist ({}). First bytes of response: {snippet:?}",
            tidm_net::describe_sent_headers(&ctx.to_options(None))
        );
    }
    Ok(())
}

pub async fn run_dash(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    let dest = &sanitize_dest_for_kind(dest, DownloadKind::Dash);
    let manifest = client.get_text(url, &ctx.to_options(None)).await?;
    let periods = tidm_media::dash::parse(&manifest, url)?;
    let pairing = periods
        .iter()
        .flatten()
        .max_by_key(|(v, a)| {
            v.as_ref().map(|r| r.bandwidth).unwrap_or(0) + a.as_ref().map(|r| r.bandwidth).unwrap_or(0)
        })
        .context("manifest had no usable video/audio representations")?;

    match pairing {
        (Some(video), Some(audio)) => {
            let video_tmp = dest.with_extension("video.tmp");
            let audio_tmp = dest.with_extension("audio.tmp");
            download_representation(client, video, &video_tmp, concurrency, ctx, progress).await?;
            download_representation(client, audio, &audio_tmp, concurrency, ctx, None).await?;
            tidm_net::report_converting(progress);
            mux_demuxed(&video_tmp, &audio_tmp, dest).await?;
            tokio::fs::remove_file(&video_tmp).await.ok();
            tokio::fs::remove_file(&audio_tmp).await.ok();
        }
        (Some(video), None) => {
            let tmp = dest.with_extension("tmp");
            download_representation(client, video, &tmp, concurrency, ctx, progress).await?;
            tidm_net::report_converting(progress);
            mux_single(&tmp, dest).await?;
            tokio::fs::remove_file(&tmp).await.ok();
        }
        (None, Some(audio)) => {
            let tmp = dest.with_extension("tmp");
            download_representation(client, audio, &tmp, concurrency, ctx, progress).await?;
            tidm_net::report_converting(progress);
            mux_single(&tmp, dest).await?;
            tokio::fs::remove_file(&tmp).await.ok();
        }
        (None, None) => bail!("manifest pairing had neither video nor audio"),
    }
    Ok(())
}

/// Dispatches to the right job runner based on `kind`.
pub async fn run(
    client: &HttpClient,
    kind: DownloadKind,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<()> {
    match kind {
        DownloadKind::Http => run_http(client, url, dest, concurrency, ctx, progress).await,
        DownloadKind::Hls => run_hls(client, url, dest, concurrency, ctx, progress).await,
        DownloadKind::Dash => run_dash(client, url, dest, concurrency, ctx, progress).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_manifest_extension_to_mp4_for_hls_and_dash() {
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/master.m3u8"), DownloadKind::Hls),
            PathBuf::from("downloads/master.mp4")
        );
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/manifest.mpd"), DownloadKind::Dash),
            PathBuf::from("downloads/manifest.mp4")
        );
    }

    #[test]
    fn rewrites_extensionless_dest_to_mp4_for_hls_and_dash() {
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/29whtyqk8y~WU4mnOWXhR"), DownloadKind::Hls),
            PathBuf::from("downloads/29whtyqk8y~WU4mnOWXhR.mp4")
        );
    }

    #[test]
    fn leaves_explicit_user_chosen_extensions_alone() {
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/movie.mkv"), DownloadKind::Hls),
            PathBuf::from("downloads/movie.mkv")
        );
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/movie.mp4"), DownloadKind::Dash),
            PathBuf::from("downloads/movie.mp4")
        );
    }

    #[test]
    fn never_touches_http_downloads() {
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/29whtyqk8y~WU4mnOWXhR"), DownloadKind::Http),
            PathBuf::from("downloads/29whtyqk8y~WU4mnOWXhR")
        );
        assert_eq!(
            sanitize_dest_for_kind(Path::new("downloads/data.m3u8"), DownloadKind::Http),
            PathBuf::from("downloads/data.m3u8")
        );
    }
}
