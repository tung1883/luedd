
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use luedd_media::dash::download_representation;
use luedd_media::hls::{download_playlist, parse_master_playlist, parse_media_playlist};
use luedd_media::mux::{mux_demuxed, mux_single};
use luedd_media::mux::MuxProgress;
use luedd_media::quality::{dash_variant_key, hls_variant_key};
use luedd_net::{HttpClient, ProgressTx, RequestContext};

use crate::naming::cache_dir_for;
use crate::progressive;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DownloadKind {
    Http,
    Hls,
    Dash,
}

impl DownloadKind {
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

async fn finish_mux(produced: PathBuf, dest: &Path) -> Result<PathBuf> {
    let final_dest = if produced.extension() == dest.extension() { dest.to_path_buf() } else { dest.with_extension("mkv") };
    tokio::fs::rename(&produced, &final_dest).await.context("moving muxed output into place")?;
    Ok(final_dest)
}

pub async fn run_http(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
) -> Result<PathBuf> {
    progressive::download(client, url, dest, concurrency, ctx, progress).await?;
    Ok(dest.to_path_buf())
}

pub async fn run_hls(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
    quality: Option<&str>,
) -> Result<PathBuf> {
    let dest = &sanitize_dest_for_kind(dest, DownloadKind::Hls);
    let cache_dir = cache_dir_for(dest);
    tokio::fs::create_dir_all(&cache_dir).await?;
    let text = client.get_text(url, &ctx.to_options(None)).await?;
    let lines: Vec<&str> = text.lines().collect();

    let final_dest = if text.contains("#EXT-X-STREAM-INF") {
        let containers = parse_master_playlist(&lines, url)?;
        let best = quality
            .and_then(|q| containers.iter().find(|c| hls_variant_key(c) == q))
            .or_else(|| {
                containers
                    .iter()
                    .max_by_key(|c| c.attributes.get("BANDWIDTH").and_then(|b| b.parse::<u64>().ok()).unwrap_or(0))
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

                let video_tmp = cache_dir.join("video.ts");
                let audio_tmp = cache_dir.join("audio.ts");
                download_playlist(client, &video_playlist, &video_tmp, &cache_dir.join("video-segments"), concurrency, ctx, progress).await?;
                download_playlist(client, &audio_playlist, &audio_tmp, &cache_dir.join("audio-segments"), concurrency, ctx, None).await?;
                luedd_net::report_converting(progress);
                let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, (video_playlist.total_duration * 1000.0) as u64));
                let produced = mux_demuxed(&video_tmp, &audio_tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
                finish_mux(produced, dest).await?
            }
            None => {
                let assembled_tmp = cache_dir.join("assembled.ts");
                download_playlist(client, &video_playlist, &assembled_tmp, &cache_dir.join("segments"), concurrency, ctx, progress).await?;
                luedd_net::report_converting(progress);
                let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, (video_playlist.total_duration * 1000.0) as u64));
                let produced = mux_single(&assembled_tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
                finish_mux(produced, dest).await?
            }
        }
    } else if text.contains("#EXTINF") {
        let playlist = parse_media_playlist(&lines, url)?;
        let assembled_tmp = cache_dir.join("assembled.ts");
        download_playlist(client, &playlist, &assembled_tmp, &cache_dir.join("segments"), concurrency, ctx, progress).await?;
        luedd_net::report_converting(progress);
        let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, (playlist.total_duration * 1000.0) as u64));
        let produced = mux_single(&assembled_tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
        finish_mux(produced, dest).await?
    } else {
        let snippet: String = text.chars().take(200).collect();
        bail!(
            "URL did not look like a master or media HLS playlist ({}). First bytes of response: {snippet:?}",
            luedd_net::describe_sent_headers(&ctx.to_options(None))
        );
    };
    tokio::fs::remove_dir_all(&cache_dir).await.ok();
    Ok(final_dest)
}

pub async fn run_dash(
    client: &HttpClient,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
    quality: Option<&str>,
) -> Result<PathBuf> {
    let dest = &sanitize_dest_for_kind(dest, DownloadKind::Dash);
    let cache_dir = cache_dir_for(dest);
    tokio::fs::create_dir_all(&cache_dir).await?;
    let manifest = client.get_text(url, &ctx.to_options(None)).await?;
    let periods = luedd_media::dash::parse(&manifest, url)?;
    let pairing = quality
        .and_then(|q| periods.iter().flatten().find(|(v, a)| dash_variant_key(v.as_ref(), a.as_ref()) == q))
        .or_else(|| {
            periods.iter().flatten().max_by_key(|(v, a)| {
                v.as_ref().map(|r| r.bandwidth).unwrap_or(0) + a.as_ref().map(|r| r.bandwidth).unwrap_or(0)
            })
        })
        .context("manifest had no usable video/audio representations")?;

    let final_dest = match pairing {
        (Some(video), Some(audio)) => {
            let video_tmp = cache_dir.join("video.m4s");
            let audio_tmp = cache_dir.join("audio.m4s");
            download_representation(client, video, &video_tmp, &cache_dir.join("video-segments"), concurrency, ctx, progress).await?;
            download_representation(client, audio, &audio_tmp, &cache_dir.join("audio-segments"), concurrency, ctx, None).await?;
            luedd_net::report_converting(progress);
            let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, video.duration_ms.max(0) as u64));
            let produced = mux_demuxed(&video_tmp, &audio_tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
            finish_mux(produced, dest).await?
        }
        (Some(video), None) => {
            let tmp = cache_dir.join("video.m4s");
            download_representation(client, video, &tmp, &cache_dir.join("segments"), concurrency, ctx, progress).await?;
            luedd_net::report_converting(progress);
            let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, video.duration_ms.max(0) as u64));
            let produced = mux_single(&tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
            finish_mux(produced, dest).await?
        }
        (None, Some(audio)) => {
            let tmp = cache_dir.join("audio.m4s");
            download_representation(client, audio, &tmp, &cache_dir.join("segments"), concurrency, ctx, progress).await?;
            luedd_net::report_converting(progress);
            let mux_progress: Option<MuxProgress> = progress.map(|tx| (tx, audio.duration_ms.max(0) as u64));
            let produced = mux_single(&tmp, &cache_dir.join("output.mp4"), mux_progress).await?;
            finish_mux(produced, dest).await?
        }
        (None, None) => bail!("manifest pairing had neither video nor audio"),
    };
    tokio::fs::remove_dir_all(&cache_dir).await.ok();
    Ok(final_dest)
}

pub async fn run(
    client: &HttpClient,
    kind: DownloadKind,
    url: &str,
    dest: &Path,
    concurrency: usize,
    ctx: &RequestContext,
    progress: Option<&ProgressTx>,
    quality: Option<&str>,
) -> Result<PathBuf> {
    match kind {
        DownloadKind::Http => run_http(client, url, dest, concurrency, ctx, progress).await,
        DownloadKind::Hls => run_hls(client, url, dest, concurrency, ctx, progress, quality).await,
        DownloadKind::Dash => run_dash(client, url, dest, concurrency, ctx, progress, quality).await,
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
