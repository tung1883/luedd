//! Shells out to `ffmpeg` to remux downloaded segments into a final container,
//! equivalent to XDM's `FFmpegMediaProcessor` (`MergeAudioVideStream` /
//! `MergeHLSAudioVideStream`). Never re-encodes (`-c copy`) since the underlying
//! stream is already valid - only assembly/remuxing is needed.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use tidm_net::ProgressTx;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// What to report ffmpeg's mux progress against - the same `ProgressTx`
/// already used for download progress (`JobEvent::Progress` may now follow a
/// `JobEvent::Converting`, see `tidm_net::JobEvent`), paired with the total
/// duration in milliseconds the caller already knows from the parsed
/// HLS/DASH playlist (`HlsPlaylist::total_duration` / `Representation::duration_ms`).
/// `None` (either the whole tuple, or just a `None` duration) skips
/// `-progress` entirely - the caller doesn't always know a duration up front.
pub type MuxProgress<'a> = (&'a ProgressTx, u64);

/// Muxes a single assembled TS/media file into `output` (mp4 by default), falling
/// back to `.mkv` if the mp4 mux fails (mirrors XDM's fallback behavior). Returns
/// whichever path actually got written - `output` itself, or its `.mkv` fallback -
/// since the caller (moving the result out of a per-download cache dir into its
/// real destination) needs to know which one actually happened.
pub async fn mux_single(input: &Path, output: &Path, progress: Option<MuxProgress<'_>>) -> Result<PathBuf> {
    if run_ffmpeg(
        &[
            "-y",
            "-fflags",
            "+genpts",
            "-i",
            &input.to_string_lossy(),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-movflags",
            "+faststart",
            &output.to_string_lossy(),
        ],
        progress,
    )
    .await
    .is_ok()
    {
        return Ok(output.to_path_buf());
    }

    let mkv_output = output.with_extension("mkv");
    run_ffmpeg(
        &[
            "-y",
            "-fflags",
            "+genpts",
            "-i",
            &input.to_string_lossy(),
            "-c",
            "copy",
            &mkv_output.to_string_lossy(),
        ],
        progress,
    )
    .await
    .with_context(|| format!("ffmpeg mux failed for both mp4 and mkv fallback: {}", input.display()))?;
    Ok(mkv_output)
}

/// Muxes separately-downloaded video and audio streams into one output container,
/// equivalent to `MergeAudioVideStream`. Returns whichever path actually got
/// written, same reasoning as `mux_single`.
pub async fn mux_demuxed(video: &Path, audio: &Path, output: &Path, progress: Option<MuxProgress<'_>>) -> Result<PathBuf> {
    if run_ffmpeg(
        &[
            "-y",
            "-i",
            &video.to_string_lossy(),
            "-i",
            &audio.to_string_lossy(),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            &output.to_string_lossy(),
        ],
        progress,
    )
    .await
    .is_ok()
    {
        return Ok(output.to_path_buf());
    }

    let mkv_output = output.with_extension("mkv");
    run_ffmpeg(
        &[
            "-y",
            "-i",
            &video.to_string_lossy(),
            "-i",
            &audio.to_string_lossy(),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c",
            "copy",
            &mkv_output.to_string_lossy(),
        ],
        progress,
    )
    .await
    .with_context(|| "ffmpeg demuxed mux failed for both mp4 and mkv fallback")?;
    Ok(mkv_output)
}

/// Runs `ffmpeg` fully asynchronously via `tokio::process::Command` (piped
/// stdout/stderr, `.wait()`ed on rather than the old `spawn_blocking(Command::
/// output())`), so it never blocks a tokio worker thread for the whole mux
/// duration - important since Tauri's `list_downloads` command (polled every
/// 2s by the frontend) shares that same runtime, and a blocked worker thread
/// would freeze the UI on a stale status until ffmpeg finished. When
/// `progress` is given, also passes `-progress pipe:1 -nostats` and streams
/// parsed `out_time_us=` lines through it as it runs, rather than only
/// reporting once at the very end.
async fn run_ffmpeg(args: &[&str], progress: Option<MuxProgress<'_>>) -> Result<()> {
    let mut owned_args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    if progress.is_some() {
        owned_args.push("-progress".to_string());
        owned_args.push("pipe:1".to_string());
        owned_args.push("-nostats".to_string());
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.args(&owned_args).stdout(Stdio::piped()).stderr(Stdio::piped());

    #[cfg(windows)]
    {
        // `tokio::process::Command` exposes `creation_flags` as an inherent
        // method directly (unlike `std::process::Command`, which needs the
        // `CommandExt` trait imported) - no extra import needed here.
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let mut child = cmd.spawn().context("failed to spawn ffmpeg - is it installed and on PATH?")?;

    let progress_task = match (child.stdout.take(), progress) {
        (Some(stdout), Some((tx, total_ms))) => {
            let tx = tx.clone();
            Some(tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // `out_time_us=` is unambiguously microseconds (unlike
                    // `out_time_ms=`, which some ffmpeg builds populate with
                    // microseconds too despite the name) - convert once here
                    // rather than trusting either field's naming.
                    if let Some(us) = line.strip_prefix("out_time_us=").and_then(|v| v.trim().parse::<i64>().ok()) {
                        if us >= 0 {
                            tidm_net::report_progress(Some(&tx), (us as u64) / 1000, total_ms);
                        }
                    }
                }
            }))
        }
        _ => None,
    };

    let mut stderr_output = Vec::new();
    if let Some(mut stderr) = child.stderr.take() {
        stderr.read_to_end(&mut stderr_output).await.ok();
    }

    let status = child.wait().await.context("waiting for ffmpeg to exit")?;
    if let Some(task) = progress_task {
        task.await.ok();
    }

    if !status.success() {
        bail!("ffmpeg exited with {}: {}", status, String::from_utf8_lossy(&stderr_output));
    }
    Ok(())
}
