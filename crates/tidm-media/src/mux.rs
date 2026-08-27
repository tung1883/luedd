//! Shells out to `ffmpeg` to remux downloaded segments into a final container,
//! equivalent to XDM's `FFmpegMediaProcessor` (`MergeAudioVideStream` /
//! `MergeHLSAudioVideStream`). Never re-encodes (`-c copy`) since the underlying
//! stream is already valid - only assembly/remuxing is needed.

use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};

/// Muxes a single assembled TS/media file into `output` (mp4 by default), falling
/// back to `.mkv` if the mp4 mux fails (mirrors XDM's fallback behavior).
pub async fn mux_single(input: &Path, output: &Path) -> Result<()> {
    if run_ffmpeg(&[
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
    ])
    .await
    .is_ok()
    {
        return Ok(());
    }

    let mkv_output = output.with_extension("mkv");
    run_ffmpeg(&[
        "-y",
        "-fflags",
        "+genpts",
        "-i",
        &input.to_string_lossy(),
        "-c",
        "copy",
        &mkv_output.to_string_lossy(),
    ])
    .await
    .with_context(|| format!("ffmpeg mux failed for both mp4 and mkv fallback: {}", input.display()))
}

/// Muxes separately-downloaded video and audio streams into one output container,
/// equivalent to `MergeAudioVideStream`.
pub async fn mux_demuxed(video: &Path, audio: &Path, output: &Path) -> Result<()> {
    if run_ffmpeg(&[
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
    ])
    .await
    .is_ok()
    {
        return Ok(());
    }

    let mkv_output = output.with_extension("mkv");
    run_ffmpeg(&[
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
    ])
    .await
    .with_context(|| "ffmpeg demuxed mux failed for both mp4 and mkv fallback")
}

/// Runs `ffmpeg` on a blocking thread pool thread rather than the calling
/// tokio worker thread. `Command::output()` blocks synchronously for the
/// entire process lifetime; called directly from an async fn it would starve
/// that worker thread (and, with it, whatever else needs to run on the same
/// runtime - e.g. Tauri's `list_downloads` command, polled every 2s by the
/// frontend) for as long as the mux takes, freezing the UI on a stale status
/// (like "Downloading" instead of "Converting") until ffmpeg finishes.
async fn run_ffmpeg(args: &[&str]) -> Result<()> {
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new("ffmpeg");
        cmd.args(&args);

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x08000000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        let output = cmd.output().context("failed to spawn ffmpeg - is it installed and on PATH?")?;
        if !output.status.success() {
            bail!(
                "ffmpeg exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    })
    .await
    .context("ffmpeg task panicked")?
}
