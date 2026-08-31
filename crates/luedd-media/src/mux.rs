
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use luedd_net::ProgressTx;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

pub type MuxProgress<'a> = (&'a ProgressTx, u64);

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

async fn run_ffmpeg(args: &[&str], progress: Option<MuxProgress<'_>>) -> Result<()> {
    let mut owned_args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    if progress.is_some() {
        owned_args.push("-progress".to_string());
        owned_args.push("pipe:1".to_string());
        owned_args.push("-nostats".to_string());
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.args(&owned_args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());

    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW: no console window flash when spawned from the GUI app.
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let mut child = cmd.spawn().context("failed to spawn ffmpeg - is it installed and on PATH?")?;

    let progress_task = match (child.stdout.take(), progress) {
        (Some(stdout), Some((tx, total_ms))) => {
            let tx = tx.clone();
            Some(tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(us) = line.strip_prefix("out_time_us=").and_then(|v| v.trim().parse::<i64>().ok()) {
                        if us >= 0 {
                            luedd_net::report_progress(Some(&tx), (us as u64) / 1000, total_ms);
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
