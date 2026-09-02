//! yt-dlp backend.
//!
//! `yt-dlp -J` resolves a page URL to a list of media formats for the quality
//! menu ([`probe_qualities`]); [`run`] then lets **yt-dlp itself** download and
//! mux the chosen format. (An earlier design delegated the byte transfer to the
//! built-in http/hls/dash backends like XDM does, but YouTube now rejects
//! direct CDN fetches that don't come from yt-dlp's own downloader.)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use luedd_media::quality::QualityOption;
use luedd_net::{HttpClient, JobEvent, ProgressTx};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex, Semaphore};

use super::{BackendConfig, Confidence, DownloadBackend, DownloadReq, Outcome, Sniff};

/// Hosts routed to yt-dlp. Bare media URLs (`.mp4`, `.m3u8`) stay on the fast
/// built-in path.
const YTDLP_HOSTS: &[&str] = &[
    "youtube.com",
    "youtu.be",
    "twitch.tv",
    "vimeo.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "reddit.com",
    "dailymotion.com",
    "bilibili.com",
    "soundcloud.com",
    "facebook.com",
    "instagram.com",
];

/// `--force-ipv4` is the fix for the `RemoteDisconnected` / connection-reset
/// errors YouTube's InnerTube API throws (an IPv6 routing problem); the rest
/// just retries harder with a real socket timeout. Client selection is left to
/// yt-dlp's own default rotation.
const RESILIENCE_ARGS: &[&str] = &[
    "--force-ipv4",
    "--extractor-retries",
    "8",
    "--retries",
    "10",
    "--fragment-retries",
    "10",
    "--retry-sleep",
    "4",
    "--socket-timeout",
    "30",
    "--sleep-requests",
    "1",
    // YouTube deliberately throttles non-browser downloads to a crawl; below
    // this rate yt-dlp re-fetches a fresh media URL instead of dripping along.
    "--throttled-rate",
    "100K",
];

/// yt-dlp errors worth another whole attempt (network resets / rate limits),
/// as opposed to "video unavailable" / "sign in" which won't fix themselves.
fn is_transient(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    ["remotedisconnected", "connection aborted", "connection reset", "timed out", "temporary failure", "unable to download api page", "http error 5"]
        .iter()
        .any(|p| e.contains(p))
}

const EXTRACT_TIMEOUT: Duration = Duration::from_secs(210);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const MAX_JSON_BYTES: usize = 48 * 1024 * 1024;

pub struct YtdlpBackend {
    _client: HttpClient,
    /// yt-dlp is heavy; never fork more than a couple at once.
    slots: Semaphore,
    info_cache: Mutex<HashMap<String, Arc<Value>>>,
}

impl YtdlpBackend {
    pub fn new(client: HttpClient) -> Self {
        Self {
            _client: client,
            slots: Semaphore::new(2),
            info_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn extract(&self, url: &str, cfg: &BackendConfig, cookie: Option<&str>) -> Result<Arc<Value>> {
        if let Some(hit) = self.info_cache.lock().await.get(url).cloned() {
            return Ok(hit);
        }
        let _permit = self.slots.acquire().await.expect("semaphore closed");

        let bin = ytdlp_bin(cfg);
        let mut args: Vec<String> = vec![
            "--no-warnings".into(),
            "-q".into(),
            "-i".into(),
            "--no-playlist".into(),
            "-J".into(),
        ];
        args.extend(RESILIENCE_ARGS.iter().map(|s| s.to_string()));
        if let Some(browser) = &cfg.cookies_from_browser {
            args.push("--cookies-from-browser".into());
            args.push(browser.clone());
        }
        let cookie_file = cookie.and_then(|c| write_cookie_file(c, url));
        if let Some(cf) = &cookie_file {
            args.push("--cookies".into());
            args.push(cf.0.to_string_lossy().into_owned());
        }
        args.push(url.to_string());

        let mut attempt = 0u32;
        let json = loop {
            match spawn_json(&bin, &args, EXTRACT_TIMEOUT).await {
                Ok(v) => break v,
                Err(e) if attempt < 2 && is_transient(&e.to_string()) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(5 * attempt as u64)).await;
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("yt-dlp extraction failed for {url} (is yt-dlp installed / on PATH?)")
                    })
                }
            }
        };
        let info = Arc::new(json);
        self.info_cache.lock().await.insert(url.to_string(), info.clone());
        Ok(info)
    }
}

#[async_trait]
impl DownloadBackend for YtdlpBackend {
    fn id(&self) -> &'static str {
        "ytdlp"
    }

    fn can_handle(&self, url: &str, _sniff: Option<&Sniff>) -> Confidence {
        match host_of(url) {
            Some(host) if YTDLP_HOSTS.iter().any(|h| host == *h || host.ends_with(&format!(".{h}"))) => {
                Confidence::Strong
            }
            _ => Confidence::No,
        }
    }

    fn page_hosts(&self) -> &'static [&'static str] {
        YTDLP_HOSTS
    }

    async fn thumbnail(&self, req: &DownloadReq) -> Result<Option<(String, bool)>> {
        let info = self.extract(&req.url, &req.config, req.ctx.cookie.as_deref()).await?;
        if let Some(t) = info.get("thumbnail").and_then(Value::as_str) {
            return Ok(Some((t.to_string(), false)));
        }
        let best = info
            .get("thumbnails")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|t| {
                let url = t.get("url").and_then(Value::as_str)?;
                let area = (t.get("width").and_then(Value::as_u64).unwrap_or(0))
                    * (t.get("height").and_then(Value::as_u64).unwrap_or(1));
                Some((area, url.to_string()))
            })
            .max_by_key(|(area, _)| *area)
            .map(|(_, url)| (url, false));
        Ok(best)
    }

    async fn probe_qualities(&self, req: &DownloadReq) -> Result<Vec<QualityOption>> {
        let info = self.extract(&req.url, &req.config, req.ctx.cookie.as_deref()).await?;
        let formats = info.get("formats").and_then(Value::as_array).cloned().unwrap_or_default();

        let mut out = Vec::new();

        // Progressive (audio+video in one file) - one click, no mux.
        for f in &formats {
            if has_video(f) && has_audio(f) && f.get("url").and_then(Value::as_str).is_some() {
                out.push(quality_option(f, format_id(f)));
            }
        }

        // Video-only x best-audio, one entry per distinct resolution (keep the
        // best-bitrate format at each height).
        let best_audio = best_audio_only(&formats);
        let audio_id = best_audio.map(format_id).unwrap_or("");
        let mut by_height: std::collections::BTreeMap<u64, &Value> = std::collections::BTreeMap::new();
        for f in &formats {
            if !has_video(f) || has_audio(f) || f.get("url").and_then(Value::as_str).is_none() {
                continue;
            }
            let h = resolution_of(f).map(|(_, h)| h as u64).unwrap_or(0);
            by_height
                .entry(h)
                .and_modify(|cur| {
                    if bandwidth_of(f) > bandwidth_of(cur) {
                        *cur = f;
                    }
                })
                .or_insert(f);
        }
        for (_, f) in by_height.iter().rev() {
            let key = if audio_id.is_empty() {
                format_id(f).to_string()
            } else {
                format!("{}+{}", format_id(f), audio_id)
            };
            out.push(quality_option(f, key));
        }

        // Audio only.
        if let Some(a) = best_audio {
            let abr = a.get("abr").and_then(Value::as_f64).map(|v| format!("{:.0}k ", v)).unwrap_or_default();
            let codec = a.get("acodec").and_then(Value::as_str).unwrap_or("audio");
            out.push(QualityOption {
                label: format!("Audio only ({abr}{codec})"),
                bandwidth: bandwidth_of(a),
                resolution: None,
                variant_key: format_id(a).to_string(),
            });
        }

        out.dedup_by(|a, b| a.variant_key == b.variant_key);
        Ok(out)
    }

    async fn describe(&self, req: &DownloadReq) -> super::EntryMeta {
        self.cached_meta(&req.url).await
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let bin = ytdlp_bin(&req.config);
        let stem = final_stem(req);
        let out_tmpl = req.dest_dir.join(format!("{stem}.%(ext)s"));

        let selector = req.quality.clone().unwrap_or_else(|| "bv*+ba/b".to_string());
        let mut args: Vec<String> = vec![
            "--no-warnings".into(),
            "--newline".into(),
            "--progress".into(),
            "-i".into(),
            "--no-playlist".into(),
            "--no-part".into(),
            "--force-overwrites".into(),
            "-f".into(),
            selector,
            "--merge-output-format".into(),
            "mp4".into(),
            "-o".into(),
            out_tmpl.to_string_lossy().into_owned(),
        ];
        args.extend(RESILIENCE_ARGS.iter().map(|s| s.to_string()));
        if let Some(browser) = &req.config.cookies_from_browser {
            args.push("--cookies-from-browser".into());
            args.push(browser.clone());
        }
        let cookie_file = req.ctx.cookie.as_deref().and_then(|c| write_cookie_file(c, &req.url));
        if let Some(cf) = &cookie_file {
            args.push("--cookies".into());
            args.push(cf.0.to_string_lossy().into_owned());
        }
        if let Some(referer) = req.ctx.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("referer")) {
            args.push("--referer".into());
            args.push(referer.1.clone());
        }
        args.push("--print".into());
        args.push("after_move:filepath".into());
        args.push(req.url.clone());

        // Index of the `--cookies <file>` value in `args`, if we added one.
        let cookie_arg_idx = cookie_file.as_ref().map(|_| {
            args.iter().position(|a| a == "--cookies").map(|i| i + 1).unwrap_or(usize::MAX)
        });

        let _permit = self.slots.acquire().await.expect("semaphore closed");

        // Retry transient failures ("RemoteDisconnected", HTTP 413/5xx on the
        // InnerTube API - often caused by an oversized cookie jar). On the last
        // attempt, drop the cookie entirely: public videos need none, and a bad
        // cookie is a common cause of these very errors.
        let mut attempt = 0u32;
        let path = loop {
            match spawn_ytdlp_download(&bin, &args, &req.dest_dir, &stem, progress).await {
                Ok(p) => break p,
                Err(e) if attempt < 3 && is_transient(&e.to_string()) => {
                    attempt += 1;
                    tracing::warn!(%attempt, error = %e, "yt-dlp transient failure, retrying");
                    if attempt == 3 {
                        if let Some(idx) = cookie_arg_idx {
                            if idx != usize::MAX && idx < args.len() {
                                args.remove(idx);
                                args.remove(idx - 1); // the "--cookies" flag
                                tracing::warn!("retrying yt-dlp without the captured cookie");
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(5 * attempt as u64)).await;
                }
                Err(e) => return Err(e),
            }
        };
        let meta = self.cached_meta(&req.url).await;
        Ok(Outcome { files: vec![path], meta })
    }
}

impl YtdlpBackend {
    /// Channel / title from a cached `yt-dlp -J` (populated by `probe_qualities`
    /// when the quality menu was shown). Empty if the extract never ran.
    async fn cached_meta(&self, url: &str) -> super::EntryMeta {
        let Some(info) = self.info_cache.lock().await.get(url).cloned() else {
            return super::EntryMeta::default();
        };
        let s = |k: &str| info.get(k).and_then(Value::as_str).filter(|v| !v.is_empty()).map(str::to_string);
        super::EntryMeta {
            author: s("channel").or_else(|| s("uploader")).or_else(|| s("uploader_id")),
            title: s("title"),
            media_class: None,
        }
    }
}

// --- format helpers (probe_qualities) -------------------------------------

fn format_id(f: &Value) -> &str {
    f.get("format_id").and_then(Value::as_str).unwrap_or("")
}

fn has_video(f: &Value) -> bool {
    f.get("vcodec").and_then(Value::as_str).is_some_and(|c| c != "none")
}

fn has_audio(f: &Value) -> bool {
    f.get("acodec").and_then(Value::as_str).is_some_and(|c| c != "none")
}

fn bandwidth_of(f: &Value) -> u64 {
    if let Some(tbr) = f.get("tbr").and_then(Value::as_f64) {
        return (tbr * 1000.0) as u64;
    }
    f.get("filesize").or_else(|| f.get("filesize_approx")).and_then(Value::as_u64).unwrap_or(0)
}

fn resolution_of(f: &Value) -> Option<(u32, u32)> {
    let w = f.get("width").and_then(Value::as_u64)? as u32;
    let h = f.get("height").and_then(Value::as_u64)? as u32;
    (w > 0 && h > 0).then_some((w, h))
}

fn quality_option(f: &Value, key: impl Into<String>) -> QualityOption {
    let res = resolution_of(f);
    let note = f.get("format_note").and_then(Value::as_str).filter(|s| !s.is_empty()).unwrap_or("");
    // Prefer "1080p" / "720p60"; if the note has no digits (YouTube's "Premium"
    // enhanced-bitrate variant), prefix the real resolution so it's meaningful.
    let label = match (res, note.chars().any(|c| c.is_ascii_digit())) {
        (Some((_, h)), false) if !note.is_empty() => format!("{h}p ({note})"),
        (_, _) if !note.is_empty() => note.to_string(),
        (Some((_, h)), _) => format!("{h}p"),
        _ => format!("{} kbps", bandwidth_of(f) / 1000),
    };
    QualityOption { label, bandwidth: bandwidth_of(f), resolution: res, variant_key: key.into() }
}

fn best_audio_only(formats: &[Value]) -> Option<&Value> {
    formats.iter().filter(|f| has_audio(f) && !has_video(f)).max_by_key(|f| bandwidth_of(f))
}

fn final_stem(req: &DownloadReq) -> String {
    let name = req.filename_hint.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| "video".to_string());
    Path::new(&name).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(name)
}

// --- process helpers -----------------------------------------------------

fn host_of(url: &str) -> Option<String> {
    let after = url.split_once("://")?.1;
    let authority = after.split(['/', '?', '#']).next()?;
    Some(authority.rsplit('@').next()?.split(':').next()?.to_ascii_lowercase())
}

fn ytdlp_bin(cfg: &BackendConfig) -> PathBuf {
    if let Some(p) = &cfg.ytdlp_path {
        return p.clone();
    }
    if let Some(dir) = std::env::current_exe().ok().and_then(|p| p.parent().map(Path::to_path_buf)) {
        for name in ["yt-dlp.exe", "yt-dlp"] {
            for c in [dir.join("bin").join(name), dir.join(name)] {
                if c.exists() {
                    return c;
                }
            }
        }
    }
    PathBuf::from("yt-dlp")
}

/// A temp Netscape cookie file, deleted when dropped.
struct CookieFile(PathBuf);

impl Drop for CookieFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Cookies that actually matter for auth, per host family. Everything else the
/// browser sends (refresh tokens, telemetry, consent bookkeeping) is dead
/// weight that bloats every API request - and YouTube's InnerTube endpoint
/// returns **HTTP 413** (or just resets the connection) when the request
/// headers get too big.
fn is_useful_cookie(name: &str, host: &str) -> bool {
    let n = name.to_ascii_uppercase();
    if host.contains("youtube") || host.contains("google") {
        return matches!(
            n.as_str(),
            "SID" | "HSID" | "SSID" | "APISID" | "SAPISID"
                | "__SECURE-1PSID" | "__SECURE-3PSID"
                | "__SECURE-1PAPISID" | "__SECURE-3PAPISID"
                | "LOGIN_INFO" | "VISITOR_INFO1_LIVE" | "VISITOR_PRIVACY_METADATA"
                | "PREF" | "YSC" | "CONSENT" | "SOCS" | "GPS"
        );
    }
    true
}

/// Turn a captured `k=v; k2=v2` Cookie header into a temp Netscape cookie file
/// for `yt-dlp --cookies` (passing it inline blows the Windows 32 KiB
/// command-line limit). Filtered to the auth-relevant cookies and hard-capped
/// so an oversized jar can't make YouTube's API reject the request.
fn write_cookie_file(cookie: &str, url: &str) -> Option<CookieFile> {
    let cookie = cookie.trim();
    if cookie.is_empty() {
        return None;
    }
    let host = host_of(url)?;
    let base = host.strip_prefix("www.").unwrap_or(&host);
    let domain = format!(".{base}");

    let mut body = String::from("# Netscape HTTP Cookie File\n");
    let mut kept = 0;
    for pair in cookie.split(';') {
        let Some((k, v)) = pair.trim().split_once('=') else { continue };
        let (k, v) = (k.trim(), v.trim());
        if k.is_empty() || v.len() > 800 || !is_useful_cookie(k, &host) {
            continue;
        }
        body.push_str(&format!("{domain}\tTRUE\t/\tTRUE\t2000000000\t{k}\t{v}\n"));
        kept += 1;
        if kept >= 20 || body.len() > 4096 {
            break;
        }
    }
    if kept == 0 {
        return None;
    }
    let path = std::env::temp_dir().join(format!("luedd-cookies-{}.txt", uuid::Uuid::new_v4()));
    std::fs::write(&path, body).ok()?;
    Some(CookieFile(path))
}

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

async fn spawn_json(bin: &Path, args: &[String], timeout: Duration) -> Result<Value> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);

    let output = match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return Err(anyhow!("could not run {}: {e}", bin.display())),
        Err(_) => bail!("{} timed out", bin.display()),
    };
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("{} exited with {}: {}", bin.display(), output.status, err.lines().last().unwrap_or("").trim());
    }
    if output.stdout.len() > MAX_JSON_BYTES {
        bail!("{} produced {} bytes of JSON (cap {MAX_JSON_BYTES})", bin.display(), output.stdout.len());
    }
    serde_json::from_slice(&output.stdout).context("parsing yt-dlp JSON output")
}

/// Run `yt-dlp` as the downloader, streaming its `[download] N%` lines into
/// `JobEvent::Progress`. Returns the final file path.
async fn spawn_ytdlp_download(
    bin: &Path,
    args: &[String],
    dest_dir: &Path,
    stem: &str,
    progress: Option<&ProgressTx>,
) -> Result<PathBuf> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    // yt-dlp is Python; its stdout is block-buffered when piped, so progress
    // lines would only arrive in ~8 KiB bursts (or at process exit). Unbuffer
    // it so each `[download] N%` line reaches us live.
    cmd.env("PYTHONUNBUFFERED", "1");
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd.kill_on_drop(true);

    let mut child = cmd.spawn().with_context(|| format!("spawning {}", bin.display()))?;
    let stdout = child.stdout.take().expect("piped");
    let mut stderr = child.stderr.take().expect("piped");

    let mut printed_path: Option<PathBuf> = None;
    let mut lines = BufReader::new(stdout).lines();
    let started = Instant::now();

    let read = async {
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("[download]") {
                if let Some(p) = parse_progress_line(rest.trim()) {
                    if let Some(tx) = progress {
                        let _ = tx.send(p);
                    }
                }
            } else if line.starts_with("[Merger]") || line.starts_with("[ExtractAudio]") || line.starts_with("[VideoConvertor]") {
                if let Some(tx) = progress {
                    let _ = tx.send(JobEvent::Converting);
                }
            } else if !line.is_empty() && !line.starts_with('[') {
                let candidate = PathBuf::from(line);
                if candidate.is_absolute() || candidate.exists() {
                    printed_path = Some(candidate);
                }
            }
        }
    };

    let mut err_buf = Vec::new();
    let drain_err = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut err_buf);

    let status = match tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
        let (_, _) = tokio::join!(read, drain_err);
        child.wait().await
    })
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(anyhow!("yt-dlp wait failed: {e}")),
        Err(_) => bail!("yt-dlp download timed out after {}s", started.elapsed().as_secs()),
    };

    if !status.success() {
        let err = String::from_utf8_lossy(&err_buf);
        bail!("yt-dlp exited with {}: {}", status, err.lines().last().unwrap_or("").trim());
    }

    if let Some(p) = printed_path.filter(|p| p.exists()) {
        return Ok(p);
    }
    // Fallback: yt-dlp chose the container (mp4/mkv/webm) — find what landed.
    for ext in ["mp4", "mkv", "webm", "m4a", "mp3", "opus", "ogg"] {
        let c = dest_dir.join(format!("{stem}.{ext}"));
        if c.exists() {
            return Ok(c);
        }
    }
    bail!("yt-dlp finished but no output file was found for {stem}.*")
}

/// Parse `"  42.3% of  254.99MiB at  1.23MiB/s ETA 03:12"` (progressive) or
/// `"  2.9% of ~ 34.48MiB at 257.91KiB/s ETA 00:41 (frag 12/138)"` (HLS/DASH).
fn parse_progress_line(s: &str) -> Option<JobEvent> {
    let pct: f64 = s.split('%').next()?.trim().parse().ok()?;
    let total = s
        .split(" of ")
        .nth(1)
        .and_then(|rest| rest.split(" at ").next())
        .map(str::trim)
        .and_then(parse_size);
    let speed = s.split(" at ").nth(1).and_then(|rest| rest.split(" ETA").next()).map(str::trim).and_then(parse_rate);

    // Fragmented formats: "(frag 12/138)" is far more meaningful than the
    // size estimate. yt-dlp only shows it for the current stream, so it resets
    // between the video and audio legs - still a decent progress signal.
    let (done_units, total_units) = s
        .rsplit_once("(frag ")
        .and_then(|(_, r)| r.trim_end_matches(')').split_once('/'))
        .and_then(|(d, t)| Some((d.trim().parse().ok()?, t.trim().parse().ok()?)))
        .unwrap_or((0, 0));

    let downloaded = total.map(|t| (t as f64 * pct / 100.0) as u64).unwrap_or(0);
    Some(JobEvent::Progress {
        downloaded_bytes: downloaded,
        total_bytes: total,
        done_units,
        total_units,
        speed_bps: speed.unwrap_or(0),
    })
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim_start_matches('~').trim();
    let (num, unit) = s.split_at(s.find(|c: char| c.is_alphabetic())?);
    let n: f64 = num.trim().parse().ok()?;
    let mult = match unit.trim().to_ascii_uppercase().as_str() {
        "B" => 1.0,
        "KIB" | "KB" => 1024.0,
        "MIB" | "MB" => 1024.0 * 1024.0,
        "GIB" | "GB" => 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((n * mult) as u64)
}

fn parse_rate(s: &str) -> Option<u64> {
    parse_size(s.trim_end_matches("/s").trim())
}
