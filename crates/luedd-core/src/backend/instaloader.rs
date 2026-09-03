//! instaloader **engine** for [`InstagramBackend`].
//!
//! Lüdd-Insta has two interchangeable download engines behind one backend:
//!
//! - **custom** (default) - the hand-rolled GraphQL/REST crawler in
//!   `instagram.rs`.
//! - **instaloader** - forks the [instaloader](https://instaloader.github.io/)
//!   CLI. Auth comes from the browser `sessionid` cookie (written to a temp
//!   Netscape cookie file via `--load-cookies`, which needs instaloader ≥ 4.11).
//!
//! [`engine_plan`] turns `BackendConfig.instagram_engine_main` +
//! `instagram_engine_fallback` into an ordered list of engines to try (primary
//! first, then a distinct fallback). Single highlights-by-id always collapse to
//! `[Custom]` (the CLI can't target one highlight). [`run`] executes the
//! instaloader engine and returns the produced media files; the caller attaches
//! grouping metadata and walks the plan.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use luedd_net::{JobEvent, ProgressTx};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex, Semaphore};

use super::instagram::Target;
use super::{BackendConfig, DownloadReq};

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
const AVAIL_TTL: Duration = Duration::from_secs(60);
/// `--load-cookies` landed in instaloader 4.11 (Nov 2023). Older installs fall
/// back to the custom engine.
const MIN_VERSION: (u32, u32) = (4, 11);

/// Media extensions instaloader writes that we keep.
const MEDIA_EXTS: &[&str] = &["jpg", "jpeg", "png", "webp", "mp4"];

/// instaloader is heavy and aggressively rate-limited - one run at a time,
/// process-wide.
fn slots() -> &'static Semaphore {
    static S: OnceLock<Semaphore> = OnceLock::new();
    S.get_or_init(|| Semaphore::new(1))
}

/// Cached `(checked_at, usable_version)` for the resolved program.
fn avail_cache() -> &'static Mutex<Option<(Instant, Option<(u32, u32)>)>> {
    static C: OnceLock<Mutex<Option<(Instant, Option<(u32, u32)>)>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(None))
}

/// Which engine a Lüdd-Insta download should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Engine {
    Custom,
    Instaloader,
}

fn parse_engine(s: Option<&str>) -> Option<Engine> {
    match s {
        Some("instaloader") => Some(Engine::Instaloader),
        Some("custom") => Some(Engine::Custom),
        _ => None,
    }
}

/// Ordered engine plan for one download: the configured primary
/// (`instagram_engine_main`, default `Custom`), then the fallback
/// (`instagram_engine_fallback`, default none) when it is set and distinct.
///
/// A single highlight-by-id always collapses to `[Custom]` - the CLI can't
/// target one highlight.
pub(crate) fn engine_plan(cfg: &BackendConfig, target: &Target) -> Vec<Engine> {
    if matches!(target, Target::Highlight(_)) {
        return vec![Engine::Custom];
    }
    let main = parse_engine(cfg.instagram_engine_main.as_deref()).unwrap_or(Engine::Custom);
    let mut plan = vec![main];
    if let Some(fallback) = parse_engine(cfg.instagram_engine_fallback.as_deref()) {
        if fallback != main {
            plan.push(fallback);
        }
    }
    plan
}

/// `(program, leading_args)` - `cfg.instaloader_path` → `instaloader[.exe]` next
/// to the exe or in `<exe>/bin/` → `<python> -m instaloader` → bare `instaloader`.
fn program(cfg: &BackendConfig) -> (PathBuf, Vec<String>) {
    if let Some(p) = &cfg.instaloader_path {
        return (p.clone(), Vec::new());
    }
    if let Some(dir) = std::env::current_exe().ok().and_then(|p| p.parent().map(Path::to_path_buf)) {
        for name in ["instaloader.exe", "instaloader"] {
            for c in [dir.join("bin").join(name), dir.join(name)] {
                if c.exists() {
                    return (c, Vec::new());
                }
            }
        }
    }
    if let Some(py) = &cfg.python_path {
        return (py.clone(), vec!["-m".into(), "instaloader".into()]);
    }
    (PathBuf::from("instaloader"), Vec::new())
}

/// Is a usable (≥ 4.11) instaloader reachable? Cached for 60 s.
async fn usable(cfg: &BackendConfig) -> bool {
    {
        let g = avail_cache().lock().await;
        if let Some((at, v)) = g.as_ref() {
            if at.elapsed() < AVAIL_TTL {
                return v.is_some();
            }
        }
    }
    let (prog, mut args) = program(cfg);
    args.push("--version".into());
    let ver = probe_version(&prog, &args).await;
    let usable = ver.filter(|v| *v >= MIN_VERSION);
    if ver.is_some() && usable.is_none() {
        tracing::warn!(
            version = ?ver,
            "instaloader is older than {}.{} (no --load-cookies) - using the custom engine",
            MIN_VERSION.0,
            MIN_VERSION.1
        );
    }
    *avail_cache().lock().await = Some((Instant::now(), usable));
    usable.is_some()
}

/// How instaloader will actually be invoked, as a human string
/// (`instaloader`, an absolute path, or `<python> -m instaloader`).
pub fn resolved_program(cfg: &BackendConfig) -> String {
    let (prog, lead) = program(cfg);
    if lead.is_empty() {
        prog.display().to_string()
    } else {
        format!("{} {}", prog.display(), lead.join(" "))
    }
}

/// For the Settings status line:
/// `(installed_and_usable, version_string, resolved_program)`.
/// `version_string` is `Some` whenever *any* instaloader answers `--version`,
/// even one too old for `--load-cookies`.
pub async fn instaloader_status(cfg: &BackendConfig) -> (bool, Option<String>, String) {
    let (prog, mut args) = program(cfg);
    args.push("--version".into());
    let resolved = resolved_program(cfg);
    match probe_version(&prog, &args).await {
        Some(v) => (v >= MIN_VERSION, Some(format!("{}.{}", v.0, v.1)), resolved),
        None => (false, None, resolved),
    }
}

/// Run the instaloader engine for `target`, writing media into `req.dest_dir`.
/// Returns the produced media files (sorted); the caller wraps them in an
/// [`super::Outcome`] with its own grouping metadata.
pub(crate) async fn run(
    req: &DownloadReq,
    target: &Target,
    progress: Option<&ProgressTx>,
) -> Result<Vec<PathBuf>> {
    let cfg = &req.config;
    if !usable(cfg).await {
        bail!(
            "instaloader is not installed or too old (need >= {}.{}) - run \
             `pip install \"instaloader>=4.11\"` or set its path in Settings",
            MIN_VERSION.0,
            MIN_VERSION.1
        );
    }
    let _permit = slots().acquire().await.expect("semaphore closed");
    let (prog, lead) = program(cfg);

    let cookie = req
        .ctx
        .cookie
        .as_deref()
        .filter(|c| c.contains("sessionid="))
        .or(cfg.instagram.session_cookie.as_deref());
    let cookie_file = cookie.and_then(write_ig_cookie_file);

    let args = build_args(target, &req.dest_dir, cookie_file.as_ref().map(|c| c.0.as_path()), &lead);

    let before = list_dir(&req.dest_dir);
    let files = spawn_instaloader(&prog, &args, &req.dest_dir, before, progress).await?;
    if files.is_empty() {
        bail!("instaloader produced no files (private account not followed, expired story, or bad cookie)");
    }
    Ok(files)
}

/// Pure arg-vector builder (unit-tested). `lead` is the `-m instaloader` prefix
/// when we invoke via python, else empty.
fn build_args(target: &Target, dest_dir: &Path, cookie_file: Option<&Path>, lead: &[String]) -> Vec<String> {
    let mut args: Vec<String> = lead.to_vec();
    args.extend([
        "--no-metadata-json".to_string(),
        "--no-compress-json".to_string(),
        "--no-captions".to_string(),
        "--no-profile-pic".to_string(),
        "--no-video-thumbnails".to_string(),
        "--dirname-pattern".to_string(),
        dest_dir.to_string_lossy().into_owned(),
        "--filename-pattern".to_string(),
        "{date_utc}_UTC".to_string(),
    ]);
    if let Some(cf) = cookie_file {
        args.push("--load-cookies".to_string());
        args.push(cf.to_string_lossy().into_owned());
    }
    match target {
        Target::Shortcode(code, _) => {
            args.push("--".to_string());
            args.push(format!("-{code}"));
        }
        Target::Profile(user) => {
            args.push("--".to_string());
            args.push(user.clone());
        }
        Target::Stories(user) => {
            args.push("--stories".to_string());
            args.push("--no-posts".to_string());
            args.push("--".to_string());
            args.push(user.clone());
        }
        // Highlight-by-id never reaches here (engine_plan forces Custom).
        Target::Highlight(_) => {
            args.push("--".to_string());
        }
    }
    args
}

// --- process / fs helpers ------------------------------------------------

fn list_dir(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect()
}

fn is_media(p: &Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .map(|e| MEDIA_EXTS.contains(&e.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// Parse `instaloader 4.14.1` (from stdout or stderr) into `(major, minor)`.
fn parse_version(text: &str) -> Option<(u32, u32)> {
    let tok = text.split_whitespace().find(|t| t.chars().next().is_some_and(|c| c.is_ascii_digit()))?;
    let mut it = tok.split('.');
    let major: u32 = it.next()?.parse().ok()?;
    let minor: u32 = it.next().unwrap_or("0").trim_matches(|c: char| !c.is_ascii_digit()).parse().unwrap_or(0);
    Some((major, minor))
}

async fn probe_version(prog: &Path, args: &[String]) -> Option<(u32, u32)> {
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);
    let out = tokio::time::timeout(VERSION_TIMEOUT, cmd.output()).await.ok()?.ok()?;
    let text =
        format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    parse_version(&text)
}

/// `"[  3/ 45] https://..."` → `(3, 45)`.
fn parse_counter_line(s: &str) -> Option<(u64, u64)> {
    let inner = s.trim().strip_prefix('[')?.split(']').next()?;
    let (d, t) = inner.split_once('/')?;
    Some((d.trim().parse().ok()?, t.trim().parse().ok()?))
}

/// A temp Netscape cookie file, deleted when dropped.
struct CookieFile(PathBuf);

impl Drop for CookieFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Turn a captured `k=v; k2=v2` Cookie header into a temp Netscape cookie file
/// for `instaloader --load-cookies`, keeping only the Instagram auth cookies.
fn write_ig_cookie_file(cookie: &str) -> Option<CookieFile> {
    const KEEP: &[&str] = &["sessionid", "csrftoken", "ds_user_id", "mid", "ig_did"];
    let cookie = cookie.trim();
    if cookie.is_empty() {
        return None;
    }
    let mut body = String::from("# Netscape HTTP Cookie File\n");
    let mut kept = 0;
    for pair in cookie.split(';') {
        let Some((k, v)) = pair.trim().split_once('=') else { continue };
        let (k, v) = (k.trim(), v.trim());
        if v.is_empty() || !KEEP.contains(&k) {
            continue;
        }
        body.push_str(&format!(".instagram.com\tTRUE\t/\tTRUE\t2000000000\t{k}\t{v}\n"));
        kept += 1;
    }
    if kept == 0 {
        return None;
    }
    let path = std::env::temp_dir().join(format!("luedd-ig-cookies-{}.txt", uuid::Uuid::new_v4()));
    std::fs::write(&path, body).ok()?;
    Some(CookieFile(path))
}

/// Run instaloader, streaming its `[ n/ m]` counter lines (on stderr) into
/// `JobEvent::Progress`. Returns the media files that appeared in `dest_dir`.
async fn spawn_instaloader(
    prog: &Path,
    args: &[String],
    dest_dir: &Path,
    before: Vec<PathBuf>,
    progress: Option<&ProgressTx>,
) -> Result<Vec<PathBuf>> {
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd.env("PYTHONUNBUFFERED", "1");
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd.kill_on_drop(true);

    let mut child = cmd.spawn().with_context(|| format!("spawning {}", prog.display()))?;
    let stderr = child.stderr.take().expect("piped");
    let mut stdout = child.stdout.take().expect("piped");
    let started = Instant::now();

    if let Some(tx) = progress {
        let _ = tx.send(JobEvent::Progress {
            downloaded_bytes: 0,
            total_bytes: None,
            done_units: 0,
            total_units: 1,
            speed_bps: 0,
        });
    }

    let mut last_lines: Vec<String> = Vec::new();
    let mut lines = BufReader::new(stderr).lines();
    let read = async {
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }
            if let Some((done, total)) = parse_counter_line(&line) {
                if let Some(tx) = progress {
                    let _ = tx.send(JobEvent::Progress {
                        downloaded_bytes: 0,
                        total_bytes: None,
                        done_units: done,
                        total_units: total.max(done),
                        speed_bps: 0,
                    });
                }
            }
            last_lines.push(line);
            if last_lines.len() > 12 {
                last_lines.remove(0);
            }
        }
    };
    let mut out_buf = Vec::new();
    let drain_out = tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut out_buf);

    let status = match tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
        let (_, _) = tokio::join!(read, drain_out);
        child.wait().await
    })
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(anyhow!("instaloader wait failed: {e}")),
        Err(_) => bail!("instaloader timed out after {}s", started.elapsed().as_secs()),
    };

    if !status.success() {
        let msg = last_lines.iter().rev().find(|l| !l.starts_with('[')).cloned().unwrap_or_default();
        bail!("instaloader exited with {status}: {msg}");
    }

    let mut files: Vec<PathBuf> =
        list_dir(dest_dir).into_iter().filter(|p| is_media(p) && !before.contains(p)).collect();
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_reads_either_stream() {
        assert_eq!(parse_version("instaloader 4.14.1"), Some((4, 14)));
        assert_eq!(parse_version("Instaloader\n4.9\n"), Some((4, 9)));
        assert_eq!(parse_version("no version here"), None);
    }

    #[test]
    fn parse_counter_line_reads_progress() {
        assert_eq!(parse_counter_line("[  3/ 45] https://scontent..."), Some((3, 45)));
        assert_eq!(parse_counter_line("[ 45/ 45]"), Some((45, 45)));
        assert_eq!(parse_counter_line("Retrieving posts from profile x."), None);
    }

    #[test]
    fn write_ig_cookie_file_keeps_only_session_cookies() {
        let big = "datr=junk; sessionid=ABC%3A123; rur=EAG; csrftoken=TOK; ds_user_id=42; ig_nrcb=1";
        let cf = write_ig_cookie_file(big).expect("some cookies kept");
        let body = std::fs::read_to_string(&cf.0).unwrap();
        assert!(body.starts_with("# Netscape HTTP Cookie File"));
        assert!(body.contains("\tsessionid\tABC%3A123"));
        assert!(body.contains("\tcsrftoken\tTOK"));
        assert!(body.contains("\tds_user_id\t42"));
        assert!(!body.contains("datr"));
        assert!(!body.contains("rur"));
    }

    #[test]
    fn build_args_shapes_per_target() {
        let dir = Path::new("/dl/quynhingx");

        let a = build_args(&Target::Shortcode("Abc123".into(), false), dir, None, &[]);
        assert!(a.windows(2).any(|w| w[0] == "--dirname-pattern" && w[1] == "/dl/quynhingx"));
        assert_eq!(a.last().unwrap(), "-Abc123");
        assert_eq!(a[a.len() - 2], "--");

        let p = build_args(&Target::Profile("quynhingx".into()), dir, None, &[]);
        assert_eq!(p.last().unwrap(), "quynhingx");

        let s = build_args(&Target::Stories("quynhingx".into()), dir, Some(Path::new("/tmp/c.txt")), &[]);
        assert!(s.contains(&"--stories".to_string()));
        assert!(s.contains(&"--no-posts".to_string()));
        assert!(s.windows(2).any(|w| w[0] == "--load-cookies" && w[1] == "/tmp/c.txt"));

        let py = build_args(&Target::Profile("x".into()), dir, None, &["-m".into(), "instaloader".into()]);
        assert_eq!(&py[..2], &["-m".to_string(), "instaloader".to_string()]);
    }

    #[test]
    fn highlight_always_uses_custom_engine() {
        let mut cfg = BackendConfig::default();
        cfg.instagram_engine_main = Some("instaloader".to_string());
        cfg.instagram_engine_fallback = Some("instaloader".to_string());
        assert_eq!(engine_plan(&cfg, &Target::Highlight("123".into())), vec![Engine::Custom]);
    }

    #[test]
    fn unset_plan_is_custom_only() {
        let cfg = BackendConfig::default();
        assert_eq!(
            engine_plan(&cfg, &Target::Shortcode("abc".into(), false)),
            vec![Engine::Custom]
        );
    }

    #[test]
    fn main_and_distinct_fallback_are_ordered() {
        let cfg = BackendConfig {
            instagram_engine_main: Some("instaloader".into()),
            instagram_engine_fallback: Some("custom".into()),
            ..Default::default()
        };
        assert_eq!(
            engine_plan(&cfg, &Target::Profile("x".into())),
            vec![Engine::Instaloader, Engine::Custom]
        );
    }

    #[test]
    fn fallback_equal_to_main_is_dropped() {
        let cfg = BackendConfig {
            instagram_engine_main: Some("instaloader".into()),
            instagram_engine_fallback: Some("instaloader".into()),
            ..Default::default()
        };
        assert_eq!(engine_plan(&cfg, &Target::Profile("x".into())), vec![Engine::Instaloader]);
    }
}
