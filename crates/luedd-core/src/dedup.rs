//! Duplicate-file finder. Download names carry a per-download hash, so the same
//! video fetched twice never shares a filename: duplicates are found by content,
//! never by name.
//!
//! Pipeline (each stage only sees what the previous one could not rule out):
//!   1. walk the tree, group by size           (unique size = unique file)
//!   2. hash head + tail 64 KiB of each file    (small files: the whole file)
//!   3. full SHA-256 of what still collides
//! Stages 2 and 3 run on a few threads, report progress, and can be cancelled.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

const SAMPLE_BYTES: u64 = 64 * 1024;
const READ_BUF: usize = 1024 * 1024;
const THREADS: usize = 4;
const EMIT_EVERY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize)]
pub struct DupFile {
    pub path: PathBuf,
    /// Seconds since the Unix epoch (0 if unavailable).
    pub modified: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DupGroup {
    pub size: u64,
    pub sha256: String,
    /// Oldest first.
    pub files: Vec<DupFile>,
}

impl DupGroup {
    pub fn wasted_bytes(&self) -> u64 {
        self.size * (self.files.len() as u64 - 1)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DedupReport {
    pub scanned_files: usize,
    pub groups: Vec<DupGroup>,
    pub wasted_bytes: u64,
    /// The scan was cancelled; `groups` is empty.
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Progress {
    /// `walk` | `sample` | `hash`
    pub phase: &'static str,
    pub files_done: u64,
    pub files_total: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

struct Tracker<'a> {
    cb: &'a (dyn Fn(Progress) + Sync),
    phase: Mutex<(&'static str, u64, u64)>, // name, files_total, bytes_total
    files_done: AtomicU64,
    bytes_done: AtomicU64,
    last: Mutex<Instant>,
}

impl<'a> Tracker<'a> {
    fn new(cb: &'a (dyn Fn(Progress) + Sync)) -> Self {
        Tracker {
            cb,
            phase: Mutex::new(("walk", 0, 0)),
            files_done: AtomicU64::new(0),
            bytes_done: AtomicU64::new(0),
            last: Mutex::new(Instant::now() - EMIT_EVERY),
        }
    }

    fn start(&self, phase: &'static str, files_total: u64, bytes_total: u64) {
        *self.phase.lock().unwrap() = (phase, files_total, bytes_total);
        self.files_done.store(0, Ordering::Relaxed);
        self.bytes_done.store(0, Ordering::Relaxed);
        self.emit(true);
    }

    fn emit(&self, force: bool) {
        {
            let mut last = self.last.lock().unwrap();
            if !force && last.elapsed() < EMIT_EVERY {
                return;
            }
            *last = Instant::now();
        }
        let (phase, files_total, bytes_total) = *self.phase.lock().unwrap();
        (self.cb)(Progress {
            phase,
            files_done: self.files_done.load(Ordering::Relaxed),
            files_total,
            bytes_done: self.bytes_done.load(Ordering::Relaxed),
            bytes_total,
        });
    }

    fn bytes(&self, n: u64) {
        self.bytes_done.fetch_add(n, Ordering::Relaxed);
        self.emit(false);
    }

    fn file_done(&self) {
        self.files_done.fetch_add(1, Ordering::Relaxed);
        self.emit(false);
    }
}

/// Directories never worth scanning for duplicate downloads: dependency and VCS
/// trees, and anything marked as a cache (cargo's `target`, etc. carry a
/// `CACHEDIR.TAG`). They hold huge numbers of tiny files.
fn skip_dir(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    matches!(name, "node_modules" | ".git" | ".luedd-cache") || path.join("CACHEDIR.TAG").exists()
}

fn skip(path: &Path) -> bool {
    let in_cache = path.components().any(|c| c.as_os_str() == ".luedd-cache");
    let partial = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("partial") || e.eq_ignore_ascii_case("part"))
        .unwrap_or(false);
    in_cache || partial
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Feed `len` bytes starting at the file's current position into `hasher`.
/// `None` when cancelled.
fn feed(f: &mut File, mut len: u64, hasher: &mut Sha256, buf: &mut [u8], cancel: &AtomicBool, tr: &Tracker) -> std::io::Result<Option<()>> {
    while len > 0 {
        if cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let want = (buf.len() as u64).min(len) as usize;
        let n = f.read(&mut buf[..want])?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        tr.bytes(n as u64);
        len -= n as u64;
    }
    Ok(Some(()))
}

/// Hash of the head and tail samples; the whole file when it is no bigger than
/// the two samples (then it equals the full hash, `complete == true`).
fn sample_hash(path: &Path, size: u64, cancel: &AtomicBool, tr: &Tracker) -> Option<(String, bool)> {
    let mut f = File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; READ_BUF.min((SAMPLE_BYTES * 2) as usize)];
    let complete = size <= SAMPLE_BYTES * 2;
    if complete {
        feed(&mut f, size, &mut hasher, &mut buf, cancel, tr).ok()??;
    } else {
        feed(&mut f, SAMPLE_BYTES, &mut hasher, &mut buf, cancel, tr).ok()??;
        f.seek(SeekFrom::Start(size - SAMPLE_BYTES)).ok()?;
        feed(&mut f, SAMPLE_BYTES, &mut hasher, &mut buf, cancel, tr).ok()??;
    }
    tr.file_done();
    Some((hex(&hasher.finalize()), complete))
}

fn full_hash(path: &Path, size: u64, cancel: &AtomicBool, tr: &Tracker) -> Option<String> {
    let mut f = File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; READ_BUF];
    feed(&mut f, size, &mut hasher, &mut buf, cancel, tr).ok()??;
    tr.file_done();
    Some(hex(&hasher.finalize()))
}

/// `f` over every item on `THREADS` scoped threads, results in input order.
/// Once `cancel` is set no further items are started (their slots stay `None`).
fn par_map<T: Sync, R: Send>(items: &[T], cancel: &AtomicBool, f: impl Fn(&T) -> R + Sync) -> Vec<Option<R>> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    std::thread::scope(|s| {
        for _ in 0..THREADS.min(items.len().max(1)) {
            s.spawn(|| loop {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= items.len() {
                    break;
                }
                let r = f(&items[i]);
                out.lock().unwrap()[i] = Some(r);
            });
        }
    });
    out.into_inner().unwrap()
}

fn into_groups(entries: Vec<(PathBuf, u64, String)>) -> Vec<(u64, String, Vec<PathBuf>)> {
    let mut map: HashMap<(u64, String), Vec<PathBuf>> = HashMap::new();
    for (path, size, hash) in entries {
        map.entry((size, hash)).or_default().push(path);
    }
    map.into_iter().filter(|(_, v)| v.len() > 1).map(|((s, h), v)| (s, h, v)).collect()
}

/// Scan `dir` recursively. Files smaller than `min_size` bytes are ignored
/// (empty files are all "identical"); unreadable files are skipped. Blocking.
pub fn scan(dir: &Path, min_size: u64) -> DedupReport {
    scan_with(&[dir.to_path_buf()], min_size, &AtomicBool::new(false), &|_| {})
}

/// Roots that sit inside another root would list their files twice.
fn without_nested(dirs: &[PathBuf]) -> Vec<PathBuf> {
    let canon: Vec<PathBuf> = dirs.iter().map(|d| std::fs::canonicalize(d).unwrap_or_else(|_| d.clone())).collect();
    let mut keep = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let nested = canon.iter().enumerate().any(|(j, other)| {
            j != i && canon[i].starts_with(other) && (canon[i] != *other || j < i)
        });
        if !nested {
            keep.push(d.clone());
        }
    }
    keep
}

/// Scan several folders as one pool: a file in one folder can duplicate a file in
/// another.
pub fn scan_with(dirs: &[PathBuf], min_size: u64, cancel: &AtomicBool, on_progress: &(dyn Fn(Progress) + Sync)) -> DedupReport {
    let tr = Tracker::new(on_progress);
    let cancelled = |scanned| DedupReport { scanned_files: scanned, groups: Vec::new(), wasted_bytes: 0, cancelled: true };

    // 1. walk
    tr.start("walk", 0, 0);
    let mut candidates: Vec<(PathBuf, u64)> = Vec::new();
    let mut scanned = 0usize;
    for dir in without_nested(dirs) {
        let walker = WalkDir::new(&dir)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| e.depth() == 0 || !e.file_type().is_dir() || !skip_dir(e.path()));
        for entry in walker.filter_map(Result::ok) {
            if cancel.load(Ordering::Relaxed) {
                return cancelled(scanned);
            }
            if !entry.file_type().is_file() || skip(entry.path()) {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            scanned += 1;
            tr.file_done();
            if meta.len() >= min_size.max(1) {
                candidates.push((entry.path().to_path_buf(), meta.len()));
            }
        }
    }

    let mut by_size: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    for (p, size) in candidates {
        by_size.entry(size).or_default().push(p);
    }
    let same_size: Vec<(PathBuf, u64)> = by_size
        .into_iter()
        .filter(|(_, v)| v.len() > 1)
        .flat_map(|(size, v)| v.into_iter().map(move |p| (p, size)))
        .collect();

    // 2. head + tail sample
    let sample_bytes: u64 = same_size.iter().map(|(_, s)| (*s).min(SAMPLE_BYTES * 2)).sum();
    tr.start("sample", same_size.len() as u64, sample_bytes);
    let sampled = par_map(&same_size, cancel, |(p, s)| sample_hash(p, *s, cancel, &tr));
    if cancel.load(Ordering::Relaxed) {
        return cancelled(scanned);
    }
    let mut finished: Vec<(u64, String, Vec<PathBuf>)> = Vec::new(); // whole-file hashes already
    let mut partial_entries: Vec<(PathBuf, u64, String)> = Vec::new();
    let mut complete_entries: Vec<(PathBuf, u64, String)> = Vec::new();
    for ((path, size), res) in same_size.into_iter().zip(sampled) {
        match res.flatten() {
            Some((h, true)) => complete_entries.push((path, size, h)),
            Some((h, false)) => partial_entries.push((path, size, h)),
            None => {}
        }
    }
    finished.extend(into_groups(complete_entries));

    // 3. full hash of the files whose samples still collide
    let need_full: Vec<(PathBuf, u64)> = into_groups(partial_entries)
        .into_iter()
        .flat_map(|(size, _, v)| v.into_iter().map(move |p| (p, size)))
        .collect();
    let full_bytes: u64 = need_full.iter().map(|(_, s)| *s).sum();
    tr.start("hash", need_full.len() as u64, full_bytes);
    let hashed = par_map(&need_full, cancel, |(p, s)| full_hash(p, *s, cancel, &tr));
    if cancel.load(Ordering::Relaxed) {
        return cancelled(scanned);
    }
    let full_entries: Vec<(PathBuf, u64, String)> = need_full
        .into_iter()
        .zip(hashed)
        .filter_map(|((p, s), h)| h.flatten().map(|h| (p, s, h)))
        .collect();
    finished.extend(into_groups(full_entries));

    let mut groups: Vec<DupGroup> = finished
        .into_iter()
        .map(|(size, sha256, paths)| {
            let mut files: Vec<DupFile> = paths
                .into_iter()
                .map(|path| {
                    let modified = std::fs::metadata(&path)
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    DupFile { path, modified }
                })
                .collect();
            files.sort_by(|a, b| a.modified.cmp(&b.modified).then_with(|| a.path.cmp(&b.path)));
            DupGroup { size, sha256, files }
        })
        .collect();
    groups.sort_by(|a, b| b.wasted_bytes().cmp(&a.wasted_bytes()).then_with(|| a.sha256.cmp(&b.sha256)));
    let wasted_bytes = groups.iter().map(DupGroup::wasted_bytes).sum();
    DedupReport { scanned_files: scanned, groups, wasted_bytes, cancelled: false }
}

/// One duplicate group's outcome: keep `keep`, remove everything in `delete`.
#[derive(Debug, Clone, Deserialize)]
pub struct DeletePlan {
    pub keep: PathBuf,
    pub delete: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteFailure {
    pub path: PathBuf,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DeleteReport {
    pub deleted: Vec<PathBuf>,
    pub freed_bytes: u64,
    pub failed: Vec<DeleteFailure>,
}

fn read_range(f: &mut File, at: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    f.seek(SeekFrom::Start(at))?;
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Cheap guard against a file that changed since the scan: same size, and the
/// head and tail samples are identical.
fn still_matches(keep: &Path, other: &Path) -> std::io::Result<bool> {
    let (mut a, mut b) = (File::open(keep)?, File::open(other)?);
    let size = a.metadata()?.len();
    if size != b.metadata()?.len() {
        return Ok(false);
    }
    let n = size.min(SAMPLE_BYTES) as usize;
    if read_range(&mut a, 0, n)? != read_range(&mut b, 0, n)? {
        return Ok(false);
    }
    let tail = size.saturating_sub(n as u64);
    Ok(read_range(&mut a, tail, n)? == read_range(&mut b, tail, n)?)
}

/// Move the unwanted copies to the Recycle Bin (recoverable). Each file is
/// re-checked against its kept twin first; anything that no longer matches, or
/// is the kept file itself, is refused rather than deleted.
pub fn delete_extras(plans: &[DeletePlan]) -> DeleteReport {
    let mut report = DeleteReport::default();
    for plan in plans {
        let keep_ok = std::fs::metadata(&plan.keep).map(|m| m.is_file()).unwrap_or(false);
        let keep_canon = std::fs::canonicalize(&plan.keep).ok();
        for path in &plan.delete {
            let fail = |report: &mut DeleteReport, e: String| report.failed.push(DeleteFailure { path: path.clone(), error: e });
            if !keep_ok {
                fail(&mut report, "the file to keep is missing".into());
                continue;
            }
            if keep_canon.is_some() && std::fs::canonicalize(path).ok() == keep_canon {
                fail(&mut report, "this is the file being kept".into());
                continue;
            }
            let size = match std::fs::metadata(path) {
                Ok(m) if m.is_file() => m.len(),
                Ok(_) => {
                    fail(&mut report, "not a file".into());
                    continue;
                }
                Err(e) => {
                    fail(&mut report, e.to_string());
                    continue;
                }
            };
            match still_matches(&plan.keep, path) {
                Ok(true) => {}
                Ok(false) => {
                    fail(&mut report, "no longer identical to the kept file".into());
                    continue;
                }
                Err(e) => {
                    fail(&mut report, e.to_string());
                    continue;
                }
            }
            match trash::delete(path) {
                Ok(()) => {
                    report.freed_bytes += size;
                    report.deleted.push(path.clone());
                }
                Err(e) => fail(&mut report, e.to_string()),
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("luedd-dedup-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn finds_identical_content_under_different_names() {
        let d = tmp("dup");
        std::fs::write(d.join("a-1111.mp4"), b"same bytes here").unwrap();
        std::fs::create_dir_all(d.join("sub")).unwrap();
        std::fs::write(d.join("sub/b-2222.mp4"), b"same bytes here").unwrap();
        std::fs::write(d.join("c.mp4"), b"different bytes!").unwrap();
        let r = scan(&d, 1);
        assert_eq!(r.scanned_files, 3);
        assert_eq!(r.groups.len(), 1);
        assert_eq!(r.groups[0].files.len(), 2);
        assert_eq!(r.wasted_bytes, 15);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn same_size_different_content_is_not_a_duplicate() {
        let d = tmp("size");
        std::fs::write(d.join("a"), b"aaaa").unwrap();
        std::fs::write(d.join("b"), b"bbbb").unwrap();
        assert!(scan(&d, 1).groups.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn differs_only_in_the_middle_is_still_distinguished() {
        // head and tail samples match, only the middle differs: must take the
        // full-hash stage to tell them apart
        let d = tmp("mid");
        let n = (SAMPLE_BYTES * 4) as usize;
        let mut x = vec![7u8; n];
        let mut y = x.clone();
        x[n / 2] = 1;
        y[n / 2] = 2;
        std::fs::write(d.join("x"), &x).unwrap();
        std::fs::write(d.join("y"), &y).unwrap();
        assert!(scan(&d, 1).groups.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn large_identical_files_group_via_full_hash() {
        let d = tmp("large");
        let data: Vec<u8> = (0..(SAMPLE_BYTES * 5) as usize).map(|i| (i % 251) as u8).collect();
        std::fs::write(d.join("x"), &data).unwrap();
        std::fs::write(d.join("y"), &data).unwrap();
        let r = scan(&d, 1);
        assert_eq!(r.groups.len(), 1);
        let full = hex(&Sha256::digest(&data));
        assert_eq!(r.groups[0].sha256, full);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn ignores_partials_cache_dirs_and_small_files() {
        let d = tmp("skip");
        std::fs::create_dir_all(d.join(".luedd-cache/x")).unwrap();
        std::fs::write(d.join("a.partial"), b"dupdupdup").unwrap();
        std::fs::write(d.join(".luedd-cache/x/seg"), b"dupdupdup").unwrap();
        std::fs::write(d.join("keep"), b"dupdupdup").unwrap();
        std::fs::write(d.join("e1"), b"").unwrap();
        std::fs::write(d.join("e2"), b"").unwrap();
        assert!(scan(&d, 1).groups.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn skips_dependency_vcs_and_cache_dirs() {
        let d = tmp("junk");
        for dir in ["node_modules/x", ".git/objects", "target/debug"] {
            std::fs::create_dir_all(d.join(dir)).unwrap();
        }
        std::fs::write(d.join("target/CACHEDIR.TAG"), b"Signature: 8a477f597d28d172789f06886806bc55").unwrap();
        for f in ["node_modules/x/a", ".git/objects/b", "target/debug/c", "keep"] {
            std::fs::write(d.join(f), b"dupdupdup").unwrap();
        }
        let r = scan(&d, 1);
        assert_eq!(r.scanned_files, 1);
        assert!(r.groups.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn duplicates_are_found_across_folders_and_nested_roots_are_not_double_counted() {
        let d = tmp("multi");
        std::fs::create_dir_all(d.join("one")).unwrap();
        std::fs::create_dir_all(d.join("two")).unwrap();
        std::fs::create_dir_all(d.join("one/inner")).unwrap();
        std::fs::write(d.join("one/a"), b"same bytes").unwrap();
        std::fs::write(d.join("two/b"), b"same bytes").unwrap();
        std::fs::write(d.join("one/inner/c"), b"unique one").unwrap();
        // `one/inner` is inside `one`; `one` is listed twice
        let dirs = vec![d.join("one"), d.join("two"), d.join("one/inner"), d.join("one")];
        let r = scan_with(&dirs, 1, &AtomicBool::new(false), &|_| {});
        assert_eq!(r.scanned_files, 3);
        assert_eq!(r.groups.len(), 1);
        assert_eq!(r.groups[0].files.len(), 2);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn delete_refuses_the_kept_file_a_missing_keeper_and_changed_files() {
        let d = tmp("del");
        std::fs::write(d.join("keep"), b"same bytes").unwrap();
        std::fs::write(d.join("changed"), b"diff bytes!").unwrap();
        let r = delete_extras(&[
            DeletePlan { keep: d.join("keep"), delete: vec![d.join("keep"), d.join("changed")] },
            DeletePlan { keep: d.join("gone"), delete: vec![d.join("keep")] },
        ]);
        assert!(r.deleted.is_empty());
        assert_eq!(r.failed.len(), 3);
        assert!(d.join("keep").exists() && d.join("changed").exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn cancel_returns_flagged_empty_report() {
        let d = tmp("cancel");
        std::fs::write(d.join("a"), b"same").unwrap();
        std::fs::write(d.join("b"), b"same").unwrap();
        let r = scan_with(&[d.clone()], 1, &AtomicBool::new(true), &|_| {});
        assert!(r.cancelled && r.groups.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }
}
