//! Download benchmark: `serve` runs a loopback HTTP/Range server (its own process,
//! so its CPU never lands in the client's numbers); `run` performs one measured
//! download and prints one JSON line. `scripts/bench.ps1` drives both, for a
//! baseline build and the current build, several runs each.
//!
//!   bench_download serve --size 1073741824 [--rate-mbps 40] [--jitter] [--slow-tail]
//!                        [--latency-ms 40] [--small-size 204800]
//!   bench_download run --scenario big|many --url http://127.0.0.1:PORT --dest FILE_OR_DIR
//!                      [--conns 4] [--files 100] [--parallel 1] [--item-delay-ms 0]

use std::io::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use luedd_net::{HttpClient, RequestContext};

const RING: usize = 1 << 20;

/// Deterministic content: byte at absolute position `pos` of any file.
fn ring() -> Vec<u8> {
    (0..RING).map(|i| ((i * 31 + i / 251) % 256) as u8).collect()
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}
fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

// ---------------------------------------------------------------- server ----

struct ServeCfg {
    size: u64,
    small_size: u64,
    rate_bps: Option<u64>,
    jitter: bool,
    slow_tail: bool,
    latency: Duration,
}

async fn serve(args: &[String]) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cfg = std::sync::Arc::new(ServeCfg {
        size: arg(args, "--size").and_then(|v| v.parse().ok()).unwrap_or(1 << 30),
        small_size: arg(args, "--small-size").and_then(|v| v.parse().ok()).unwrap_or(204_800),
        rate_bps: arg(args, "--rate-mbps").and_then(|v| v.parse::<f64>().ok()).map(|m| (m * 1_048_576.0) as u64),
        jitter: flag(args, "--jitter"),
        slow_tail: flag(args, "--slow-tail"),
        latency: Duration::from_millis(arg(args, "--latency-ms").and_then(|v| v.parse().ok()).unwrap_or(0)),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("PORT {}", listener.local_addr().unwrap().port());
    std::io::stdout().flush().ok();
    let ring = std::sync::Arc::new(ring());

    loop {
        let Ok((mut s, _)) = listener.accept().await else { continue };
        s.set_nodelay(true).ok();
        let (cfg, ring) = (cfg.clone(), ring.clone());
        tokio::spawn(async move {
            let mut pending: Vec<u8> = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                let end = loop {
                    if let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(i);
                    }
                    match s.read(&mut tmp).await {
                        Ok(0) | Err(_) => break None,
                        Ok(n) => pending.extend_from_slice(&tmp[..n]),
                    }
                };
                let Some(i) = end else { return };
                let head = String::from_utf8_lossy(&pending[..i]).to_string();
                pending.drain(..i + 4);
                let mut first = head.lines().next().unwrap_or("").split_whitespace();
                let method = first.next().unwrap_or("").to_string();
                let path = first.next().unwrap_or("").to_string();
                let range = head.lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("range: bytes=").map(str::to_string));

                if !cfg.latency.is_zero() {
                    tokio::time::sleep(cfg.latency).await;
                }
                let total = if path.starts_with("/small/") { cfg.small_size } else { cfg.size };
                let (status, a, b) = match &range {
                    Some(r) => {
                        let (a, b) = r.split_once('-').unwrap();
                        let a: u64 = a.parse().unwrap_or(0);
                        let b: u64 = b.parse::<u64>().unwrap_or(total - 1).min(total - 1);
                        ("206 Partial Content", a, b)
                    }
                    None => ("200 OK", 0, total - 1),
                };
                let len = b - a + 1;
                let cr = if range.is_some() { format!("Content-Range: bytes {a}-{b}/{total}\r\n") } else { String::new() };
                let hdr = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{cr}Content-Type: application/octet-stream\r\nConnection: keep-alive\r\n\r\n",
                    if method == "HEAD" { total } else { len }
                );
                if s.write_all(hdr.as_bytes()).await.is_err() {
                    return;
                }
                if method == "HEAD" {
                    continue;
                }
                // speed factor for this response
                let mut factor = 1.0f64;
                if cfg.jitter {
                    let h = (a.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as f64 / (1u64 << 24) as f64; // 0..1
                    factor = 0.3 + 1.2 * h;
                }
                if cfg.slow_tail && a >= total / 4 * 3 {
                    factor *= 0.25;
                }
                let mut pos = a;
                let mut sent = 0u64;
                let t0 = Instant::now();
                let mut chunk: Vec<u8> = Vec::with_capacity(64 * 1024);
                while sent < len {
                    let n = (len - sent).min(64 * 1024) as usize;
                    chunk.clear();
                    let mut left = n;
                    let mut p = pos;
                    while left > 0 {
                        let off = (p % RING as u64) as usize;
                        let take = left.min(RING - off);
                        chunk.extend_from_slice(&ring[off..off + take]);
                        p += take as u64;
                        left -= take;
                    }
                    if s.write_all(&chunk).await.is_err() {
                        return;
                    }
                    pos += n as u64;
                    sent += n as u64;
                    if let Some(rate) = cfg.rate_bps {
                        let want = Duration::from_secs_f64(sent as f64 / (rate as f64 * factor));
                        if let Some(sleep) = want.checked_sub(t0.elapsed()) {
                            tokio::time::sleep(sleep).await;
                        }
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------- metrics ---

#[derive(Default, Clone, Copy)]
struct Snap {
    write_ops: u64,
    write_bytes: u64,
    cpu_s: f64,
    kernel_s: f64,
    peak_mb: f64,
}

#[cfg(windows)]
fn snap() -> Snap {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessIoCounters, GetProcessTimes, IO_COUNTERS};
    unsafe {
        let h = GetCurrentProcess();
        let mut io: IO_COUNTERS = std::mem::zeroed();
        GetProcessIoCounters(h, &mut io);
        let (mut c, mut e, mut k, mut u): (FILETIME, FILETIME, FILETIME, FILETIME) = std::mem::zeroed();
        GetProcessTimes(h, &mut c, &mut e, &mut k, &mut u);
        let ft = |f: FILETIME| ((f.dwHighDateTime as u64) << 32 | f.dwLowDateTime as u64) as f64 / 1e7;
        let mut mem: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
        mem.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        GetProcessMemoryInfo(h, &mut mem, mem.cb);
        Snap {
            write_ops: io.WriteOperationCount,
            write_bytes: io.WriteTransferCount,
            cpu_s: ft(k) + ft(u),
            kernel_s: ft(k),
            peak_mb: mem.PeakWorkingSetSize as f64 / 1_048_576.0,
        }
    }
}

#[cfg(not(windows))]
fn snap() -> Snap {
    Snap::default()
}

fn verify(path: &std::path::Path, expect_len: u64) -> bool {
    use std::io::Read;
    let ring = ring();
    let Ok(mut f) = std::fs::File::open(path) else { return false };
    if f.metadata().map(|m| m.len()).unwrap_or(0) != expect_len {
        return false;
    }
    let mut buf = vec![0u8; 1 << 20];
    let mut pos = 0u64;
    loop {
        let n = f.read(&mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        for (i, b) in buf[..n].iter().enumerate() {
            if *b != ring[((pos + i as u64) % RING as u64) as usize] {
                return false;
            }
        }
        pos += n as u64;
    }
    pos == expect_len
}

// ----------------------------------------------------------------- client ---

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("serve") => serve(&args).await,
        Some("run") => run(&args).await,
        _ => eprintln!("usage: bench_download serve|run ..."),
    }
}

async fn run(args: &[String]) {
    let scenario = arg(args, "--scenario").unwrap_or_else(|| "big".into());
    let base = arg(args, "--url").expect("--url");
    let dest = PathBuf::from(arg(args, "--dest").expect("--dest"));
    let conns: usize = arg(args, "--conns").and_then(|v| v.parse().ok()).unwrap_or(4);
    let client = HttpClient::new().unwrap();
    let ctx = RequestContext::default();

    let before = snap();
    let t0 = Instant::now();
    let (bytes, ok);
    match scenario.as_str() {
        "big" => {
            let expect: u64 = arg(args, "--size").and_then(|v| v.parse().ok()).expect("--size");
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            let r = luedd_core::progressive::download(&client, &format!("{base}/big.bin"), &dest, conns, &ctx, None).await;
            let wall = t0.elapsed();
            let after = snap();
            bytes = expect;
            ok = r.is_ok() && verify(&dest, expect);
            report(&scenario, ok, wall, bytes, before, after, 1);
            std::fs::remove_file(&dest).ok();
        }
        _ => {
            // many small files, like an Instagram profile's CDN items
            let files: usize = arg(args, "--files").and_then(|v| v.parse().ok()).unwrap_or(100);
            let parallel: usize = arg(args, "--parallel").and_then(|v| v.parse().ok()).unwrap_or(1);
            let item_delay = Duration::from_millis(arg(args, "--item-delay-ms").and_then(|v| v.parse().ok()).unwrap_or(0));
            let small: u64 = arg(args, "--small-size").and_then(|v| v.parse().ok()).unwrap_or(204_800);
            std::fs::create_dir_all(&dest).unwrap();
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(parallel.max(1)));
            let mut tasks = Vec::new();
            for i in 0..files {
                let permit = sem.clone().acquire_owned().await.unwrap();
                let (client, ctx) = (client.clone(), ctx.clone());
                let url = format!("{base}/small/{i}.jpg");
                let out = dest.join(format!("{i}.jpg"));
                tasks.push(tokio::spawn(async move {
                    #[cfg(feature = "bench_new")]
                    let r = luedd_core::jobs::run_http_simple(&client, &url, &out, &ctx).await;
                    #[cfg(not(feature = "bench_new"))]
                    let r = luedd_core::jobs::run_http(&client, &url, &out, 1, &ctx, None).await;
                    drop(permit);
                    r.is_ok()
                }));
                if !item_delay.is_zero() && parallel <= 1 {
                    // the old loop slept after every item
                    tokio::time::sleep(item_delay).await;
                }
            }
            let mut all_ok = true;
            for t in tasks {
                all_ok &= t.await.unwrap_or(false);
            }
            let wall = t0.elapsed();
            let after = snap();
            let good = (0..files).all(|i| verify(&dest.join(format!("{i}.jpg")), small));
            bytes = small * files as u64;
            ok = all_ok && good;
            report(&scenario, ok, wall, bytes, before, after, files);
            std::fs::remove_dir_all(&dest).ok();
        }
    }
}

fn report(scenario: &str, ok: bool, wall: Duration, bytes: u64, a: Snap, b: Snap, files: usize) {
    let secs = wall.as_secs_f64();
    println!(
        "{}",
        serde_json::json!({
            "scenario": scenario,
            "ok": ok,
            "wall_ms": (secs * 1000.0).round(),
            "mb_s": ((bytes as f64 / 1_048_576.0) / secs * 10.0).round() / 10.0,
            "cpu_s": ((b.cpu_s - a.cpu_s) * 1000.0).round() / 1000.0,
            "kernel_s": ((b.kernel_s - a.kernel_s) * 1000.0).round() / 1000.0,
            "write_ops": b.write_ops - a.write_ops,
            "write_bytes_x": ((b.write_bytes - a.write_bytes) as f64 / bytes as f64 * 100.0).round() / 100.0,
            "write_ops_per_file": ((b.write_ops - a.write_ops) as f64 / files as f64 * 10.0).round() / 10.0,
            "peak_mb": b.peak_mb.round(),
            "files_s": (files as f64 / secs * 10.0).round() / 10.0,
        })
    );
}
