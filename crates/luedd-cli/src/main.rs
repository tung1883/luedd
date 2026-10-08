use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use luedd_core::jobs::DownloadKind;
use luedd_core::queue::{default_settings_path, default_store_path, DownloadEntry, DownloadManager, DownloadStore, SettingsStore};
use luedd_net::{HttpClient, RequestContext};

#[derive(Parser)]
#[command(name = "luedd-cli", about = "luedd-rs milestone driver / debugging CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Hls {
        url: String,
        #[arg(short, long, default_value = "out.mp4")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    Get {
        url: String,
        #[arg(short, long, default_value = "out.bin")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    Dash {
        url: String,
        #[arg(short, long, default_value = "out.mp4")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    Queue {
        #[command(subcommand)]
        action: QueueAction,
    },
    /// Report duplicate files (same content, any name) under a folder.
    Dedup {
        /// Folders to scan together; defaults to the configured download folder.
        dirs: Vec<PathBuf>,
        /// Ignore files smaller than this many bytes (default 1 MiB).
        #[arg(long, default_value_t = 1_048_576)]
        min_size: u64,
        #[arg(long)]
        json: bool,
    },
    /// List every asset URL inside a JSON document (URL or local file).
    Json {
        source: String,
        /// Also keep absolute http(s) strings without a known file extension.
        #[arg(long)]
        scan_all: bool,
        /// Browser `Cookie` header to send when fetching a URL.
        #[arg(long)]
        cookie: Option<String>,
        /// Object key whose value labels each asset's group (e.g. title).
        #[arg(long)]
        name_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Serve {
        #[arg(short, long, default_value_t = 8597)]
        port: u16,
        #[arg(short, long)]
        download_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum QueueAction {
    Add {
        url: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    List,
    Run {
        #[arg(short, long, default_value_t = 2)]
        max_concurrent: usize,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    Remove {
        id: String,
        #[arg(long)]
        delete_files: bool,
    },
    Retry { id: String },
    ClearFinished {
        #[arg(long)]
        delete_files: bool,
    },
}

fn store_path() -> PathBuf {
    default_store_path(&luedd_core::queue::default_data_dir())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Command::Hls { url, output, concurrency } => run_job(DownloadKind::Hls, &url, &output, concurrency).await,
        Command::Get { url, output, concurrency } => run_job(DownloadKind::Http, &url, &output, concurrency).await,
        Command::Dash { url, output, concurrency } => run_job(DownloadKind::Dash, &url, &output, concurrency).await,
        Command::Queue { action } => run_queue_action(action).await,
        Command::Dedup { dirs, min_size, json } => run_dedup(dirs, min_size, json).await,
        Command::Json { source, scan_all, cookie, name_key, json } => {
            run_json(source, scan_all, cookie, name_key, json).await
        }
        Command::Serve { port, download_dir } => run_serve(port, download_dir).await,
    }
}

fn human_size(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 { format!("{n} B") } else { format!("{v:.1} {}", UNITS[i]) }
}

async fn run_json(source: String, scan_all: bool, cookie: Option<String>, name_key: Option<String>, json: bool) -> Result<()> {
    let (text, base) = if source.starts_with("http://") || source.starts_with("https://") {
        let client = HttpClient::new()?;
        let ctx = RequestContext { headers: Default::default(), cookie };
        let text = client.get_text(&source, &ctx.to_options(None)).await?;
        (text, url::Url::parse(&source).ok())
    } else {
        (std::fs::read_to_string(&source).with_context(|| format!("reading {source}"))?, None)
    };
    let bom = char::from_u32(0xFEFF).unwrap_or(' ');
    let value: serde_json::Value = serde_json::from_str(text.trim_start_matches(bom)).context("not valid JSON")?;
    let res = luedd_core::json_assets::scan(&value, &luedd_core::json_assets::ScanOptions { base, scan_all, name_key });
    if json {
        println!("{}", serde_json::to_string_pretty(&res)?);
        return Ok(());
    }
    for a in &res.assets {
        let dup = if a.duplicate { "  (duplicate)" } else { "" };
        println!("{}\t{}{}", a.path, a.url, dup);
    }
    let dups = res.assets.iter().filter(|a| a.duplicate).count();
    eprintln!("{} assets ({} duplicate) in {} nodes{}", res.assets.len(), dups, res.nodes, if res.truncated { ", truncated" } else { "" });
    Ok(())
}

async fn run_dedup(dirs: Vec<PathBuf>, min_size: u64, json: bool) -> Result<()> {
    let dirs = if dirs.is_empty() {
        let data_dir = luedd_core::queue::default_data_dir();
        let settings = SettingsStore::open(default_settings_path(&data_dir), &data_dir).await?;
        vec![settings.get().await.download_dir]
    } else {
        dirs
    };
    let scan_dirs = dirs.clone();
    let report = tokio::task::spawn_blocking(move || {
        let never = std::sync::atomic::AtomicBool::new(false);
        luedd_core::dedup::scan_with(&scan_dirs, min_size, &never, &|p| {
            if !json {
                eprint!(
                    "\r{:<7} {}/{} files  {} / {}   ",
                    p.phase,
                    p.files_done,
                    p.files_total,
                    human_size(p.bytes_done),
                    human_size(p.bytes_total)
                );
            }
        })
    })
    .await?;
    if !json {
        eprintln!();
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    for (i, g) in report.groups.iter().enumerate() {
        println!("#{} {} x{}  (sha256 {})", i + 1, human_size(g.size), g.files.len(), &g.sha256[..12.min(g.sha256.len())]);
        for (j, f) in g.files.iter().enumerate() {
            println!("  {} {}", if j == 0 { "keep?" } else { "dup  " }, f.path.display());
        }
    }
    println!(
        "{} duplicate group(s), {} reclaimable, {} file(s) scanned in {}",
        report.groups.len(),
        human_size(report.wasted_bytes),
        report.scanned_files,
        dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(", ")
    );
    Ok(())
}

async fn run_serve(port: u16, download_dir: Option<PathBuf>) -> Result<()> {
    let data_dir = luedd_core::queue::default_data_dir();
    let store = Arc::new(DownloadStore::open(store_path()).await?);
    let settings = Arc::new(SettingsStore::open(default_settings_path(&data_dir), &data_dir).await?);
    if let Some(dir) = download_dir {
        let mut current = settings.get().await;
        current.download_dir = dir;
        settings.set(current).await?;
    }
    tokio::fs::create_dir_all(&settings.get().await.download_dir).await.ok();
    let client = HttpClient::new().context("building http client")?;
    let instagram = Arc::new(luedd_core::backend::InstagramBackend::new(client.clone()));
    let ytdlp = Arc::new(luedd_core::backend::YtdlpBackend::new(client.clone()));
    let registry = {
        let mut r = luedd_core::backend::BackendRegistry::with_builtins(client.clone());
        r.register(Arc::new(luedd_core::backend::JsonBackend::new(client.clone())));
        r.register(Arc::new(luedd_core::backend::DocsBackend));
        r.register(ytdlp.clone());
        r.register(instagram.clone());
        r.register(Arc::new(luedd_core::backend::TorrentBackend::new(data_dir.join("torrent"))));
        Arc::new(r)
    };
    let ig_library = Arc::new(
        luedd_core::ig_library::IgLibraryStore::open(luedd_core::ig_library::default_ig_library_path(&data_dir)).await?,
    );
    let yt_library = Arc::new(
        luedd_core::yt_library::YtLibraryStore::open(luedd_core::yt_library::default_yt_library_path(&data_dir)).await?,
    );
    let manager = Arc::new(
        DownloadManager::new(store.clone(), client, 2, 8)
            .with_backends(registry.clone(), settings.get().await.backends),
    );
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).with_context(|| format!("binding 127.0.0.1:{port}"))?;
    luedd_ipc::server::serve(store, manager, registry, instagram, ig_library, ytdlp, yt_library, luedd_ipc::server::ServerConfig { settings, build_id: "cli".into(), on_new_detection: None, on_focus_request: None, ig_cookie_cache: Some(data_dir.join("ig_session")), json_library_path: Some(luedd_core::json_library::default_json_library_path(&data_dir)), on_json_open: None }, listener).await
}

async fn run_job(kind: DownloadKind, url: &str, output: &PathBuf, concurrency: usize) -> Result<()> {
    let client = HttpClient::new()?;
    luedd_core::jobs::run(&client, kind, url, output, concurrency, &RequestContext::default(), None, None).await?;
    tracing::info!(output = %output.display(), "done");
    Ok(())
}

async fn run_queue_action(action: QueueAction) -> Result<()> {
    let store = Arc::new(DownloadStore::open(store_path()).await?);

    match action {
        QueueAction::Add { url, output } => {
            let kind = DownloadKind::guess_from_url(&url);
            let output = luedd_core::jobs::sanitize_dest_for_kind(&output, kind);
            let entry = DownloadEntry::new(url, output, kind);
            println!("queued {} (kind={:?})", entry.id, entry.kind);
            store.add_entry(entry).await?;
        }
        QueueAction::List => {
            for entry in store.list_entries().await {
                println!(
                    "{}  {:?}  {:?}  {}{}",
                    entry.id,
                    entry.kind,
                    entry.status,
                    entry.url,
                    entry.error.map(|e| format!("  error={e}")).unwrap_or_default()
                );
            }
        }
        QueueAction::Run { max_concurrent, concurrency } => {
            let client = HttpClient::new().context("building http client")?;
            let manager = DownloadManager::new(store, client, max_concurrent, concurrency);
            manager.run_queued().await?;
        }
        QueueAction::Remove { id, delete_files } => {
            let client = HttpClient::new().context("building http client")?;
            let manager = DownloadManager::new(store, client, 1, 1);
            if manager.remove_entry(&id, delete_files).await? {
                println!("removed {id}");
            } else {
                println!("no entry with id {id}");
            }
        }
        QueueAction::Retry { id } => {
            if store.retry_entry(&id).await? {
                println!("queued {id} for retry");
            } else {
                println!("entry {id} was not Failed/Cancelled, nothing to retry");
            }
        }
        QueueAction::ClearFinished { delete_files } => {
            let client = HttpClient::new().context("building http client")?;
            let manager = DownloadManager::new(store, client, 1, 1);
            let count = manager.clear_finished(delete_files).await?;
            println!("cleared {count} finished/failed/cancelled entries");
        }
    }
    Ok(())
}
