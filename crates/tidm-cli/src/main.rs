use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tidm_core::jobs::DownloadKind;
use tidm_core::queue::{default_settings_path, default_store_path, DownloadEntry, DownloadManager, DownloadStore, SettingsStore};
use tidm_net::{HttpClient, RequestContext};

#[derive(Parser)]
#[command(name = "tidm-cli", about = "tidm-rs milestone driver / debugging CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download an HLS (.m3u8) stream to a playable file.
    Hls {
        url: String,
        #[arg(short, long, default_value = "out.mp4")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Download a plain file over HTTP with multi-connection resume support.
    Get {
        url: String,
        #[arg(short, long, default_value = "out.bin")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Download a DASH (.mpd) stream to a playable file.
    Dash {
        url: String,
        #[arg(short, long, default_value = "out.mp4")]
        output: PathBuf,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Persisted download queue (M3): add entries, list them, or run everything queued.
    Queue {
        #[command(subcommand)]
        action: QueueAction,
    },
    /// Run the local server (M4) the browser extension's connector.js talks to.
    Serve {
        #[arg(short, long, default_value_t = 8597)]
        port: u16,
        #[arg(short, long)]
        download_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum QueueAction {
    /// Add a URL to the persisted queue (kind auto-detected from the URL/extension).
    Add {
        url: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// List all entries and their status.
    List,
    /// Run every entry currently queued, up to --max-concurrent at once.
    Run {
        #[arg(short, long, default_value_t = 2)]
        max_concurrent: usize,
        #[arg(short, long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Remove one entry by id. Pass --delete-files to also delete its output/temp files.
    Remove {
        id: String,
        #[arg(long)]
        delete_files: bool,
    },
    /// Reset a Failed/Cancelled entry back to Queued so the next `run` retries it.
    Retry { id: String },
    /// Remove every Finished/Failed/Cancelled entry. Pass --delete-files to also
    /// delete each one's output/temp files.
    ClearFinished {
        #[arg(long)]
        delete_files: bool,
    },
}

fn store_path() -> PathBuf {
    default_store_path(&tidm_core::queue::default_data_dir())
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
        Command::Serve { port, download_dir } => run_serve(port, download_dir).await,
    }
}

async fn run_serve(port: u16, download_dir: Option<PathBuf>) -> Result<()> {
    let data_dir = tidm_core::queue::default_data_dir();
    let store = Arc::new(DownloadStore::open(store_path()).await?);
    let settings = Arc::new(SettingsStore::open(default_settings_path(&data_dir), &data_dir).await?);
    if let Some(dir) = download_dir {
        let mut current = settings.get().await;
        current.download_dir = dir;
        settings.set(current).await?;
    }
    tokio::fs::create_dir_all(&settings.get().await.download_dir).await.ok();
    let client = HttpClient::new().context("building http client")?;
    let manager = Arc::new(DownloadManager::new(store.clone(), client, 2, 8));
    tidm_ipc::server::serve(store, manager, tidm_ipc::server::ServerConfig { settings }, port).await
}

async fn run_job(kind: DownloadKind, url: &str, output: &PathBuf, concurrency: usize) -> Result<()> {
    let client = HttpClient::new()?;
    tidm_core::jobs::run(&client, kind, url, output, concurrency, &RequestContext::default(), None, None).await?;
    tracing::info!(output = %output.display(), "done");
    Ok(())
}

async fn run_queue_action(action: QueueAction) -> Result<()> {
    let store = Arc::new(DownloadStore::open(store_path()).await?);

    match action {
        QueueAction::Add { url, output } => {
            let kind = DownloadKind::guess_from_url(&url);
            let output = tidm_core::jobs::sanitize_dest_for_kind(&output, kind);
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
