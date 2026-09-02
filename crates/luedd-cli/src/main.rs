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
        Command::Serve { port, download_dir } => run_serve(port, download_dir).await,
    }
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
    let registry = {
        let mut r = luedd_core::backend::BackendRegistry::with_builtins(client.clone());
        r.register(Arc::new(luedd_core::backend::YtdlpBackend::new(client.clone())));
        r.register(instagram.clone());
        Arc::new(r)
    };
    let ig_library = Arc::new(
        luedd_core::ig_library::IgLibraryStore::open(luedd_core::ig_library::default_ig_library_path(&data_dir)).await?,
    );
    let manager = Arc::new(
        DownloadManager::new(store.clone(), client, 2, 8)
            .with_backends(registry.clone(), settings.get().await.backends),
    );
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).with_context(|| format!("binding 127.0.0.1:{port}"))?;
    luedd_ipc::server::serve(store, manager, registry, instagram, ig_library, luedd_ipc::server::ServerConfig { settings, build_id: "cli".into(), on_new_detection: None, on_focus_request: None }, listener).await
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
