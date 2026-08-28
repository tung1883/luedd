
use std::io::{stdin, stdout};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use luedd_ipc::wire::{read_message, write_message};

const SERVER_URL: &str = "http://127.0.0.1:8597/sync";

#[derive(Debug, Deserialize)]
struct IncomingMessage {
    #[allow(dead_code)]
    #[serde(default)]
    action: Option<String>,
}

#[derive(Debug, Serialize)]
struct AckMessage {
    launched: bool,
    already_running: bool,
    error: Option<String>,
}

fn main() {
    let mut stdin = stdin();
    let mut stdout = stdout();

    loop {
        let msg: Option<IncomingMessage> = match read_message(&mut stdin) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("luedd-nmhost: wire read error: {e}");
                break;
            }
        };
        let Some(_msg) = msg else { break };

        let ack = ensure_app_running();
        if write_message(&mut stdout, &ack).is_err() {
            break;
        }
    }
}

fn ensure_app_running() -> AckMessage {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => return AckMessage { launched: false, already_running: false, error: Some(e.to_string()) },
    };

    rt.block_on(async {
        if probe_server().await {
            return AckMessage { launched: false, already_running: true, error: None };
        }

        match spawn_app() {
            Ok(()) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                AckMessage { launched: true, already_running: false, error: None }
            }
            Err(e) => AckMessage { launched: false, already_running: false, error: Some(e.to_string()) },
        }
    })
}

async fn probe_server() -> bool {
    reqwest::Client::new()
        .get(SERVER_URL)
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

fn spawn_app() -> std::io::Result<()> {
    let exe = std::env::var("LUEDD_APP_PATH").unwrap_or_else(|_| "luedd-app".to_string());
    std::process::Command::new(exe).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;
    Ok(())
}
