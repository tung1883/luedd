//! Native messaging host entry point - the process a browser launches via
//! `chrome.runtime.connectNative`/`sendNativeMessage` (stdio, length-prefixed
//! JSON per `tidm_ipc::wire`). This extension's actual data channel is a plain
//! HTTP fetch from the service worker to `127.0.0.1:8597` (see `server.rs`),
//! so this host's only job - matching what `XDM.App.Host` did for the newer
//! transport - is to make sure that server is actually running, launching the
//! main `tidm-app` process if it isn't, then ack back over stdio.
//!
//! This is a real gap XDM never closed on Linux: `LinuxNativeHost/xdm_messaging_host.py`
//! was an empty placeholder file. This implementation is deliberately
//! cross-platform (`std::process::Command`) rather than Windows-only.

use std::io::{stdin, stdout};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tidm_ipc::wire::{read_message, write_message};

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

    // A native messaging host handles one connection for its whole lifetime -
    // the browser keeps it running and can send further messages, but for our
    // purposes ("is the app up, launch it if not") one round-trip per launch
    // is enough, so we loop only to stay alive for as long as the browser
    // holds the pipe open (reading returns None on the browser's disconnect).
    loop {
        let msg: Option<IncomingMessage> = match read_message(&mut stdin) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("tidm-nmhost: wire read error: {e}");
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
                // Give the app a moment to bind its server before the extension's
                // next poll; a failed probe here isn't fatal, the extension will
                // just retry on its own ~1-minute alarm cadence.
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
    let exe = std::env::var("TIDM_APP_PATH").unwrap_or_else(|_| "tidm-app".to_string());
    std::process::Command::new(exe).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;
    Ok(())
}
