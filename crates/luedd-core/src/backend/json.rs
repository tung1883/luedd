//! Lüdd-JSON: the plugin behind "download every asset in a JSON file".
//!
//! The scan (finding asset URLs in a document) lives in [`crate::json_assets`];
//! this backend only runs the per-asset downloads it queues, so they get their
//! own provider tab in the main list. It never claims a URL on its own - entries
//! are created with `backend_id = "json"` by the `/json/queue` endpoint - and
//! hands the actual transfer to the matching built-in transport.

use anyhow::Result;
use async_trait::async_trait;
use luedd_net::{HttpClient, ProgressTx};

use super::{Confidence, DashBackend, DownloadBackend, DownloadReq, HlsBackend, HttpBackend, Outcome, Sniff};
use crate::jobs::DownloadKind;

pub struct JsonBackend {
    http: HttpBackend,
    hls: HlsBackend,
    dash: DashBackend,
}

impl JsonBackend {
    pub fn new(client: HttpClient) -> Self {
        Self {
            http: HttpBackend::new(client.clone()),
            hls: HlsBackend::new(client.clone()),
            dash: DashBackend::new(client),
        }
    }
}

#[async_trait]
impl DownloadBackend for JsonBackend {
    fn id(&self) -> &'static str {
        "json"
    }

    fn can_handle(&self, _url: &str, _sniff: Option<&Sniff>) -> Confidence {
        Confidence::No
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        match DownloadKind::guess_from_url(&req.url) {
            DownloadKind::Hls => self.hls.run(req, progress).await,
            DownloadKind::Dash => self.dash.run(req, progress).await,
            DownloadKind::Http => self.http.run(req, progress).await,
        }
    }
}
