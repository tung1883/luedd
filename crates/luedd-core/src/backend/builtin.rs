//! The three transport built-ins. Thin wrappers over [`crate::jobs`] — the
//! download engine itself is unchanged.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use luedd_media::quality::{probe_dash_qualities, probe_hls_qualities, QualityOption};
use luedd_net::{HttpClient, ProgressTx};

use super::{Confidence, DownloadBackend, DownloadReq, Outcome, Sniff};
use crate::jobs::{self, DownloadKind};

fn dest_for(req: &DownloadReq, kind: DownloadKind) -> PathBuf {
    let name = req.filename_hint.clone().unwrap_or_else(|| "download".to_string());
    jobs::sanitize_dest_for_kind(&req.dest_dir.join(name), kind)
}

fn sniff_ext_is(sniff: Option<&Sniff>, want: &str) -> bool {
    sniff.and_then(|s| s.real_ext.as_deref()) == Some(want)
}

// --- http -----------------------------------------------------------------

pub struct HttpBackend {
    client: HttpClient,
}

impl HttpBackend {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DownloadBackend for HttpBackend {
    fn id(&self) -> &'static str {
        "http"
    }

    fn can_handle(&self, _url: &str, _sniff: Option<&Sniff>) -> Confidence {
        // The universal fallback — never claims a URL strongly.
        Confidence::Weak
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let dest = dest_for(req, DownloadKind::Http);
        let path = jobs::run_http(&self.client, &req.url, &dest, req.concurrency, &req.ctx, progress).await?;
        Ok(Outcome::single(path))
    }
}

// --- hls ------------------------------------------------------------------

pub struct HlsBackend {
    client: HttpClient,
}

impl HlsBackend {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DownloadBackend for HlsBackend {
    fn id(&self) -> &'static str {
        "hls"
    }

    fn can_handle(&self, url: &str, sniff: Option<&Sniff>) -> Confidence {
        if DownloadKind::guess_from_url(url) == DownloadKind::Hls || sniff_ext_is(sniff, "m3u8") {
            Confidence::Strong
        } else {
            Confidence::No
        }
    }

    async fn probe_qualities(&self, req: &DownloadReq) -> Result<Vec<QualityOption>> {
        probe_hls_qualities(&self.client, &req.url, &req.ctx).await
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let dest = dest_for(req, DownloadKind::Hls);
        let path = jobs::run_hls(
            &self.client,
            &req.url,
            &dest,
            req.concurrency,
            &req.ctx,
            progress,
            req.quality.as_deref(),
        )
        .await?;
        Ok(Outcome::single(path))
    }
}

// --- dash -----------------------------------------------------------------

pub struct DashBackend {
    client: HttpClient,
}

impl DashBackend {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DownloadBackend for DashBackend {
    fn id(&self) -> &'static str {
        "dash"
    }

    fn can_handle(&self, url: &str, sniff: Option<&Sniff>) -> Confidence {
        if DownloadKind::guess_from_url(url) == DownloadKind::Dash || sniff_ext_is(sniff, "mpd") {
            Confidence::Strong
        } else {
            Confidence::No
        }
    }

    async fn probe_qualities(&self, req: &DownloadReq) -> Result<Vec<QualityOption>> {
        probe_dash_qualities(&self.client, &req.url, &req.ctx).await
    }

    async fn run(&self, req: &DownloadReq, progress: Option<&ProgressTx>) -> Result<Outcome> {
        let dest = dest_for(req, DownloadKind::Dash);
        let path = jobs::run_dash(
            &self.client,
            &req.url,
            &dest,
            req.concurrency,
            &req.ctx,
            progress,
            req.quality.as_deref(),
        )
        .await?;
        Ok(Outcome::single(path))
    }
}
