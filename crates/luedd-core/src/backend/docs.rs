//! Lüdd-Docs: documents captured page by page in the browser (Google Drive
//! viewer, Scribd, Studocu ...). The extension streams the pages to the app,
//! which writes the PDF and records a finished entry with `backend_id = "docs"`
//! (see the `/docs/*` endpoints). This backend only exists so those entries get
//! their own provider tab; there is nothing to fetch on a retry.

use anyhow::{bail, Result};
use async_trait::async_trait;
use luedd_net::ProgressTx;

use super::{Confidence, DownloadBackend, DownloadReq, Outcome, Sniff};

pub struct DocsBackend;

#[async_trait]
impl DownloadBackend for DocsBackend {
    fn id(&self) -> &'static str {
        "docs"
    }

    fn can_handle(&self, _url: &str, _sniff: Option<&Sniff>) -> Confidence {
        Confidence::No
    }

    async fn run(&self, _req: &DownloadReq, _progress: Option<&ProgressTx>) -> Result<Outcome> {
        bail!("Lüdd-Docs pages are captured in the browser; open the document and use the Lüdd pill again")
    }
}
