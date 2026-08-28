use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use wreq::Client;
use wreq_util::Emulation;

pub use wreq;

pub const DEFAULT_RETRY_ATTEMPTS: usize = 6;
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy)]
pub enum JobEvent {
    Progress { done: u64, total: u64 },
    Converting,
}

pub type ProgressTx = tokio::sync::mpsc::UnboundedSender<JobEvent>;

pub fn report_progress(tx: Option<&ProgressTx>, done: u64, total: u64) {
    if let Some(tx) = tx {
        let _ = tx.send(JobEvent::Progress { done, total });
    }
}

pub fn report_converting(tx: Option<&ProgressTx>) {
    if let Some(tx) = tx {
        let _ = tx.send(JobEvent::Converting);
    }
}

pub async fn retry<F, Fut, T>(attempts: usize, initial_delay: Duration, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut delay = initial_delay;
    let mut last_err = None;
    for attempt in 0..attempts.max(1) {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt + 1 < attempts {
                    tracing::debug!(attempt, error = %e, "retrying after failure");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.expect("attempts.max(1) guarantees at least one iteration"))
}

#[derive(Debug, Clone, Default)]
pub struct ProbeInfo {
    pub content_length: Option<u64>,
    pub accept_ranges: bool,
    pub content_type: Option<String>,
}

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    pub headers: HashMap<String, String>,
    pub cookies: Option<String>,
    pub byte_range: Option<(u64, u64)>,
}

#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    pub headers: HashMap<String, String>,
    pub cookie: Option<String>,
}

impl RequestContext {
    pub fn to_options(&self, byte_range: Option<(u64, u64)>) -> RequestOptions {
        RequestOptions { headers: self.headers.clone(), cookies: self.cookie.clone(), byte_range }
    }
}

pub fn describe_sent_headers(opts: &RequestOptions) -> String {
    let mut names: Vec<String> = opts
        .headers
        .keys()
        .filter(|k| !EXCLUDED_REPLAY_HEADERS.contains(&k.to_ascii_lowercase().as_str()))
        .cloned()
        .collect();
    if opts.cookies.is_some() && !names.iter().any(|n| n.eq_ignore_ascii_case("cookie")) {
        names.push("Cookie".to_string());
    }
    names.sort_unstable();

    let no_context_captured = !names.iter().any(|n| n.eq_ignore_ascii_case("referer") || n.eq_ignore_ascii_case("cookie"));
    if no_context_captured {
        format!(
            "sent headers: {} (no Referer/Cookie were captured for this download; a Chrome-emulation default User-Agent still applies)",
            if names.is_empty() { "none captured".to_string() } else { names.join(", ") }
        )
    } else {
        format!("sent headers: {}", names.join(", "))
    }
}

const EXCLUDED_REPLAY_HEADERS: &[&str] = &[
    "accept-encoding",
    "host",
    "content-length",
    "connection",
    "cookie",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "sec-fetch-user",
    "sec-gpc",
    "origin",
];

fn apply_headers(mut req: wreq::RequestBuilder, opts: &RequestOptions) -> wreq::RequestBuilder {
    for (k, v) in &opts.headers {
        let lower = k.to_ascii_lowercase();
        if EXCLUDED_REPLAY_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        req = req.header(k, v);
    }
    if let Some(cookie) = &opts.cookies {
        req = req.header("Cookie", cookie);
    }
    req
}

impl HttpClient {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .emulation(Emulation::Chrome137)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(180))
            .redirect(wreq::redirect::Policy::default())
            .pool_max_idle_per_host(0)
            .build()
            .context("failed to build wreq client")?;
        Ok(Self { client })
    }

    pub async fn get_text(&self, url: &str, opts: &RequestOptions) -> Result<String> {
        let bytes = self.get_bytes(url, opts).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    pub async fn get_bytes(&self, url: &str, opts: &RequestOptions) -> Result<Vec<u8>> {
        let mut req = apply_headers(self.client.get(url), opts);
        if let Some((offset, length)) = opts.byte_range {
            let end = offset + length.saturating_sub(1);
            req = req.header("Range", format!("bytes={offset}-{end}"));
        }

        let resp = req.send().await.with_context(|| format!("GET {url} failed"))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("GET {url} returned status {status} ({})", describe_sent_headers(opts));
        }
        let bytes = resp.bytes().await.with_context(|| format!("reading body of {url}"))?;
        Ok(bytes.to_vec())
    }

    pub async fn probe(&self, url: &str, opts: &RequestOptions) -> Result<ProbeInfo> {
        let req = apply_headers(self.client.head(url), opts);
        let resp = req.send().await.with_context(|| format!("HEAD {url} failed"))?;
        let content_length =
            resp.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
        let accept_ranges = resp
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);
        let content_type = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).map(str::to_string);
        Ok(ProbeInfo { content_length, accept_ranges, content_type })
    }

    pub async fn get_response(&self, url: &str, opts: &RequestOptions) -> Result<wreq::Response> {
        let mut req = apply_headers(self.client.get(url), opts);
        if let Some((offset, length)) = opts.byte_range {
            let end = offset + length.saturating_sub(1);
            req = req.header("Range", format!("bytes={offset}-{end}"));
        }
        req.send().await.with_context(|| format!("GET {url} failed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retry_succeeds_after_transient_failures() {
        let calls = AtomicUsize::new(0);
        let result = retry(4, Duration::from_millis(1), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    bail!("transient failure {n}");
                }
                Ok(n)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_propagates_last_error_after_exhausting_attempts() {
        let calls = AtomicUsize::new(0);
        let result: Result<()> = retry(3, Duration::from_millis(1), || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { bail!("always fails") }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_calls_exactly_once_on_immediate_success() {
        let calls = AtomicUsize::new(0);
        let result = retry(5, Duration::from_millis(1), || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, anyhow::Error>(42) }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn describe_sent_headers_reports_none_captured_with_emulation_hint_when_nothing_captured() {
        let opts = RequestOptions::default();
        assert_eq!(
            describe_sent_headers(&opts),
            "sent headers: none captured (no Referer/Cookie were captured for this download; a Chrome-emulation default User-Agent still applies)"
        );
    }

    #[test]
    fn describe_sent_headers_excludes_headers_that_apply_headers_also_strips() {
        let mut headers = HashMap::new();
        headers.insert("Referer".to_string(), "https://page.example/".to_string());
        headers.insert("Sec-Fetch-Site".to_string(), "cross-site".to_string());
        headers.insert("Origin".to_string(), "https://page.example".to_string());
        let opts = RequestOptions { headers, cookies: None, byte_range: None };

        let described = describe_sent_headers(&opts);
        assert_eq!(described, "sent headers: Referer");
    }

    #[test]
    fn describe_sent_headers_lists_header_names_sorted_without_leaking_values() {
        let mut headers = HashMap::new();
        headers.insert("Referer".to_string(), "https://secret-page.example/session=abc".to_string());
        headers.insert("User-Agent".to_string(), "Mozilla/5.0".to_string());
        let opts = RequestOptions { headers, cookies: Some("session=super-secret-token".to_string()), byte_range: None };

        let described = describe_sent_headers(&opts);
        assert_eq!(described, "sent headers: Cookie, Referer, User-Agent");
        assert!(!described.contains("secret"), "must not leak header/cookie values");
    }

    #[test]
    fn apply_headers_strips_hop_by_hop_headers_but_keeps_real_ones() {
        let mut headers = HashMap::new();
        headers.insert("Accept-Encoding".to_string(), "gzip, deflate, br, zstd".to_string());
        headers.insert("Host".to_string(), "stale-host.example".to_string());
        headers.insert("Connection".to_string(), "keep-alive".to_string());
        headers.insert("Referer".to_string(), "https://page.example/".to_string());
        headers.insert("Cookie".to_string(), "from-headers-map=1".to_string());
        let opts = RequestOptions { headers, cookies: Some("real-cookie=1".to_string()), byte_range: None };

        let client = Client::new();
        let req = apply_headers(client.get("https://example.com/"), &opts).build().unwrap();
        let sent = req.headers();

        assert!(!sent.contains_key("accept-encoding"), "must not replay the browser's Accept-Encoding");
        assert!(!sent.contains_key("host"), "must not replay a stale Host header");
        assert!(!sent.contains_key("connection"), "must not replay Connection");
        assert_eq!(sent.get("referer").unwrap(), "https://page.example/");
        let cookies: Vec<_> = sent.get_all("cookie").iter().collect();
        assert_eq!(cookies.len(), 1, "cookie must be sent exactly once, not duplicated from headers map");
        assert_eq!(cookies[0], "real-cookie=1");
    }

    #[test]
    fn apply_headers_strips_fetch_metadata_headers_a_standalone_client_cannot_honestly_reproduce() {
        let mut headers = HashMap::new();
        headers.insert("Sec-Fetch-Dest".to_string(), "empty".to_string());
        headers.insert("Sec-Fetch-Mode".to_string(), "cors".to_string());
        headers.insert("Sec-Fetch-Site".to_string(), "cross-site".to_string());
        headers.insert("Sec-GPC".to_string(), "1".to_string());
        headers.insert("Origin".to_string(), "https://page.example".to_string());
        headers.insert("Referer".to_string(), "https://page.example/".to_string());
        let opts = RequestOptions { headers, cookies: None, byte_range: None };

        let client = Client::new();
        let req = apply_headers(client.get("https://example.com/"), &opts).build().unwrap();
        let sent = req.headers();

        assert!(!sent.contains_key("sec-fetch-dest"));
        assert!(!sent.contains_key("sec-fetch-mode"));
        assert!(!sent.contains_key("sec-fetch-site"));
        assert!(!sent.contains_key("sec-gpc"));
        assert!(!sent.contains_key("origin"));
        assert_eq!(sent.get("referer").unwrap(), "https://page.example/", "Referer is real and should still be sent");
    }

    #[test]
    fn apply_headers_adds_no_user_agent_of_its_own_when_none_captured() {
        let opts = RequestOptions::default();
        let client = Client::new();
        let req = apply_headers(client.get("https://example.com/"), &opts).build().unwrap();
        let sent = req.headers();

        assert!(!sent.contains_key("user-agent"));
    }

    #[test]
    fn apply_headers_replays_a_captured_user_agent_exactly_once() {
        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_string(), "Mozilla/5.0 (Firefox real browser UA)".to_string());
        let opts = RequestOptions { headers, cookies: None, byte_range: None };

        let client = Client::new();
        let req = apply_headers(client.get("https://example.com/"), &opts).build().unwrap();
        let sent = req.headers();

        let uas: Vec<_> = sent.get_all("user-agent").iter().collect();
        assert_eq!(uas.len(), 1, "must not send two User-Agent headers");
        assert_eq!(uas[0], "Mozilla/5.0 (Firefox real browser UA)");
    }
}
