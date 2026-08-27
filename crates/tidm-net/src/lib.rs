use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use wreq::Client;
use wreq_util::Emulation;

pub use wreq;

pub const DEFAULT_RETRY_ATTEMPTS: usize = 6;
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Sent over a download's `ProgressTx` to report either forward progress or a
/// transition into the (network-idle, CPU-bound) muxing phase - a job runner
/// (`tidm-core::jobs`) emits both from the same channel so the receiving end
/// (`DownloadManager`) can update the persisted `DownloadEntry`'s progress and
/// status from one place.
#[derive(Debug, Clone, Copy)]
pub enum JobEvent {
    /// `(done, total)` - segments for HLS/DASH, bytes for a plain HTTP download.
    Progress { done: u64, total: u64 },
    /// All segments/pieces are fetched; ffmpeg is now muxing them into the
    /// final output. `Progress` events *do* still follow one of these -
    /// `tidm_media::mux::run_ffmpeg` reports ffmpeg's own `-progress`
    /// `(elapsed_ms, total_duration_ms)` through the same channel once muxing
    /// starts producing output, rather than leaving the whole Converting
    /// phase unreported.
    Converting,
}

pub type ProgressTx = tokio::sync::mpsc::UnboundedSender<JobEvent>;

/// Sends a progress update if a channel was provided; silently does nothing
/// otherwise (most callers - CLI one-shot runs, tests - don't wire one up) and
/// never fails the download if the receiver has already been dropped.
pub fn report_progress(tx: Option<&ProgressTx>, done: u64, total: u64) {
    if let Some(tx) = tx {
        let _ = tx.send(JobEvent::Progress { done, total });
    }
}

/// Signals that muxing has started - see `JobEvent::Converting`.
pub fn report_converting(tx: Option<&ProgressTx>) {
    if let Some(tx) = tx {
        let _ = tx.send(JobEvent::Converting);
    }
}

/// Retries `f` up to `attempts` times with exponential backoff starting at
/// `initial_delay` (capped at 10s), returning the last error if every attempt
/// fails. Equivalent in purpose to XDM's `PieceGrabber` retry loop
/// (`Config.Instance.MaxRetry`/`RetryDelay`) - ported here as a small generic
/// helper rather than a global config singleton, since callers (HLS/DASH
/// segment fetches, progressive piece fetches) have different retry units.
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

/// Result of a HEAD probe: whether the server supports byte-range requests and,
/// if known, the resource's total size. Equivalent in purpose to XDM's
/// `PieceGrabber.Connect`/`CreateProbeResult` resumability check, but done via a
/// cheap HEAD instead of reading a partial GET response.
#[derive(Debug, Clone, Default)]
pub struct ProbeInfo {
    pub content_length: Option<u64>,
    pub accept_ranges: bool,
    pub content_type: Option<String>,
}

/// Thin HTTP client wrapper, the Rust equivalent of XDM.Core's `IHttpClient`.
/// Kept minimal for the HLS milestone: plain GET with optional headers/cookies
/// and an optional byte range.
#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    pub headers: HashMap<String, String>,
    pub cookies: Option<String>,
    /// Inclusive byte range, (offset, length) matching XDM's `HlsMediaSegment.ByteRange`.
    pub byte_range: Option<(u64, u64)>,
}

/// Headers/cookie captured once (e.g. from the page a media URL was detected
/// on: Referer/Origin/User-Agent/Cookie) and replayed on every request a
/// download makes - manifest, keys, and every segment/piece. Kept separate
/// from `RequestOptions` because a whole download shares one `RequestContext`
/// while each individual HTTP call still needs its own `byte_range`.
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

/// Summarizes what was actually sent on a failed request, without leaking
/// secret values (cookie/token contents) into logs or the GUI's error column -
/// just which header *names* were present. Turns an opaque "403 Forbidden"
/// into something diagnosable: e.g. a captured-headers download that's still
/// missing a real `Referer` (the page's Referrer-Policy may have stripped it
/// before the browser ever sent it - replaying a header that was never there
/// to begin with isn't fixable by us) shows up immediately instead of needing
/// a manual repro.
///
/// Mirrors `apply_headers`'s exclusion list rather than just listing
/// `opts.headers` raw - otherwise this reports headers that were captured but
/// never actually reach the wire (previously: `Sec-Fetch-*`/`Origin` kept
/// showing up here even after `apply_headers` was changed to strip them,
/// since this function never applied the same filtering - a real, misleading
/// bug during Cloudflare debugging). Doesn't list `User-Agent` unless one was
/// actually captured - the real value on the wire comes from `HttpClient`'s
/// Chrome emulation profile when nothing was, and this has no way to know
/// what that profile's current UA string is.
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

/// Header names that must never be replayed verbatim from a captured browser
/// request onto our own outgoing request:
/// - `accept-encoding`: reqwest only auto-decompresses gzip/brotli/deflate/zstd
///   responses when *it* set this header; if the caller supplies one (as we do,
///   replaying the real browser's `gzip, deflate, br, zstd`), reqwest sends it
///   as-is and leaves response decoding to the caller - which we don't do -
///   so a compressed response comes back as raw undecoded bytes (observed:
///   `get_text` returning garbled binary instead of an HLS playlist).
/// - `host`/`content-length`/`connection`: connection-management headers a
///   browser sets for its own TCP/TLS session; reqwest derives these itself
///   from the URL and body, and a stale replayed value can conflict with it.
/// - `cookie`: already applied explicitly from `RequestOptions.cookies` below;
///   skipped here so a cookie captured into both `headers` and `cookie` (as
///   the extension's `createRequestData` does) isn't sent twice.
/// - `sec-fetch-*`/`sec-gpc`/`origin`: fetch-metadata headers a real browser
///   sets automatically based on the exact navigation/fetch context of the
///   request that triggered it - values that are structurally impossible for
///   a standalone HTTP client to reproduce honestly (there is no real "site"
///   or "mode" here). Some bot-management (observed against a Cloudflare-
///   protected CDN) treats a request carrying these but backed by a
///   non-browser TLS/HTTP stack as *more* suspicious than one that doesn't
///   try to look like a browser at all - a working reference tool for the
///   same site (github.com/jeffmkw/Missav_ChromeTool) sends only
///   User-Agent/Referer/Cookie and succeeds. Replaying them was an attempt to
///   look more authentic; the evidence says it backfires.
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

/// Note on User-Agent: unlike the old reqwest-based client, this doesn't force
/// its own default here. `HttpClient::new`'s Chrome emulation profile supplies
/// an authentic, version-matched User-Agent (and everything else that goes
/// with it - the TLS/HTTP2 fingerprint) as part of impersonating a real
/// browser; a captured one (from the extension) below still overrides it when
/// present, since that's a real value from the page that detected this URL.
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
    /// Uses `wreq` (a `reqwest`-API-shaped client backed by a patched,
    /// browser-matching BoringSSL) with a Chrome emulation profile instead of
    /// plain `reqwest`/rustls. This is load-bearing, not cosmetic: some sites'
    /// bot-management (confirmed against a Cloudflare-protected CDN we
    /// otherwise could not get past at all - see the milestone history around
    /// "surrit.com") fingerprints the TLS handshake and HTTP/2 connection
    /// characteristics themselves (JA3/JA4-style), which no amount of
    /// faithfully replaying *headers* through a normal TLS stack can pass,
    /// since the check happens below the HTTP layer entirely. The emulation
    /// profile also supplies its own authentic, version-matched header set
    /// (User-Agent, `sec-ch-ua-*`, `sec-fetch-*`, `accept-encoding`, ...) as
    /// part of impersonating a real browser end-to-end; `apply_headers`
    /// replaces individual ones with real captured values when present
    /// (confirmed to replace rather than duplicate, unlike plain reqwest's
    /// client-level defaults) rather than adding its own fallback.
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .emulation(Emulation::Chrome137)
            .connect_timeout(Duration::from_secs(10))
            // A total request timeout (covers the whole response body being
            // read, not just headers) - 60s turned out too tight for a large
            // segment/piece over a slow CDN (observed: "operation timed out"
            // reading a segment's body well before it actually stalled).
            // Retries handle transient failures, but a legitimately-large,
            // legitimately-slow transfer shouldn't be treated as failed just
            // because of an arbitrary cap shorter than it needs.
            .timeout(Duration::from_secs(180))
            // wreq defaults to `redirect::Policy::none()` - unlike plain
            // reqwest, it does *not* follow redirects unless told to. Segment
            // URLs served through an intermediate decoy/obfuscation host
            // (observed: a `qooglecdn.com` URL 302-ing to the real CDN - the
            // same disguise pattern documented in `m3u8-guide.txt` for
            // hiding a segment's real origin) need this to resolve at all;
            // without it the 302 itself surfaced as a hard failure.
            .redirect(wreq::redirect::Policy::default())
            // Some CDNs (observed: TikTok's) silently reset a keep-alive
            // connection server-side while the client still considers it idle
            // and reusable, producing a repeatable "end of file before
            // message length reached" on the next request over that same
            // connection - retries alone don't help if they keep landing on
            // the same broken connection. Disabling pooling trades a little
            // latency (a fresh TCP+TLS handshake per request) for not getting
            // stuck reusing a connection the server already dropped.
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

    /// HEAD request to check resumability/size without downloading the body.
    pub async fn probe(&self, url: &str, opts: &RequestOptions) -> Result<ProbeInfo> {
        let req = apply_headers(self.client.head(url), opts);
        let resp = req.send().await.with_context(|| format!("HEAD {url} failed"))?;
        // `Response::content_length()` reflects the actual (empty) body size for a
        // HEAD response, not the `Content-Length` header value - read the header
        // directly to learn the resource's real size.
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

    /// Sends a GET (optionally range-restricted) and returns the raw response for
    /// the caller to stream and status-check itself - used by the progressive
    /// downloader, which needs to write bytes incrementally rather than buffer
    /// a whole piece in memory.
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
        // Unlike the old reqwest-based client, `apply_headers` no longer
        // forces a fallback User-Agent itself - `HttpClient::new`'s Chrome
        // emulation profile supplies an authentic, version-matched one at
        // send time instead (confirmed live: an explicit per-request
        // `.header("User-Agent", ...)` replaces that profile default rather
        // than duplicating it). A bare client with no emulation, as used in
        // these header-filtering-only tests, has no default to fall back to.
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
