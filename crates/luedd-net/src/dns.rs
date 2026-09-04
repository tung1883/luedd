//! A DNS resolver that falls back to DNS-over-HTTPS.
//!
//! Some hosts (ad/stream CDNs behind privacy-hostile registrars, or names an
//! ISP's resolver simply doesn't carry) resolve in Chrome — which ships its own
//! DoH — but not through the OS stub resolver. `getaddrinfo` then returns
//! `WSAHOST_NOT_FOUND` and every Lüdd fetch/preview/download for that host dies
//! with "No such host is known".
//!
//! This resolver tries the system first (fast, unchanged for everything that
//! already works) and, only when that turns up nothing, asks Cloudflare's and
//! Google's DoH endpoints *by IP* (so the lookup itself needs no DNS). Results
//! are cached for 5 minutes.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wreq::dns::{Addrs, Name, Resolve, Resolving};

const CACHE_TTL: Duration = Duration::from_secs(300);

/// `(endpoint reachable by literal IP, query-name key)`. Both speak the
/// `application/dns-json` format.
const DOH_ENDPOINTS: &[&str] = &["https://1.1.1.1/dns-query", "https://8.8.8.8/resolve"];

struct Inner {
    /// Only ever connects to the literal DoH IPs above, so it needs no resolver
    /// of its own.
    http: wreq::Client,
    cache: Mutex<HashMap<String, (Vec<IpAddr>, Instant)>>,
}

#[derive(Clone)]
pub struct SystemThenDoh(Arc<Inner>);

impl SystemThenDoh {
    pub fn new() -> Self {
        let http = wreq::Client::builder()
            .timeout(Duration::from_secs(6))
            .build()
            .expect("build DoH client");
        Self(Arc::new(Inner { http, cache: Mutex::new(HashMap::new()) }))
    }
}

impl Default for SystemThenDoh {
    fn default() -> Self {
        Self::new()
    }
}

impl Inner {
    fn cached(&self, host: &str) -> Option<Vec<IpAddr>> {
        let cache = self.cache.lock().ok()?;
        let (ips, at) = cache.get(host)?;
        (at.elapsed() < CACHE_TTL).then(|| ips.clone())
    }

    async fn doh(&self, host: &str) -> Vec<IpAddr> {
        if let Some(ips) = self.cached(host) {
            return ips;
        }
        let mut ips = Vec::new();
        for endpoint in DOH_ENDPOINTS {
            for qtype in ["A", "AAAA"] {
                let url = format!("{endpoint}?name={host}&type={qtype}");
                let Ok(resp) = self.http.get(&url).header("accept", "application/dns-json").send().await
                else {
                    continue;
                };
                let Ok(text) = resp.text().await else { continue };
                let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
                if let Some(answers) = json.get("Answer").and_then(|a| a.as_array()) {
                    for a in answers {
                        if let Some(ip) =
                            a.get("data").and_then(|d| d.as_str()).and_then(|s| s.trim().parse::<IpAddr>().ok())
                        {
                            ips.push(ip);
                        }
                    }
                }
            }
            if !ips.is_empty() {
                break;
            }
        }
        if !ips.is_empty() {
            if let Ok(mut cache) = self.cache.lock() {
                cache.insert(host.to_string(), (ips.clone(), Instant::now()));
            }
            tracing::debug!(%host, count = ips.len(), "resolved via DoH (system resolver had nothing)");
        }
        ips
    }
}

impl Resolve for SystemThenDoh {
    fn resolve(&self, name: Name) -> Resolving {
        let inner = self.0.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            // 1. System resolver — fast, and unchanged for every host that
            //    already works.
            if let Ok(iter) = tokio::net::lookup_host((host.as_str(), 0)).await {
                let addrs: Vec<SocketAddr> = iter.collect();
                if !addrs.is_empty() {
                    return Ok(Box::new(addrs.into_iter()) as Addrs);
                }
            }
            // 2. DoH fallback.
            let ips = inner.doh(&host).await;
            if ips.is_empty() {
                return Err(format!("could not resolve {host} (system resolver and DoH both failed)").into());
            }
            let addrs: Vec<SocketAddr> = ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}
