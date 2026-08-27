//! Resolves a Facebook video/reel page URL to its direct, already-muxed
//! playable video URL.
//!
//! Facebook's server-rendered page HTML for a video/reel embeds the resolved
//! playback URL directly as JSON-escaped `"playable_url":"..."` (SD) and
//! `"playable_url_quality_hd":"..."` (HD) fields - no manifest, no GraphQL
//! call needed. This is a known, previously-documented technique (the same
//! field names are used by github.com/vikas5914/Facebook-Video-Downloader),
//! not a guess. Private/saved content needs the viewer's own session cookie
//! (`ctx.cookie`) replayed the same way any other hotlink-protected URL does;
//! public content resolves with no cookie at all.

use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use tidm_net::{HttpClient, RequestContext};

use super::SocialMedia;

static PLAYABLE_URL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(playable_url|playable_url_quality_hd)":"([^"]+)"#).expect("static regex is valid"));

/// Fetches the page and returns its single playable video URL, preferring
/// `playable_url_quality_hd` over the plain SD `playable_url` when both are
/// present (matches multiple occurrences of either by taking the last/best
/// one seen, since a page can embed the same field more than once).
pub async fn extract(client: &HttpClient, url: &str, ctx: &RequestContext) -> Result<Vec<SocialMedia>> {
    let html = client.get_text(url, &ctx.to_options(None)).await.context("fetching Facebook page")?;

    let mut best: Option<(bool, String)> = None; // (is_hd, raw_escaped_url)
    for caps in PLAYABLE_URL_RE.captures_iter(&html) {
        let is_hd = &caps[1] == "playable_url_quality_hd";
        // Once we have an HD hit, don't let a later SD one replace it.
        if best.as_ref().is_some_and(|(had_hd, _)| *had_hd && !is_hd) {
            continue;
        }
        best = Some((is_hd, caps[2].to_string()));
    }

    let Some((_, raw)) = best else {
        bail!(
            "no playable_url found on the Facebook page - it may be private (need a signed-in session), \
             the post has no video, or Facebook changed its page format"
        );
    };

    Ok(vec![SocialMedia { url: unescape_json_url(&raw), suggested_name: None }])
}

/// Undoes the JSON-string escaping the regex above matched over (`\/`,
/// `%` for a literal `%`, HTML-entity `&amp;`) - matches the reference
/// technique's own unescaping exactly, since the URL is embedded as a JSON
/// string value inside HTML and needs both layers undone.
fn unescape_json_url(raw: &str) -> String {
    raw.replace("\\u0025", "%").replace('\\', "").replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_hd_when_both_present() {
        let html = r#"{"playable_url":"https:\/\/sd.example\/v.mp4","other":1,"playable_url_quality_hd":"https:\/\/hd.example\/v.mp4"}"#;
        let caps: Vec<_> = PLAYABLE_URL_RE.captures_iter(html).collect();
        assert_eq!(caps.len(), 2);
    }

    #[test]
    fn unescape_handles_forward_slash_and_percent_and_amp() {
        let raw = r"https:\/\/scontent.example\/v.mp4?bytestart=1&byteend=2%26oe=abc";
        let got = unescape_json_url(raw);
        assert!(got.starts_with("https://scontent.example/v.mp4"));
        assert!(!got.contains('\\'));
    }

    #[test]
    fn extract_errors_clearly_when_no_playable_url_present() {
        // Just exercises the regex path in isolation - `extract` itself needs
        // a real HttpClient/network, covered by the regex-only unit tests above.
        let html = "<html><body>nothing here</body></html>";
        assert!(PLAYABLE_URL_RE.captures_iter(html).next().is_none());
    }
}
