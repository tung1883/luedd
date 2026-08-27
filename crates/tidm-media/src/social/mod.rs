//! Extractors for sites that don't expose a real HLS/DASH manifest or a
//! direct downloadable link at all - the URL you're given is a *page*, and
//! the actual media URL is buried in that page's own HTML or an internal API
//! response. Each extractor turns "a page URL" into one or more direct,
//! immediately-downloadable media URLs, which the caller then treats exactly
//! like any other resolved `Http` download.
//!
//! Unlike HLS/DASH, these are unofficial and inherently fragile: the sites
//! involved actively change their internal structure, and the resolved CDN
//! URLs are typically short-lived (signed, expiring in hours) - extraction
//! should happen right before queuing a download, not far ahead of it.

pub mod facebook;
pub mod instagram;

use anyhow::Result;
use tidm_net::{HttpClient, RequestContext};

/// One directly-downloadable media URL resolved from a social site's page -
/// the video (or, for a carousel, one of several) a post/reel actually links to.
#[derive(Debug, Clone, PartialEq)]
pub struct SocialMedia {
    pub url: String,
    /// Best-effort filename hint (e.g. from the post's own alt text), when
    /// the site's response offers one - `None` falls back to the caller's
    /// usual URL/title-based naming.
    pub suggested_name: Option<String>,
}

/// Which extractor a URL should go through, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocialSite {
    Facebook,
    Instagram,
}

/// Best-effort sniff from the URL's host/path - matches broadly (any
/// `facebook.com`/`fb.watch` or `instagram.com` URL) since both extractors
/// themselves fail cleanly with a clear error on a URL that isn't actually a
/// post/reel/video (a profile page, a comment link, etc.), rather than
/// needing this to be a precise path matcher.
pub fn detect_site(url: &str) -> Option<SocialSite> {
    let lower = url.to_ascii_lowercase();
    if lower.contains("facebook.com/") || lower.contains("fb.watch/") {
        Some(SocialSite::Facebook)
    } else if lower.contains("instagram.com/") {
        Some(SocialSite::Instagram)
    } else {
        None
    }
}

/// Resolves a social-site page URL into one or more direct media URLs.
pub async fn extract(site: SocialSite, client: &HttpClient, url: &str, ctx: &RequestContext) -> Result<Vec<SocialMedia>> {
    match site {
        SocialSite::Facebook => facebook::extract(client, url, ctx).await,
        SocialSite::Instagram => instagram::extract(client, url, ctx).await,
    }
}
