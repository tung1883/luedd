//! Resolves an Instagram post/reel/carousel page URL to its direct media
//! URL(s) via Instagram's own internal (undocumented, unofficial) persisted
//! GraphQL API - the same API the web client itself uses, replayed directly
//! rather than scraped from a live browser request.
//!
//! This is fragile by nature and worth stating plainly: `DOC_ID_SHORTCODE_MEDIA`
//! is a build artifact of Instagram's frontend, not a stable public API, and
//! *will* eventually stop resolving without notice - when it does, this fails
//! with a clear "expired" error rather than silently returning garbage, and
//! the id needs refreshing from a live Instagram session (DevTools -> Network
//! -> filter `graphql`) rather than anything this code can rediscover itself.
//! Resolved CDN URLs are signed and expire in hours - extraction should
//! happen right before queuing, not far ahead of it.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tidm_net::{HttpClient, RequestContext};

use super::SocialMedia;

/// Relay persisted-document id for the single post/reel/carousel query
/// (`data.xdt_shortcode_media`). Observed 2025-2026; expect it to rot -
/// see the module doc comment.
const DOC_ID_SHORTCODE_MEDIA: &str = "24368985919464652";

/// Stable, publicly-known app id Instagram's own web client sends on every
/// request - without it the API returns a logged-out/empty response even
/// with a valid session cookie. Not a secret; the same value appears in
/// every Instagram web client's own JS bundle.
const IG_APP_ID: &str = "936619743392459";

/// Pulls the post/reel/tv shortcode out of the URL's own path - the only
/// thing needed to build the query, no page fetch required first.
fn extract_shortcode(url: &str) -> Option<String> {
    for marker in ["/p/", "/reel/", "/tv/"] {
        if let Some(idx) = url.find(marker) {
            let rest = &url[idx + marker.len()..];
            let code = rest.split(['/', '?', '#']).next()?;
            if !code.is_empty() {
                return Some(code.to_string());
            }
        }
    }
    None
}

pub async fn extract(client: &HttpClient, url: &str, ctx: &RequestContext) -> Result<Vec<SocialMedia>> {
    let shortcode = extract_shortcode(url)
        .with_context(|| format!("could not find a /p/, /reel/, or /tv/ shortcode in {url}"))?;

    let variables = serde_json::json!({
        "shortcode": shortcode,
        "fetch_tagged_user_count": null,
        "hoisted_comment_id": null,
        "hoisted_reply_id": null,
    });
    let mut query_url = url::Url::parse("https://www.instagram.com/graphql/query/").expect("static URL is valid");
    query_url
        .query_pairs_mut()
        .append_pair("doc_id", DOC_ID_SHORTCODE_MEDIA)
        .append_pair("variables", &variables.to_string());

    let mut opts = ctx.to_options(None);
    opts.headers.insert("x-ig-app-id".to_string(), IG_APP_ID.to_string());
    // A referer matching the post's own profile page reduces the chance of a
    // bot-challenge on story/reel fetches - best-effort, not required for
    // public posts.
    opts.headers.entry("referer".to_string()).or_insert_with(|| "https://www.instagram.com/".to_string());

    let body = client.get_text(query_url.as_str(), &opts).await.context("fetching Instagram GraphQL response")?;
    let json: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "Instagram GraphQL response wasn't JSON (likely a login/challenge page - \
             session cookie missing, stale, or this post needs a follower relationship): {}",
            body.chars().take(200).collect::<String>()
        )
    })?;

    if json.get("errors").is_some() {
        bail!(
            "Instagram GraphQL query was rejected (doc_id={DOC_ID_SHORTCODE_MEDIA} has likely expired and needs \
             refreshing from a live Instagram session): {}",
            json["errors"]
        );
    }

    let media = json
        .pointer("/data/xdt_shortcode_media")
        .context("Instagram response had no xdt_shortcode_media - private post, deleted, or the response schema changed")?;

    extract_media_urls(media)
}

/// Handles both current (`xdt_`-prefixed) and legacy Instagram response
/// schemas, per the reference doc's field table: a carousel iterates its
/// children (each itself either a video or an image); a bare node is either
/// a single video (`video_url`/`video_versions[]`) or a single image
/// (largest of `display_resources[]`/`image_versions2.candidates[]`).
fn extract_media_urls(media: &Value) -> Result<Vec<SocialMedia>> {
    if let Some(children) = media.pointer("/edge_sidecar_to_children/edges").and_then(|v| v.as_array()) {
        let items: Vec<SocialMedia> =
            children.iter().filter_map(|edge| edge.get("node")).filter_map(single_item_media).collect();
        if items.is_empty() {
            bail!("carousel post had no downloadable video/image items");
        }
        return Ok(items);
    }

    single_item_media(media).map(|m| vec![m]).context("post had neither a video nor an image URL")
}

fn single_item_media(node: &Value) -> Option<SocialMedia> {
    let is_video = node.get("is_video").and_then(|v| v.as_bool()).unwrap_or(false)
        || node.get("media_type").and_then(|v| v.as_i64()) == Some(2);

    if is_video {
        if let Some(url) = node.get("video_url").and_then(|v| v.as_str()) {
            return Some(SocialMedia { url: url.to_string(), suggested_name: None });
        }
        if let Some(url) = node.pointer("/video_versions/0/url").and_then(|v| v.as_str()) {
            return Some(SocialMedia { url: url.to_string(), suggested_name: None });
        }
    }

    // Largest image: index 0 of either field is the largest candidate/resource.
    if let Some(url) = node.pointer("/display_resources/0/src").and_then(|v| v.as_str()) {
        return Some(SocialMedia { url: url.to_string(), suggested_name: None });
    }
    if let Some(url) = node.pointer("/image_versions2/candidates/0/url").and_then(|v| v.as_str()) {
        return Some(SocialMedia { url: url.to_string(), suggested_name: None });
    }
    node.get("display_url").and_then(|v| v.as_str()).map(|url| SocialMedia { url: url.to_string(), suggested_name: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_shortcode_handles_all_three_path_kinds() {
        assert_eq!(extract_shortcode("https://www.instagram.com/p/ABC123/").as_deref(), Some("ABC123"));
        assert_eq!(extract_shortcode("https://www.instagram.com/reel/XYZ789/?igsh=1").as_deref(), Some("XYZ789"));
        assert_eq!(extract_shortcode("https://www.instagram.com/tv/DEF456").as_deref(), Some("DEF456"));
    }

    #[test]
    fn extract_shortcode_none_for_a_profile_url() {
        assert_eq!(extract_shortcode("https://www.instagram.com/someuser/"), None);
    }

    #[test]
    fn single_video_prefers_video_url_field() {
        let node = serde_json::json!({ "is_video": true, "video_url": "https://cdn.example/v.mp4" });
        let media = single_item_media(&node).unwrap();
        assert_eq!(media.url, "https://cdn.example/v.mp4");
    }

    #[test]
    fn single_video_falls_back_to_video_versions() {
        let node = serde_json::json!({ "media_type": 2, "video_versions": [{"url": "https://cdn.example/v2.mp4"}] });
        let media = single_item_media(&node).unwrap();
        assert_eq!(media.url, "https://cdn.example/v2.mp4");
    }

    #[test]
    fn single_image_prefers_largest_display_resource() {
        let node = serde_json::json!({
            "is_video": false,
            "display_resources": [{"src": "https://cdn.example/large.jpg"}, {"src": "https://cdn.example/small.jpg"}]
        });
        let media = single_item_media(&node).unwrap();
        assert_eq!(media.url, "https://cdn.example/large.jpg");
    }

    #[test]
    fn carousel_extracts_every_child() {
        let media = serde_json::json!({
            "edge_sidecar_to_children": {
                "edges": [
                    { "node": { "is_video": true, "video_url": "https://cdn.example/1.mp4" } },
                    { "node": { "is_video": false, "display_resources": [{"src": "https://cdn.example/2.jpg"}] } },
                ]
            }
        });
        let items = extract_media_urls(&media).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].url, "https://cdn.example/1.mp4");
        assert_eq!(items[1].url, "https://cdn.example/2.jpg");
    }

    #[test]
    fn errors_cleanly_when_nothing_resolvable() {
        let media = serde_json::json!({ "some_other_field": true });
        assert!(extract_media_urls(&media).is_err());
    }
}
