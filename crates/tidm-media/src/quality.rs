
use anyhow::Result;
use tidm_net::{HttpClient, RequestContext};

use crate::dash::{self, Representation};
use crate::hls::{parse_master_playlist, HlsPlaylistContainer};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QualityOption {
    pub label: String,
    pub bandwidth: u64,
    pub resolution: Option<(u32, u32)>,
    pub variant_key: String,
}

pub fn hls_variant_key(container: &HlsPlaylistContainer) -> String {
    container.attributes.get("BANDWIDTH").cloned().unwrap_or_default()
}

pub fn dash_variant_key(video: Option<&Representation>, audio: Option<&Representation>) -> String {
    let bandwidth = video.map(|r| r.bandwidth).unwrap_or(0) + audio.map(|r| r.bandwidth).unwrap_or(0);
    match video {
        Some(v) if v.width > 0 && v.height > 0 => format!("{}x{}-{bandwidth}", v.width, v.height),
        _ => bandwidth.to_string(),
    }
}

pub async fn probe_hls_qualities(client: &HttpClient, url: &str, ctx: &RequestContext) -> Result<Vec<QualityOption>> {
    let text = client.get_text(url, &ctx.to_options(None)).await?;
    let lines: Vec<&str> = text.lines().collect();
    if !text.contains("#EXT-X-STREAM-INF") {
        return Ok(Vec::new());
    }
    let containers = parse_master_playlist(&lines, url)?;
    Ok(containers
        .iter()
        .map(|c| {
            let bandwidth = c.attributes.get("BANDWIDTH").and_then(|b| b.parse::<u64>().ok()).unwrap_or(0);
            let resolution = c.attributes.get("RESOLUTION").and_then(|r| {
                let (w, h) = r.split_once('x')?;
                Some((w.parse().ok()?, h.parse().ok()?))
            });
            QualityOption { label: c.quality(), bandwidth, resolution, variant_key: hls_variant_key(c) }
        })
        .collect())
}

pub async fn probe_dash_qualities(client: &HttpClient, url: &str, ctx: &RequestContext) -> Result<Vec<QualityOption>> {
    let manifest = client.get_text(url, &ctx.to_options(None)).await?;
    let periods = dash::parse(&manifest, url)?;
    Ok(periods
        .iter()
        .flatten()
        .map(|(video, audio)| {
            let bandwidth = (video.as_ref().map(|r| r.bandwidth).unwrap_or(0) + audio.as_ref().map(|r| r.bandwidth).unwrap_or(0)).max(0) as u64;
            let resolution = video.as_ref().filter(|v| v.width > 0 && v.height > 0).map(|v| (v.width as u32, v.height as u32));
            let label = match resolution {
                Some((w, h)) => format!("{w}x{h}"),
                None => format!("{bandwidth} bps"),
            };
            QualityOption { label, bandwidth, resolution, variant_key: dash_variant_key(video.as_ref(), audio.as_ref()) }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use url::Url;

    fn hls_container(bandwidth: &str, resolution: Option<&str>) -> HlsPlaylistContainer {
        let mut attributes = HashMap::new();
        attributes.insert("BANDWIDTH".to_string(), bandwidth.to_string());
        if let Some(res) = resolution {
            attributes.insert("RESOLUTION".to_string(), res.to_string());
        }
        HlsPlaylistContainer {
            video_playlist: Some(Url::parse("https://cdn.example/video.m3u8").unwrap()),
            audio_playlist: None,
            attributes,
        }
    }

    fn dash_representation(width: i32, height: i32, bandwidth: i64) -> Representation {
        Representation { segments: Vec::new(), width, height, codec: None, bandwidth, duration_ms: 0, mime_type: String::new(), language: None }
    }

    #[test]
    fn hls_variant_key_is_the_bandwidth_attribute_verbatim() {
        let container = hls_container("4200000", Some("1920x1080"));
        assert_eq!(hls_variant_key(&container), "4200000");
    }

    #[test]
    fn hls_variant_key_is_stable_across_two_separate_parses() {
        let a = hls_container("2100000", Some("1280x720"));
        let b = hls_container("2100000", Some("1280x720"));
        assert_eq!(hls_variant_key(&a), hls_variant_key(&b));
    }

    #[test]
    fn dash_variant_key_uses_resolution_and_bandwidth_when_video_has_real_dimensions() {
        let video = dash_representation(1920, 1080, 4_000_000);
        let audio = dash_representation(0, 0, 128_000);
        assert_eq!(dash_variant_key(Some(&video), Some(&audio)), "1920x1080-4128000");
    }

    #[test]
    fn dash_variant_key_falls_back_to_bandwidth_only_for_audio_only_pairing() {
        let audio = dash_representation(0, 0, 128_000);
        assert_eq!(dash_variant_key(None, Some(&audio)), "128000");
    }

    #[test]
    fn dash_variant_key_falls_back_to_bandwidth_only_when_video_has_no_real_dimensions() {
        let video = dash_representation(0, 0, 500_000);
        assert_eq!(dash_variant_key(Some(&video), None), "500000");
    }
}
