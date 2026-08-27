use std::collections::HashMap;

use thiserror::Error;
use url::Url;

use super::attributes::parse_attributes;
use super::model::{HlsMediaSegment, HlsPlaylist, HlsPlaylistContainer};

#[derive(Debug, Error)]
pub enum HlsParseError {
    #[error("playlist does not start with #EXTM3U")]
    MissingSignature,
    #[error("unsupported EXT-X-KEY METHOD: {0}")]
    UnsupportedKeyMethod(String),
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("malformed tag {tag}: {detail}")]
    Malformed { tag: &'static str, detail: String },
}

/// Derive the AES-128 IV from the media sequence number when no explicit `IV=` attribute
/// is present, per RFC 8216 §5.2: a 16-byte (32 hex char) big-endian value, zero-padded.
///
/// XDM's original `HlsParser.cs` used `mediaSequence.ToString("X")` here with no
/// zero-padding, which produces a short IV for small sequence numbers and is not
/// spec-compliant. Fixed here rather than replicated.
pub fn iv_from_media_sequence(sequence: u64) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv[8..].copy_from_slice(&sequence.to_be_bytes());
    iv
}

fn parse_iv_attr(hex: &str) -> Option<[u8; 16]> {
    let hex = hex.strip_prefix("0x").or_else(|| hex.strip_prefix("0X")).unwrap_or(hex);
    if hex.len() != 32 {
        return None;
    }
    let mut iv = [0u8; 16];
    for i in 0..16 {
        iv[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(iv)
}

struct KeyState {
    url: Option<Url>,
    iv_attr: Option<[u8; 16]>,
    encrypted: bool,
}

/// Parses an HLS media (leaf) playlist: a flat list of segment URIs plus metadata tags.
/// Equivalent of `HlsParser.ParseMediaSegments`.
pub fn parse_media_playlist(lines: &[&str], base_url: &str) -> Result<HlsPlaylist, HlsParseError> {
    let base = Url::parse(base_url)?;

    let first_nonblank = lines.iter().map(|l| l.trim()).find(|l| !l.is_empty());
    if first_nonblank != Some("#EXTM3U") {
        return Err(HlsParseError::MissingSignature);
    }

    let mut playlist = HlsPlaylist::default();
    let mut media_sequence: u64 = 0;
    let mut pending_duration: f64 = 0.0;
    let mut pending_byte_range: Option<(u64, u64)> = None;
    let mut last_segment_end: u64 = 0;
    let mut key = KeyState { url: None, iv_attr: None, encrypted: false };

    for raw in lines {
        let line = raw.trim();
        if line.is_empty() || line == "#EXTM3U" {
            continue;
        }

        if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let dur_str = rest.split(',').next().unwrap_or("0");
            pending_duration = dur_str.trim().parse().unwrap_or(0.0);
        } else if let Some(rest) = line.strip_prefix("#EXT-X-BYTERANGE:") {
            playlist.has_byte_range = true;
            let (len_str, off_str) = match rest.split_once('@') {
                Some((l, o)) => (l, Some(o)),
                None => (rest, None),
            };
            let length: u64 = len_str.trim().parse().map_err(|_| HlsParseError::Malformed {
                tag: "EXT-X-BYTERANGE",
                detail: rest.to_string(),
            })?;
            let offset = match off_str {
                Some(o) => o.trim().parse().map_err(|_| HlsParseError::Malformed {
                    tag: "EXT-X-BYTERANGE",
                    detail: rest.to_string(),
                })?,
                None => last_segment_end,
            };
            pending_byte_range = Some((offset, length));
        } else if let Some(rest) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            media_sequence = rest.trim().parse().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("#EXT-X-KEY:") {
            let attrs = parse_attributes(rest);
            let method = attrs.get("METHOD").map(String::as_str).unwrap_or("NONE");
            match method {
                "NONE" => {
                    key = KeyState { url: None, iv_attr: None, encrypted: false };
                }
                "AES-128" => {
                    let keyformat = attrs.get("KEYFORMAT").map(String::as_str).unwrap_or("identity");
                    if keyformat != "identity" {
                        return Err(HlsParseError::UnsupportedKeyMethod(format!(
                            "AES-128 with KEYFORMAT={keyformat}"
                        )));
                    }
                    let key_url = attrs
                        .get("URI")
                        .map(|u| base.join(u))
                        .transpose()?;
                    let iv_attr = attrs.get("IV").and_then(|s| parse_iv_attr(s));
                    playlist.is_encrypted = true;
                    key = KeyState { url: key_url, iv_attr, encrypted: true };
                }
                other => return Err(HlsParseError::UnsupportedKeyMethod(other.to_string())),
            }
        } else if let Some(rest) = line.strip_prefix("#EXT-X-MAP:") {
            let attrs = parse_attributes(rest);
            let uri = attrs.get("URI").ok_or_else(|| HlsParseError::Malformed {
                tag: "EXT-X-MAP",
                detail: "missing URI".to_string(),
            })?;
            let url = base.join(uri)?;
            let byte_range = attrs.get("BYTERANGE").and_then(|br| {
                let (l, o) = br.split_once('@')?;
                Some((o.trim().parse().ok()?, l.trim().parse().ok()?))
            });
            let iv = if key.encrypted {
                Some(key.iv_attr.unwrap_or_else(|| iv_from_media_sequence(media_sequence)))
            } else {
                None
            };
            playlist.media_segments.push(HlsMediaSegment {
                url,
                byte_range,
                duration: 0.0,
                key_url: key.url.clone(),
                iv,
                is_init_segment: true,
            });
        } else if line == "#EXT-X-I-FRAMES-ONLY" {
            playlist.is_key_i_frame_only = true;
        } else if line.starts_with('#') {
            // Unrecognized tag (#EXT-X-TARGETDURATION, #EXT-X-ENDLIST, etc.) - ignored,
            // matching the original one-shot VOD parser's scope.
            continue;
        } else {
            // Segment URI line.
            let url = base.join(line)?;
            let iv = if key.encrypted {
                Some(key.iv_attr.unwrap_or_else(|| iv_from_media_sequence(media_sequence)))
            } else {
                None
            };
            let byte_range = pending_byte_range.take();
            if let Some((offset, length)) = byte_range {
                last_segment_end = offset + length;
            }
            playlist.media_segments.push(HlsMediaSegment {
                url,
                byte_range,
                duration: pending_duration,
                key_url: key.url.clone(),
                iv,
                is_init_segment: false,
            });
            playlist.total_duration += pending_duration;
            pending_duration = 0.0;
            media_sequence += 1;
        }
    }

    Ok(playlist)
}

/// Parses an HLS master playlist into per-variant video/audio playlist pairings.
/// Equivalent of `HlsParser.ParseMasterPlaylist`. Relies on positional coupling:
/// each `#EXT-X-STREAM-INF` line is followed by its URI line.
pub fn parse_master_playlist(
    lines: &[&str],
    base_url: &str,
) -> Result<Vec<HlsPlaylistContainer>, HlsParseError> {
    let base = Url::parse(base_url)?;

    let first_nonblank = lines.iter().map(|l| l.trim()).find(|l| !l.is_empty());
    if first_nonblank != Some("#EXTM3U") {
        return Err(HlsParseError::MissingSignature);
    }

    let mut media_renditions: Vec<HashMap<String, String>> = Vec::new();
    let mut stream_infs: Vec<HashMap<String, String>> = Vec::new();
    let mut urls: Vec<Url> = Vec::new();

    let mut pending_stream_inf: Option<HashMap<String, String>> = None;
    for raw in lines {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-MEDIA:") {
            media_renditions.push(parse_attributes(rest));
        } else if let Some(rest) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            pending_stream_inf = Some(parse_attributes(rest));
        } else if !line.starts_with('#') {
            if let Some(attrs) = pending_stream_inf.take() {
                stream_infs.push(attrs);
                urls.push(base.join(line)?);
            }
        }
    }

    let find_media_by_group = |group_id: &str, media_type: &str| -> Option<Url> {
        media_renditions.iter().find_map(|m| {
            if m.get("GROUP-ID").map(String::as_str) == Some(group_id)
                && m.get("TYPE").map(String::as_str) == Some(media_type)
            {
                m.get("URI").and_then(|u| base.join(u).ok())
            } else {
                None
            }
        })
    };

    let mut containers = Vec::with_capacity(urls.len());
    for (i, url) in urls.into_iter().enumerate() {
        let attrs = &stream_infs[i];
        let container = if let Some(audio_group) = attrs.get("AUDIO") {
            HlsPlaylistContainer {
                video_playlist: Some(url),
                audio_playlist: find_media_by_group(audio_group, "AUDIO"),
                attributes: attrs.clone(),
            }
        } else if let Some(video_group) = attrs.get("VIDEO") {
            HlsPlaylistContainer {
                video_playlist: find_media_by_group(video_group, "VIDEO"),
                audio_playlist: Some(url),
                attributes: attrs.clone(),
            }
        } else {
            HlsPlaylistContainer {
                video_playlist: Some(url),
                audio_playlist: None,
                attributes: attrs.clone(),
            }
        };
        containers.push(container);
    }

    Ok(containers)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://cdn.example/videos/stream.m3u8";

    #[test]
    fn rejects_missing_signature() {
        let lines = ["#EXTINF:6.0,", "seg1.ts"];
        let err = parse_media_playlist(&lines, BASE).unwrap_err();
        assert!(matches!(err, HlsParseError::MissingSignature));
    }

    #[test]
    fn parses_simple_media_playlist() {
        let text = "#EXTM3U\n#EXTINF:6.000000,\nseg_0001.ts\n#EXTINF:6.000000,\nseg_0002.ts\n";
        let lines: Vec<&str> = text.lines().collect();
        let playlist = parse_media_playlist(&lines, BASE).unwrap();
        assert_eq!(playlist.media_segments.len(), 2);
        assert_eq!(playlist.total_duration, 12.0);
        assert_eq!(
            playlist.media_segments[0].url.as_str(),
            "https://cdn.example/videos/seg_0001.ts"
        );
        assert!(!playlist.is_encrypted);
    }

    #[test]
    fn parses_disguised_segment_extensions_transparently() {
        // Segments named .png/.woff2/.txt should parse identically to .ts - the
        // parser never inspects extensions, only URIs.
        let text = "#EXTM3U\n#EXTINF:6.0,\nseg_0001.png\n#EXTINF:6.0,\nseg_0002.woff2\n#EXTINF:6.0,\nseg_0003.txt\n";
        let lines: Vec<&str> = text.lines().collect();
        let playlist = parse_media_playlist(&lines, BASE).unwrap();
        assert_eq!(playlist.media_segments.len(), 3);
    }

    #[test]
    fn parses_byte_range_with_implicit_offset() {
        let text = "#EXTM3U\n#EXTINF:6.0,\n#EXT-X-BYTERANGE:1000@0\nseg.ts\n#EXTINF:6.0,\n#EXT-X-BYTERANGE:2000\nseg.ts\n";
        let lines: Vec<&str> = text.lines().collect();
        let playlist = parse_media_playlist(&lines, BASE).unwrap();
        assert!(playlist.has_byte_range);
        assert_eq!(playlist.media_segments[0].byte_range, Some((0, 1000)));
        assert_eq!(playlist.media_segments[1].byte_range, Some((1000, 2000)));
    }

    #[test]
    fn parses_aes128_key_with_explicit_iv() {
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x0102030405060708090a0b0c0d0e0f10\n#EXTINF:6.0,\nseg.ts\n";
        let lines: Vec<&str> = text.lines().collect();
        let playlist = parse_media_playlist(&lines, BASE).unwrap();
        assert!(playlist.is_encrypted);
        let seg = &playlist.media_segments[0];
        assert_eq!(seg.key_url.as_ref().unwrap().as_str(), "https://cdn.example/videos/key.bin");
        assert_eq!(
            seg.iv.unwrap(),
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]
        );
    }

    #[test]
    fn derives_iv_from_media_sequence_when_absent_zero_padded() {
        let text = "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:6.0,\nseg.ts\n";
        let lines: Vec<&str> = text.lines().collect();
        let playlist = parse_media_playlist(&lines, BASE).unwrap();
        let iv = playlist.media_segments[0].iv.unwrap();
        // Zero-padded big-endian 5, NOT XDM's unpadded "5" hex string bug.
        let mut expected = [0u8; 16];
        expected[15] = 5;
        assert_eq!(iv, expected);
    }

    #[test]
    fn rejects_sample_aes() {
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key.bin\"\n#EXTINF:6.0,\nseg.ts\n";
        let lines: Vec<&str> = text.lines().collect();
        let err = parse_media_playlist(&lines, BASE).unwrap_err();
        assert!(matches!(err, HlsParseError::UnsupportedKeyMethod(_)));
    }

    #[test]
    fn parses_master_playlist_with_muxed_variants() {
        let text = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=1280x720\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1920x1080\nhigh.m3u8\n";
        let lines: Vec<&str> = text.lines().collect();
        let containers = parse_master_playlist(&lines, BASE).unwrap();
        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0].quality(), "1280x720");
        assert!(containers[0].audio_playlist.is_none());
    }

    #[test]
    fn parses_master_playlist_with_demuxed_audio_group() {
        let text = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud1\",URI=\"audio.m3u8\",LANGUAGE=\"en\"\n#EXT-X-STREAM-INF:BANDWIDTH=3000000,AUDIO=\"aud1\"\nvideo.m3u8\n";
        let lines: Vec<&str> = text.lines().collect();
        let containers = parse_master_playlist(&lines, BASE).unwrap();
        assert_eq!(containers.len(), 1);
        assert_eq!(
            containers[0].video_playlist.as_ref().unwrap().as_str(),
            "https://cdn.example/videos/video.m3u8"
        );
        assert_eq!(
            containers[0].audio_playlist.as_ref().unwrap().as_str(),
            "https://cdn.example/videos/audio.m3u8"
        );
    }
}
