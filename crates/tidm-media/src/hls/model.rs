use std::collections::HashMap;
use url::Url;

/// A single media segment in an HLS media playlist. Equivalent of `HlsMediaSegment`.
#[derive(Debug, Clone, PartialEq)]
pub struct HlsMediaSegment {
    pub url: Url,
    /// (offset, length), matching `HlsMediaSegment.ByteRange` in the original.
    pub byte_range: Option<(u64, u64)>,
    pub duration: f64,
    pub key_url: Option<Url>,
    /// 16-byte IV, either parsed from `IV=0x...` or derived from the media sequence number.
    pub iv: Option<[u8; 16]>,
    /// True for the `#EXT-X-MAP` init segment (zero-duration, must be prepended to output).
    pub is_init_segment: bool,
}

/// A parsed media (leaf) playlist. Equivalent of `HlsPlaylist`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HlsPlaylist {
    pub media_segments: Vec<HlsMediaSegment>,
    pub is_encrypted: bool,
    pub total_duration: f64,
    pub has_byte_range: bool,
    pub is_key_i_frame_only: bool,
}

/// One variant/rendition pairing extracted from a master playlist. Equivalent of
/// `HlsPlaylistContainer`.
#[derive(Debug, Clone, PartialEq)]
pub struct HlsPlaylistContainer {
    pub video_playlist: Option<Url>,
    pub audio_playlist: Option<Url>,
    pub attributes: HashMap<String, String>,
}

impl HlsPlaylistContainer {
    /// Best-effort human readable quality label, e.g. "1920x1080" or a bandwidth figure.
    pub fn quality(&self) -> String {
        if let Some(res) = self.attributes.get("RESOLUTION") {
            return res.clone();
        }
        if let Some(name) = self.attributes.get("NAME") {
            return name.clone();
        }
        if let Some(bw) = self.attributes.get("BANDWIDTH") {
            return format!("{bw} bps");
        }
        if let Some(lang) = self.attributes.get("LANGUAGE") {
            return lang.clone();
        }
        "unknown".to_string()
    }
}
