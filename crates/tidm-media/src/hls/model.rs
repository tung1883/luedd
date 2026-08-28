use std::collections::HashMap;
use url::Url;

#[derive(Debug, Clone, PartialEq)]
pub struct HlsMediaSegment {
    pub url: Url,
    pub byte_range: Option<(u64, u64)>,
    pub duration: f64,
    pub key_url: Option<Url>,
    pub iv: Option<[u8; 16]>,
    pub is_init_segment: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HlsPlaylist {
    pub media_segments: Vec<HlsMediaSegment>,
    pub is_encrypted: bool,
    pub total_duration: f64,
    pub has_byte_range: bool,
    pub is_key_i_frame_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HlsPlaylistContainer {
    pub video_playlist: Option<Url>,
    pub audio_playlist: Option<Url>,
    pub attributes: HashMap<String, String>,
}

impl HlsPlaylistContainer {
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
