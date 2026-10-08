//! Luedd-JSON: find every downloadable asset URL inside a JSON document.
//!
//! The walk is pure (no I/O): feed it a parsed `serde_json::Value` and get back
//! the asset URLs with where they sit in the document (`$.albums[0].photos[1].url`),
//! a folder-ish group per parent object, and a coarse class for filtering.

use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;
use url::Url;

/// Hard stop so a pathological document can't flood the UI / queue.
pub const MAX_ASSETS: usize = 100_000;
const MAX_DEPTH: usize = 64;

const IMAGE: &[&str] = &["jpg", "jpeg", "png", "gif", "webp", "avif", "bmp", "svg", "tif", "tiff", "heic", "ico"];
const VIDEO: &[&str] = &["mp4", "webm", "mkv", "mov", "m4v", "avi", "flv", "ts", "m3u8", "mpd", "wmv", "3gp"];
const AUDIO: &[&str] = &["mp3", "m4a", "aac", "ogg", "opus", "wav", "flac", "weba", "wma"];
const OTHER: &[&str] = &[
    "pdf", "epub", "zip", "rar", "7z", "gz", "tar", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "csv", "apk",
    "exe", "dmg", "iso", "torrent",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetClass {
    Image,
    Video,
    Audio,
    Other,
}

#[derive(Debug, Clone, Serialize)]
pub struct Asset {
    pub url: String,
    /// File name taken from the URL (percent-decoded, query stripped).
    pub name: String,
    /// JSON path of the string value, `$.albums[0].photos[1].url`.
    pub path: String,
    /// Path of the parent object (array elements collapse to their array's owner).
    pub group: String,
    /// Folder derived from `group`: `albums_0/photos`. Empty at the root.
    pub folder: String,
    /// Value of the configured `name_key` on the nearest enclosing object.
    pub label: Option<String>,
    pub class: AssetClass,
    pub ext: Option<String>,
    pub host: String,
    /// Same URL already appeared earlier in the document.
    pub duplicate: bool,
    /// Had no recognised extension; kept only because "scan all strings" is on.
    pub guessed: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Resolves relative references (`/img/a.jpg`, `a.jpg`) and `//host/x`.
    pub base: Option<Url>,
    /// Also keep absolute http(s) URLs that have no known file extension.
    pub scan_all: bool,
    /// Object key whose value names the enclosing group (`title`, `name`...).
    pub name_key: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct ScanResult {
    pub assets: Vec<Asset>,
    /// Number of keys + array elements visited.
    pub nodes: usize,
    pub truncated: bool,
}

#[derive(Clone)]
enum Seg {
    Key(String),
    Idx(usize),
}

pub fn scan(root: &Value, opts: &ScanOptions) -> ScanResult {
    let mut w = Walker { opts, out: ScanResult::default(), seen: HashSet::new(), path: Vec::new(), labels: Vec::new() };
    w.visit(root, 0);
    w.out
}

struct Walker<'a> {
    opts: &'a ScanOptions,
    out: ScanResult,
    seen: HashSet<String>,
    path: Vec<Seg>,
    /// One slot per enclosing object: its `name_key` value, if any.
    labels: Vec<Option<String>>,
}

impl Walker<'_> {
    fn visit(&mut self, v: &Value, depth: usize) {
        if self.out.truncated || depth > MAX_DEPTH {
            return;
        }
        self.out.nodes += 1;
        match v {
            Value::String(s) => self.string(s),
            Value::Array(a) => {
                for (i, item) in a.iter().enumerate() {
                    self.path.push(Seg::Idx(i));
                    self.visit(item, depth + 1);
                    self.path.pop();
                }
            }
            Value::Object(m) => {
                let label = self.opts.name_key.as_deref().and_then(|k| match m.get(k) {
                    Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
                    Some(Value::Number(n)) => Some(n.to_string()),
                    _ => None,
                });
                self.labels.push(label);
                for (k, item) in m {
                    self.path.push(Seg::Key(k.clone()));
                    self.visit(item, depth + 1);
                    self.path.pop();
                }
                self.labels.pop();
            }
            _ => {}
        }
    }

    fn string(&mut self, raw: &str) {
        let Some((url, ext, guessed)) = candidate(raw, self.opts) else { return };
        if self.out.assets.len() >= MAX_ASSETS {
            self.out.truncated = true;
            return;
        }
        let class = classify(ext.as_deref());
        let parsed = Url::parse(&url).ok();
        let host = parsed.as_ref().and_then(|u| u.host_str()).unwrap_or("").to_string();
        let name = parsed.as_ref().map(file_name_of).unwrap_or_default();
        let key = url.split('#').next().unwrap_or(&url).to_string();
        let duplicate = !self.seen.insert(key);
        let group_segs = group_of(&self.path);
        let label = self.labels.iter().rev().find_map(|l| l.clone());
        self.out.assets.push(Asset {
            url,
            name,
            path: render_path(&self.path),
            group: render_path(group_segs),
            folder: folder_of(group_segs),
            label,
            class,
            ext,
            host,
            duplicate,
            guessed,
        });
    }
}

/// `(absolute url, lowercase extension, guessed)` when `raw` looks like an asset.
fn candidate(raw: &str, opts: &ScanOptions) -> Option<(String, Option<String>, bool)> {
    let s = raw.trim();
    if s.is_empty() || s.len() > 4096 || s.contains(char::is_whitespace) || s.starts_with("data:") || s.starts_with("blob:") {
        return None;
    }
    let absolute = s.starts_with("http://") || s.starts_with("https://");
    let url = if absolute {
        Url::parse(s).ok()?
    } else if let Some(rest) = s.strip_prefix("//") {
        Url::parse(&format!("https://{rest}")).ok()?
    } else {
        // relative reference: only worth keeping with a base AND a known extension
        let base = opts.base.as_ref()?;
        if s.contains("://") || s.starts_with("mailto:") || s.starts_with("javascript:") || s.starts_with('#') {
            return None;
        }
        let joined = base.join(s).ok()?;
        ext_of(&joined)?;
        joined
    };
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    match ext_of(&url) {
        Some(ext) => Some((url.to_string(), Some(ext), false)),
        None if absolute && opts.scan_all => Some((url.to_string(), None, true)),
        None => None,
    }
}

/// Known asset extension on the URL path, lowercase.
fn ext_of(url: &Url) -> Option<String> {
    let last = url.path_segments()?.next_back()?;
    let ext = last.rsplit_once('.')?.1.to_ascii_lowercase();
    let known = IMAGE.contains(&ext.as_str())
        || VIDEO.contains(&ext.as_str())
        || AUDIO.contains(&ext.as_str())
        || OTHER.contains(&ext.as_str());
    known.then_some(ext)
}

pub fn classify(ext: Option<&str>) -> AssetClass {
    match ext {
        Some(e) if IMAGE.contains(&e) => AssetClass::Image,
        Some(e) if VIDEO.contains(&e) => AssetClass::Video,
        Some(e) if AUDIO.contains(&e) => AssetClass::Audio,
        _ => AssetClass::Other,
    }
}

fn file_name_of(url: &Url) -> String {
    let last = url.path_segments().and_then(|mut s| s.next_back()).unwrap_or("");
    let decoded = percent_decode(last);
    decoded.trim().to_string()
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Some(v) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The object that owns the value at `path`. An array element collapses to the
/// object holding the array: `albums[0].photos[1].url` groups under `albums[0]`,
/// while `albums[0].video.stream` groups under `albums[0].video`.
fn group_of(path: &[Seg]) -> &[Seg] {
    let mut end = path.len().saturating_sub(1); // drop the leaf key / index
    // a leaf that sits directly in an array element: `...photos[1]` -> owner of `photos`
    if matches!(path.get(end.wrapping_sub(1)), Some(Seg::Idx(_))) && end > 0 {
        end -= 1; // drop the index
        if matches!(path.get(end.wrapping_sub(1)), Some(Seg::Key(_))) && end > 0 {
            end -= 1; // drop the array's key
        }
    }
    &path[..end]
}

pub fn render_path(path: &[Seg]) -> String {
    let mut s = String::from("$");
    for seg in path {
        match seg {
            Seg::Key(k) if k.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') && !k.is_empty() => {
                s.push('.');
                s.push_str(k);
            }
            Seg::Key(k) => {
                s.push_str("[\"");
                s.push_str(&k.replace('"', "\\\""));
                s.push_str("\"]");
            }
            Seg::Idx(i) => s.push_str(&format!("[{i}]")),
        }
    }
    s
}

/// `[albums, 0, photos]` -> `albums_0/photos`.
fn folder_of(group: &[Seg]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for seg in group {
        match seg {
            Seg::Key(k) => parts.push(k.clone()),
            Seg::Idx(i) => match parts.last_mut() {
                Some(last) => last.push_str(&format!("_{i}")),
                None => parts.push(i.to_string()),
            },
        }
    }
    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(v: Value) -> ScanResult {
        scan(&v, &ScanOptions::default())
    }

    #[test]
    fn finds_nested_assets_with_paths_and_classes() {
        let r = run(json!({
            "albums": [{
                "title": "Summer",
                "photos": [{"url": "https://cdn.x.com/a/IMG_1.jpg?sig=1"}, {"url": "https://cdn.x.com/a/IMG_2.PNG"}],
                "video": {"stream": "https://cdn.x.com/v/clip.mp4"}
            }],
            "credits": "https://cdn.x.com/credits.pdf",
            "page": "https://example.com/about"
        }));
        let urls: Vec<_> = r.assets.iter().map(|a| a.name.as_str()).collect();
        assert!(urls.contains(&"IMG_1.jpg") && urls.contains(&"IMG_2.PNG") && urls.contains(&"clip.mp4"));
        assert_eq!(r.assets.len(), 4, "page link has no asset extension: {urls:?}");
        let img = r.assets.iter().find(|a| a.name == "IMG_1.jpg").unwrap();
        assert_eq!(img.path, "$.albums[0].photos[0].url");
        assert_eq!(img.group, "$.albums[0]");
        assert_eq!(img.folder, "albums_0");
        assert_eq!(img.class, AssetClass::Image);
        let vid = r.assets.iter().find(|a| a.name == "clip.mp4").unwrap();
        assert_eq!(vid.group, "$.albums[0].video");
        assert_eq!(vid.folder, "albums_0/video");
        assert_eq!(vid.class, AssetClass::Video);
        let pdf = r.assets.iter().find(|a| a.name == "credits.pdf").unwrap();
        assert_eq!(pdf.group, "$");
        assert_eq!(pdf.folder, "");
        assert_eq!(pdf.class, AssetClass::Other);
    }

    #[test]
    fn marks_repeated_urls_as_duplicates() {
        let r = run(json!({"a": "https://x.com/1.jpg", "b": "https://x.com/1.jpg#frag", "c": "https://x.com/2.jpg"}));
        assert_eq!(r.assets.len(), 3);
        assert_eq!(r.assets.iter().filter(|a| a.duplicate).count(), 1);
    }

    #[test]
    fn relative_urls_need_a_base_and_a_known_extension() {
        let v = json!({"a": "/img/p.jpg", "b": "img/q.png", "c": "//cdn.x.com/r.gif", "d": "/about", "e": "hello.world"});
        assert_eq!(run(v.clone()).assets.len(), 1, "only the protocol-relative one without a base");
        let opts = ScanOptions { base: Url::parse("https://site.com/api/data.json").ok(), ..Default::default() };
        let r = scan(&v, &opts);
        let mut urls: Vec<_> = r.assets.iter().map(|a| a.url.as_str()).collect();
        urls.sort();
        assert_eq!(urls, ["https://cdn.x.com/r.gif", "https://site.com/api/img/q.png", "https://site.com/img/p.jpg"]);
    }

    #[test]
    fn scan_all_keeps_extensionless_absolute_urls_as_guessed() {
        let v = json!({"u": "https://cdn.x.com/files/12345", "w": "not a url", "d": "data:image/png;base64,AAAA"});
        assert!(run(v.clone()).assets.is_empty());
        let r = scan(&v, &ScanOptions { scan_all: true, ..Default::default() });
        assert_eq!(r.assets.len(), 1);
        assert!(r.assets[0].guessed);
        assert_eq!(r.assets[0].class, AssetClass::Other);
    }

    #[test]
    fn root_array_and_name_key_label() {
        let v = json!([{"title": "One", "img": "https://x.com/a.jpg"}, {"title": "Two", "img": "https://x.com/b.jpg"}]);
        let r = scan(&v, &ScanOptions { name_key: Some("title".into()), ..Default::default() });
        assert_eq!(r.assets[0].path, "$[0].img");
        assert_eq!(r.assets[0].group, "$");
        assert_eq!(r.assets[0].label.as_deref(), Some("One"));
        assert_eq!(r.assets[1].label.as_deref(), Some("Two"));
    }

    #[test]
    fn decodes_percent_encoded_file_names() {
        let r = run(json!({"u": "https://x.com/my%20pic%2B1.jpg"}));
        assert_eq!(r.assets[0].name, "my pic+1.jpg");
    }

    #[test]
    fn odd_keys_use_bracket_paths_and_depth_is_bounded() {
        let r = run(json!({"my key": {"u": "https://x.com/a.jpg"}}));
        assert_eq!(r.assets[0].path, "$[\"my key\"].u");
        let mut deep = json!("https://x.com/deep.jpg");
        for _ in 0..200 {
            deep = json!([deep]);
        }
        assert!(run(deep).assets.is_empty(), "past MAX_DEPTH is ignored, not a stack overflow");
    }
}
