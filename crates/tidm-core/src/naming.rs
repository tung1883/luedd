//! Filename/extension inference shared by every entry-creation path (browser
//! extension, GUI "Add", CLI). The raw source URL is often a meaningless CDN
//! path or token and frequently omits or misstates the real file type (e.g. a
//! URL ending in a bare `m` that's actually an `.mp4`), so this combines the
//! page title (when the browser extension supplied one) with a type detected
//! straight from the server's response instead of trusting the URL alone -
//! the same idea as XDM's page-title-based auto-naming.

use std::path::{Path, PathBuf};

use tidm_net::{HttpClient, RequestContext};

/// Extensions we already trust from the URL/filename alone - if the current
/// name ends in one of these there's no ambiguity worth spending a network
/// round-trip resolving. Kept deliberately broad so detection only kicks in
/// for the genuinely unclear cases (missing extension, or something we don't
/// recognize at all). Deliberately excludes `.txt`: streaming sites disguise
/// HLS/DASH manifests behind generic extensions specifically to evade naive
/// "is this a video?" detection (see `m3u8-guide.txt`), and `.txt` is exactly
/// the disguise observed in practice - so it always gets sniffed instead.
const TRUSTED_EXTS: &[&str] = &[
    "mp4", "mkv", "webm", "mov", "avi", "flv", "ts", "m3u8", "mpd", "mp3", "m4a", "aac", "wav", "ogg", "flac", "pdf",
    "zip", "rar", "7z", "jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "doc", "docx", "xls", "xlsx", "ppt",
    "pptx", "csv", "json", "exe", "msi", "dmg", "apk", "srt", "vtt",
];

/// Maps a `Content-Type` value (ignoring charset/params) to a file extension.
pub fn extension_for_mime(mime: &str) -> Option<&'static str> {
    let base = mime.split(';').next().unwrap_or(mime).trim().to_ascii_lowercase();
    Some(match base.as_str() {
        "video/mp4" | "video/x-m4v" => "mp4",
        "video/webm" => "webm",
        "video/x-matroska" => "mkv",
        "video/quicktime" => "mov",
        "video/x-msvideo" => "avi",
        "video/x-flv" => "flv",
        "video/mp2t" => "ts",
        "audio/mpeg" => "mp3",
        "audio/mp4" | "audio/x-m4a" => "m4a",
        "audio/aac" => "aac",
        "audio/wav" | "audio/x-wav" | "audio/wave" => "wav",
        "audio/ogg" => "ogg",
        "audio/flac" | "audio/x-flac" => "flac",
        "application/pdf" => "pdf",
        "application/zip" => "zip",
        "application/x-rar-compressed" | "application/vnd.rar" => "rar",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/bmp" => "bmp",
        "image/svg+xml" => "svg",
        "application/vnd.apple.mpegurl" | "application/x-mpegurl" => "m3u8",
        "application/dash+xml" => "mpd",
        _ => return None,
    })
}

/// Sniffs a real file type from the first bytes of a response body via known
/// magic-number signatures - a last resort for servers that send a generic or
/// missing `Content-Type` (`application/octet-stream`, or nothing at all).
pub fn sniff_extension_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    // HLS/DASH manifests are plain text, not binary, but they're exactly the
    // thing streaming sites like to disguise behind a generic extension
    // (`.txt`, `.dat`, ...) - #EXTM3U is mandated as the literal first line of
    // any valid HLS playlist, and a DASH MPD's root element appears within the
    // first few hundred bytes right after the XML declaration.
    if bytes.starts_with(b"#EXTM3U") {
        return Some("m3u8");
    }
    if bytes[..bytes.len().min(512)].windows(4).any(|w| w == b"<MPD") {
        return Some("mpd");
    }
    if bytes.len() >= 8 && &bytes[4..8] == b"ftyp" {
        return Some("mp4");
    }
    if bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        // EBML container; webm and mkv share this header, webm is the common web case.
        return Some("webm");
    }
    if bytes.starts_with(b"%PDF-") {
        return Some("pdf");
    }
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("png");
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("jpg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("webp");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WAVE" {
        return Some("wav");
    }
    if bytes.len() >= 11 && bytes.starts_with(b"RIFF") && &bytes[8..11] == b"AVI" {
        return Some("avi");
    }
    if bytes.starts_with(b"OggS") {
        return Some("ogg");
    }
    if bytes.starts_with(b"fLaC") {
        return Some("flac");
    }
    if bytes.starts_with(b"ID3") || (bytes.len() >= 2 && bytes[0] == 0xFF && (bytes[1] & 0xE0) == 0xE0) {
        return Some("mp3");
    }
    if bytes.starts_with(b"PK\x03\x04") {
        return Some("zip");
    }
    if bytes.len() >= 189 && bytes[0] == 0x47 && bytes[188] == 0x47 {
        return Some("ts");
    }
    None
}

fn current_extension(name: &str) -> String {
    Path::new(name).extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase()
}

/// Best-effort detection of the real file extension for `url`, skipped
/// entirely when the URL/filename already ends in something we already trust
/// (no need to spend a request resolving what isn't ambiguous). Tries the
/// server's declared `Content-Type` first (from the same GET used for
/// sniffing below, not a separate HEAD - some manifest-disguising servers
/// respond differently to HEAD than GET), then falls back to sniffing
/// whatever arrives in the first few
/// KB via magic numbers/text signatures - safe regardless of the resource's
/// real size (even an unbounded live stream), since the response is dropped
/// (canceling the underlying connection) the moment enough bytes are in hand,
/// rather than ever waiting for or buffering the whole body. Returns `None`
/// if nothing conclusive was found; the caller should keep whatever extension
/// it already had.
pub async fn resolve_real_extension(client: &HttpClient, url_or_filename: &str, ctx: &RequestContext) -> Option<String> {
    if TRUSTED_EXTS.contains(&current_extension(url_or_filename).as_str()) {
        return None;
    }

    let opts = ctx.to_options(None);
    let response = client.get_response(url_or_filename, &opts).await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    if let Some(ct) = response.headers().get("content-type").and_then(|v| v.to_str().ok()) {
        if let Some(ext) = extension_for_mime(ct) {
            return Some(ext.to_string());
        }
    }

    const SAMPLE_BYTES: usize = 4096;
    let mut buf = Vec::with_capacity(SAMPLE_BYTES);
    let mut stream = response.bytes_stream();
    while buf.len() < SAMPLE_BYTES {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            _ => break,
        }
    }
    sniff_extension_from_bytes(&buf).map(str::to_string)
}

/// Decodes the handful of HTML entities that sometimes leak into
/// `document.title` (observed: `&#39;` showing up literally in a saved
/// filename instead of `'`) - some pages set their title via `innerHTML`
/// without decoding it first, or a template escapes it for HTML embedding
/// and never unescapes it back into plain text. Covers the standard named
/// entities plus numeric character references (`&#39;`, `&#x27;`); anything
/// unrecognized is left as-is rather than guessed at or dropped.
fn decode_html_entities(input: &str) -> String {
    if !input.contains('&') {
        return input.to_string();
    }
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input.as_bytes()[i] == b'&' {
            if let Some(semi) = input[i..].find(';').filter(|&o| o >= 2 && o <= 10) {
                let entity = &input[i + 1..i + semi];
                if let Some(decoded) = decode_one_html_entity(entity) {
                    out.push(decoded);
                    i += semi + 1;
                    continue;
                }
            }
        }
        let ch = input[i..].chars().next().expect("i is a valid char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn decode_one_html_entity(entity: &str) -> Option<char> {
    match entity {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        "nbsp" => Some(' '),
        _ => {
            if let Some(hex) = entity.strip_prefix("#x").or_else(|| entity.strip_prefix("#X")) {
                u32::from_str_radix(hex, 16).ok().and_then(char::from_u32)
            } else if let Some(dec) = entity.strip_prefix('#') {
                dec.parse::<u32>().ok().and_then(char::from_u32)
            } else {
                None
            }
        }
    }
}

/// Strips characters illegal in Windows/Unix filenames and collapses
/// whitespace, so a page title like `Some Show: Episode 4 | Streaming*Site`
/// becomes a usable filename component.
pub fn sanitize_filename_component(input: &str) -> String {
    let input = decode_html_entities(input);
    let cleaned: String = input
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => ' ',
            c if (c as u32) < 0x20 => ' ',
            c => c,
        })
        .collect();
    let mut s = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    s = s.trim_matches('.').trim().to_string();
    if s.chars().count() > 150 {
        s = s.chars().take(150).collect();
    }
    s
}

/// Last-path-segment fallback naming, used only when there's no page title to
/// work with (a manual GUI/CLI "Add", or a browser tab the extension couldn't
/// read a title for).
pub fn filename_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.rsplit('/').next().filter(|s| !s.is_empty()).unwrap_or("download").to_string()
}

/// Builds a filename the way XDM does: prefer the page/tab title over the raw
/// (often meaningless CDN) URL path, paired with whatever real extension was
/// determined. A detected extension always wins over the URL's own claimed
/// one, since the URL's extension is exactly what can't be trusted here.
pub fn suggest_filename(title: Option<&str>, url: &str, detected_ext: Option<&str>) -> String {
    let url_derived = filename_from_url(url);
    let url_ext = current_extension(&url_derived);

    let base = title.map(sanitize_filename_component).filter(|s| !s.is_empty()).unwrap_or_else(|| {
        Path::new(&url_derived).file_stem().and_then(|s| s.to_str()).unwrap_or("download").to_string()
    });

    match detected_ext.filter(|e| !e.is_empty()).or_else(|| if url_ext.is_empty() { None } else { Some(url_ext.as_str()) }) {
        Some(ext) => format!("{base}.{ext}"),
        None => base,
    }
}

/// Short, non-cryptographic hash (FNV-1a, 8 hex chars) - not for security,
/// just to guarantee two downloads never silently collide/overwrite each
/// other on disk (nothing that writes a fresh download's first bytes checks
/// path existence beforehand).
fn short_hash(input: &str) -> String {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in input.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

/// Final on-disk destination for a new download: flat inside `download_dir`,
/// with `filename`'s stem suffixed by a short hash. The hash is over `url`
/// *plus* the moment this particular download was added (nanosecond-precision
/// wall clock) rather than the URL alone, so every individual download gets
/// its own hash - re-adding the exact same URL a second time is a distinct
/// download with its own path, not one that silently collides with or
/// resumes into the first. Only called once per new download (the result is
/// persisted on the entry, never recomputed), so this being time-dependent
/// doesn't affect retry - a retry reuses the already-stored `dest` untouched.
/// Composes cleanly with `jobs::sanitize_dest_for_kind`, which only ever
/// rewrites the extension via `.with_extension(...)` - the hashed stem passes
/// through untouched.
pub fn dest_path(download_dir: &Path, url: &str, filename: &str) -> PathBuf {
    let now_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let hash = short_hash(&format!("{url}#{now_ns}"));
    let path = Path::new(filename);
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("download");
    let hashed_filename = match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => format!("{stem}-{hash}.{ext}"),
        None => format!("{stem}-{hash}"),
    };
    download_dir.join(hashed_filename)
}

/// Every download's intermediate/temp artifacts (resumable-download state,
/// HLS/DASH segments, ffmpeg mux inputs) live in one dedicated cache folder
/// per download, derived from `dest`'s own filename (which already contains
/// the hash from `dest_path`) rather than the URL - so nothing downstream
/// needs the URL again, and a retry (which never changes `dest`) naturally
/// finds the same cache dir a prior failed attempt left behind. Deleted once
/// the finished file has been moved into place at `dest`.
pub fn cache_dir_for(dest: &Path) -> PathBuf {
    let stem = dest.file_stem().and_then(|s| s.to_str()).unwrap_or("download");
    dest.parent().unwrap_or_else(|| Path::new(".")).join(".tidm-cache").join(stem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_for_mime_covers_common_containers() {
        assert_eq!(extension_for_mime("video/mp4; codecs=avc1"), Some("mp4"));
        assert_eq!(extension_for_mime("application/pdf"), Some("pdf"));
        assert_eq!(extension_for_mime("image/jpeg"), Some("jpg"));
        assert_eq!(extension_for_mime("application/octet-stream"), None);
    }

    #[test]
    fn sniffs_hls_manifest_disguised_behind_a_generic_extension() {
        let manifest = b"#EXTM3U\n#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=1264239\nindex-v1-a1.txt\n";
        assert_eq!(sniff_extension_from_bytes(manifest), Some("m3u8"));
        assert!(!TRUSTED_EXTS.contains(&"txt"), "txt must always be sniffed, not trusted outright");
    }

    #[test]
    fn sniffs_dash_manifest_from_mpd_root_element() {
        let manifest = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\">";
        assert_eq!(sniff_extension_from_bytes(manifest), Some("mpd"));
    }

    #[test]
    fn sniffs_mp4_from_ftyp_box() {
        let mut bytes = vec![0, 0, 0, 24];
        bytes.extend_from_slice(b"ftypisom");
        assert_eq!(sniff_extension_from_bytes(&bytes), Some("mp4"));
    }

    #[test]
    fn sniffs_pdf_and_png_signatures() {
        assert_eq!(sniff_extension_from_bytes(b"%PDF-1.7 rest of file"), Some("pdf"));
        let png = [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0];
        assert_eq!(sniff_extension_from_bytes(&png), Some("png"));
    }

    #[test]
    fn sniff_returns_none_for_unrecognized_bytes() {
        assert_eq!(sniff_extension_from_bytes(b"just some plain text"), None);
    }

    #[test]
    fn sanitize_filename_component_strips_illegal_characters_and_trims() {
        assert_eq!(sanitize_filename_component("Some Show: Episode 4 | Site*Name"), "Some Show Episode 4 Site Name");
        assert_eq!(sanitize_filename_component("  spaced   out  "), "spaced out");
    }

    #[test]
    fn sanitize_filename_component_decodes_html_entities() {
        assert_eq!(sanitize_filename_component("Who Hadn&#39;t Had"), "Who Hadn't Had");
        assert_eq!(sanitize_filename_component("Who Hadn&#x27;t Had"), "Who Hadn't Had");
        assert_eq!(sanitize_filename_component("Tom &amp; Jerry"), "Tom & Jerry");
        // "&" alone (not part of a real entity) must be left alone, not eaten.
        assert_eq!(sanitize_filename_component("R&D Update"), "R&D Update");
    }

    #[test]
    fn suggest_filename_prefers_title_over_url_and_detected_ext_over_url_ext() {
        assert_eq!(
            suggest_filename(Some("My Cool Video"), "https://cdn.example/abc123.jpg", Some("mp4")),
            "My Cool Video.mp4"
        );
    }

    #[test]
    fn suggest_filename_falls_back_to_url_when_no_title() {
        assert_eq!(suggest_filename(None, "https://cdn.example/movie.mkv", None), "movie.mkv");
    }

    #[test]
    fn suggest_filename_uses_detected_ext_when_url_has_none() {
        assert_eq!(suggest_filename(None, "https://cdn.example/videos/29whtyqk8y", Some("mp4")), "29whtyqk8y.mp4");
    }

    #[test]
    fn dest_path_gives_each_download_its_own_hash_even_for_the_same_url() {
        let dir = Path::new("downloads");
        let a = dest_path(dir, "https://cdn.example/video.m3u8", "My Cool Video.mp4");
        let b = dest_path(dir, "https://cdn.example/video.m3u8", "My Cool Video.mp4");
        assert_ne!(a, b, "re-adding the same URL must be a distinct download, not collide with/resume the first");
    }

    #[test]
    fn dest_path_different_urls_never_collide_even_with_identical_titles() {
        let dir = Path::new("downloads");
        let a = dest_path(dir, "https://cdn.example/a.m3u8", "video.mp4");
        let b = dest_path(dir, "https://cdn.example/b.m3u8", "video.mp4");
        assert_ne!(a, b, "different source URLs must never resolve to the same on-disk path");
    }

    #[test]
    fn dest_path_is_flat_and_keeps_extension_after_the_hash() {
        let dir = Path::new("downloads");
        let dest = dest_path(dir, "https://cdn.example/video.m3u8", "My Cool Video.mp4");
        assert_eq!(dest.parent().unwrap(), Path::new("downloads"));
        assert_eq!(dest.extension().and_then(|e| e.to_str()), Some("mp4"));
        let stem = dest.file_stem().and_then(|s| s.to_str()).unwrap();
        assert!(stem.starts_with("My Cool Video-"), "stem was {stem:?}");
    }

    #[test]
    fn cache_dir_for_is_derived_from_dests_own_hashed_stem() {
        let dir = Path::new("downloads");
        let dest = dest_path(dir, "https://cdn.example/video.m3u8", "My Cool Video.mp4");
        let cache_dir = cache_dir_for(&dest);
        assert_eq!(cache_dir.parent().unwrap(), Path::new("downloads/.tidm-cache"));
        assert_eq!(cache_dir.file_name().and_then(|s| s.to_str()), dest.file_stem().and_then(|s| s.to_str()));
    }

    #[test]
    fn cache_dir_for_is_stable_across_retries_since_dest_never_changes() {
        let dir = Path::new("downloads");
        let dest = dest_path(dir, "https://cdn.example/video.m3u8", "My Cool Video.mp4");
        assert_eq!(cache_dir_for(&dest), cache_dir_for(&dest));
    }
}
