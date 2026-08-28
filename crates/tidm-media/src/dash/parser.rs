use roxmltree::{Document, Node};
use thiserror::Error;
use url::Url;

use super::model::{PeriodPairings, Representation};
use super::xs_duration::parse_xs_duration;

const TRICK_MODE_URI: &str = "http://dashif.org/guidelines/trickmode";

#[derive(Debug, Error)]
pub enum DashParseError {
    #[error("XML parse error: {0}")]
    Xml(#[from] roxmltree::Error),
    #[error("missing MPD root element (found {0})")]
    MissingMpdRoot(String),
    #[error("manifest type=\"dynamic\" is not supported (live streams need a different addressing model)")]
    DynamicManifestUnsupported,
    #[error("encrypted manifest (ContentProtection present) is not supported")]
    EncryptedManifestUnsupported,
    #[error("no Period elements found in manifest")]
    NoPeriods,
    #[error("both period start and duration are missing")]
    MissingPeriodTiming,
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("invalid duration: {0}")]
    InvalidDuration(#[from] anyhow::Error),
    #[error("invalid numeric attribute {attr} on {tag}: {value}")]
    InvalidNumber { tag: &'static str, attr: &'static str, value: String },
}

fn child<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Option<Node<'a, 'input>> {
    node.children().find(|c| c.is_element() && c.tag_name().name() == name)
}

fn children<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Vec<Node<'a, 'input>> {
    node.children().filter(|c| c.is_element() && c.tag_name().name() == name).collect()
}

fn has_descendant(node: Node, name: &str) -> bool {
    node.descendants().any(|d| d.is_element() && d.tag_name().name() == name)
}

fn attr_inherited<'a>(node: Node<'a, '_>, name: &str) -> Option<&'a str> {
    node.attribute(name).or_else(|| node.parent_element().and_then(|p| p.attribute(name)))
}

fn parse_i32(v: Option<&str>, tag: &'static str, attr: &'static str) -> Result<i32, DashParseError> {
    match v {
        None => Ok(-1),
        Some(s) => s.parse().map_err(|_| DashParseError::InvalidNumber { tag, attr, value: s.to_string() }),
    }
}

fn parse_i64(v: Option<&str>, tag: &'static str, attr: &'static str) -> Result<i64, DashParseError> {
    match v {
        None => Ok(-1),
        Some(s) => s.parse().map_err(|_| DashParseError::InvalidNumber { tag, attr, value: s.to_string() }),
    }
}

pub fn parse(manifest_text: &str, playlist_url: &str) -> Result<Vec<PeriodPairings>, DashParseError> {
    let doc = Document::parse(manifest_text)?;
    let root = doc.root_element();
    if root.tag_name().name() != "MPD" {
        return Err(DashParseError::MissingMpdRoot(root.tag_name().name().to_string()));
    }
    if root.attribute("type") == Some("dynamic") {
        return Err(DashParseError::DynamicManifestUnsupported);
    }
    if has_descendant(root, "ContentProtection") {
        return Err(DashParseError::EncryptedManifestUnsupported);
    }

    let media_presentation_duration = root
        .attribute("mediaPresentationDuration")
        .map(parse_xs_duration)
        .transpose()?
        .unwrap_or(0);

    let mut base_url = Url::parse(playlist_url)?;
    if let Some(base_url_node) = child(root, "BaseURL") {
        base_url = base_url.join(base_url_node.text().unwrap_or("").trim())?;
    }

    let periods = children(root, "Period");
    if periods.is_empty() {
        return Err(DashParseError::NoPeriods);
    }

    let mut media_list = Vec::with_capacity(periods.len());
    if periods.len() > 1 {
        let durations = calculate_period_durations_if_missing(&periods, media_presentation_duration)?;
        for (period, duration) in periods.iter().zip(durations) {
            media_list.push(parse_period(*period, base_url.clone(), duration)?);
        }
    } else {
        media_list.push(parse_period(periods[0], base_url, media_presentation_duration)?);
    }

    Ok(media_list)
}

fn parse_period(period: Node, mut base_url: Url, media_presentation_duration: i64) -> Result<PeriodPairings, DashParseError> {
    let period_duration = match period.attribute("duration") {
        Some(d) => parse_xs_duration(d)?,
        None => media_presentation_duration,
    };
    if let Some(base_url_node) = child(period, "BaseURL") {
        base_url = base_url.join(base_url_node.text().unwrap_or("").trim())?;
    }

    let has_parent_segment_base = child(period, "SegmentBase").is_some();
    let mut audio_list = Vec::new();
    let mut video_list = Vec::new();

    for adaptation_set in children(period, "AdaptationSet") {
        let representations = parse_adaptation_set(adaptation_set, base_url.clone(), period_duration, has_parent_segment_base)?;
        if representations.is_empty() {
            continue;
        }
        if representations[0].mime_type.starts_with("audio") {
            audio_list.extend(representations);
        } else {
            video_list.extend(representations);
        }
    }

    let mut media_list = Vec::new();
    if !video_list.is_empty() && !audio_list.is_empty() {
        for video in &video_list {
            for audio in &audio_list {
                media_list.push((Some(video.clone()), Some(audio.clone())));
            }
        }
    } else if !video_list.is_empty() {
        for video in video_list {
            media_list.push((Some(video), None));
        }
    } else if !audio_list.is_empty() {
        for audio in audio_list {
            media_list.push((None, Some(audio)));
        }
    }

    Ok(media_list)
}

fn parse_adaptation_set(
    adaptation_set: Node,
    mut base_url: Url,
    period_duration: i64,
    has_parent_segment_base: bool,
) -> Result<Vec<Representation>, DashParseError> {
    if let Some(base_url_node) = child(adaptation_set, "BaseURL") {
        base_url = base_url.join(base_url_node.text().unwrap_or("").trim())?;
    }
    if contains_trick_mode(adaptation_set) {
        return Ok(Vec::new());
    }

    let mut representations = Vec::new();
    for xml_repr in children(adaptation_set, "Representation") {
        if let Some(repr) = parse_representation(xml_repr, base_url.clone(), period_duration, has_parent_segment_base)? {
            representations.push(repr);
        }
    }
    Ok(representations)
}

fn contains_trick_mode(adaptation_set: Node) -> bool {
    for tag in ["EssentialProperty", "SupplementalProperty"] {
        if let Some(node) = child(adaptation_set, tag) {
            if node.attribute("schemeIdUri") == Some(TRICK_MODE_URI) {
                return true;
            }
        }
    }
    false
}

fn parse_representation(
    xml_repr: Node,
    mut base_url: Url,
    period_duration: i64,
    has_parent_segment_base: bool,
) -> Result<Option<Representation>, DashParseError> {
    let mime_type = attr_inherited(xml_repr, "mimeType").unwrap_or("").to_lowercase();
    let width = parse_i32(attr_inherited(xml_repr, "width"), "Representation", "width")?;
    let height = parse_i32(attr_inherited(xml_repr, "height"), "Representation", "height")?;
    let bandwidth = parse_i64(attr_inherited(xml_repr, "bandwidth"), "Representation", "bandwidth")?;
    let codec = attr_inherited(xml_repr, "codecs").map(String::from);
    let lang = attr_inherited(xml_repr, "lang").map(String::from);

    if !mime_type.starts_with("audio") && !mime_type.starts_with("video") {
        return Ok(None);
    }

    if let Some(base_url_node) = child(xml_repr, "BaseURL") {
        base_url = base_url.join(base_url_node.text().unwrap_or("").trim())?;
    }

    let make = |segments: Vec<Url>| Representation {
        segments,
        width,
        height,
        codec: codec.clone(),
        bandwidth,
        duration_ms: period_duration,
        mime_type: mime_type.clone(),
        language: lang.clone(),
    };

    if child(xml_repr, "SegmentBase").is_some() || has_parent_segment_base {
        return Ok(Some(make(vec![base_url])));
    }

    if let Some(segment_list) = child(xml_repr, "SegmentList") {
        let mut segments = Vec::new();
        let init_node = child(segment_list, "Initialization").or_else(|| child(segment_list, "RepresentationIndex"));
        if let Some(init) = init_node {
            if let Some(source_url) = init.attribute("sourceURL") {
                segments.push(base_url.join(source_url)?);
            }
        }
        for segment_url_node in children(segment_list, "SegmentURL") {
            if let Some(media) = segment_url_node.attribute("media") {
                segments.push(base_url.join(media)?);
            }
        }
        return Ok(if segments.is_empty() { None } else { Some(make(segments)) });
    }

    let segment_template = child(xml_repr, "SegmentTemplate")
        .or_else(|| xml_repr.parent_element().and_then(|p| child(p, "SegmentTemplate")));
    if let Some(segment_template) = segment_template {
        let representation_id = xml_repr.attribute("id").unwrap_or_default();
        let bandwidth_str = attr_inherited(xml_repr, "bandwidth").unwrap_or("-1");

        if let Some(segment_timeline) = child(segment_template, "SegmentTimeline") {
            return parse_explicit_addressing(segment_template, segment_timeline, &base_url, representation_id, bandwidth_str)
                .map(|segs| segs.map(make));
        }
        return parse_simple_addressing(segment_template, &base_url, period_duration, representation_id, bandwidth_str)
            .map(|segs| segs.map(make));
    }

    Ok(None)
}

fn parse_simple_addressing(
    segment_template: Node,
    base_url: &Url,
    period_duration: i64,
    representation_id: &str,
    bandwidth: &str,
) -> Result<Option<Vec<Url>>, DashParseError> {
    let timescale: i64 = segment_template.attribute("timescale").unwrap_or("1").parse().unwrap_or(1);
    let duration: i64 = segment_template.attribute("duration").unwrap_or("1").parse().unwrap_or(1);
    let start_number: i64 = segment_template.attribute("startNumber").unwrap_or("1").parse().unwrap_or(1);
    let segment_count = ((period_duration as f64 / 1000.0) / (duration as f64 / timescale as f64)).ceil() as i64;

    let mut number = start_number;
    let mut time = start_number;

    let mut segments = Vec::with_capacity((segment_count + 1).max(0) as usize);

    if let Some(init_url) = segment_template.attribute("initialization") {
        let resolved = expand_template(init_url, number, time, bandwidth, representation_id);
        segments.push(base_url.join(&resolved)?);
    }

    let media_url = match segment_template.attribute("media") {
        Some(m) => m,
        None => return Ok(None),
    };

    for _ in 0..segment_count.max(0) {
        let resolved = expand_template(media_url, number, time, bandwidth, representation_id);
        segments.push(base_url.join(&resolved)?);
        number += 1;
        time += duration;
    }

    Ok(if segments.is_empty() { None } else { Some(segments) })
}

fn parse_explicit_addressing(
    segment_template: Node,
    segment_timeline: Node,
    base_url: &Url,
    representation_id: &str,
    bandwidth: &str,
) -> Result<Option<Vec<Url>>, DashParseError> {
    let ss = children(segment_timeline, "S");
    if ss.is_empty() {
        return Ok(None);
    }

    let mut number: i64 = segment_template.attribute("startNumber").unwrap_or("1").parse().unwrap_or(1);
    let mut time: i64 = 0;

    let mut segments = Vec::new();
    if let Some(init_url) = segment_template.attribute("initialization") {
        let resolved = expand_template(init_url, number, time, bandwidth, representation_id);
        segments.push(base_url.join(&resolved)?);
    }

    let media_url = match segment_template.attribute("media") {
        Some(m) => m,
        None => return Ok(None),
    };

    for s in ss {
        let d: i64 = s.attribute("d").ok_or_else(|| DashParseError::InvalidNumber {
            tag: "S",
            attr: "d",
            value: "missing".to_string(),
        })?.parse().map_err(|_| DashParseError::InvalidNumber { tag: "S", attr: "d", value: s.attribute("d").unwrap().to_string() })?;
        let t: i64 = s.attribute("t").and_then(|v| v.parse().ok()).unwrap_or(-1);
        let r: i64 = s.attribute("r").and_then(|v| v.parse().ok()).unwrap_or(-1);
        if t > 0 {
            time = t;
        }

        let resolved = expand_template(media_url, number, time, bandwidth, representation_id);
        segments.push(base_url.join(&resolved)?);
        number += 1;
        time += d;

        if r > 0 {
            for _ in 0..r {
                let resolved = expand_template(media_url, number, time, bandwidth, representation_id);
                segments.push(base_url.join(&resolved)?);
                number += 1;
                time += d;
            }
        }
    }

    Ok(if segments.is_empty() { None } else { Some(segments) })
}

fn expand_template(template: &str, number: i64, time: i64, bandwidth: &str, representation_id: &str) -> String {
    let protected = template.replace("$$", "\u{0}");
    let mut out = String::with_capacity(protected.len());
    let bytes = protected.as_bytes();

    let mut i = 0;
    while i < bytes.len() {
        if protected[i..].starts_with('$') {
            if let Some(end) = protected[i + 1..].find('$') {
                let token = &protected[i + 1..i + 1 + end];
                if let Some(expanded) = expand_token(token, number, time, bandwidth, representation_id) {
                    out.push_str(&expanded);
                    i = i + 1 + end + 1;
                    continue;
                }
            }
        }
        let ch = protected[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out.replace('\u{0}', "$")
}

fn expand_token(token: &str, number: i64, time: i64, bandwidth: &str, representation_id: &str) -> Option<String> {
    if token == "Number" {
        return Some(number.to_string());
    }
    if let Some(fmt) = token.strip_prefix("Number%0") {
        return Some(format_digit(number, fmt));
    }
    if token == "Time" {
        return Some(time.to_string());
    }
    if let Some(fmt) = token.strip_prefix("Time%0") {
        return Some(format_digit(time, fmt));
    }
    if token == "RepresentationID" {
        return Some(representation_id.to_string());
    }
    if token == "Bandwidth" {
        return Some(bandwidth.to_string());
    }
    None
}

fn format_digit(value: i64, fmt: &str) -> String {
    let (digits, radix) = match fmt.chars().last() {
        Some('d') | Some('D') => (&fmt[..fmt.len() - 1], 10),
        Some('x') | Some('X') => (&fmt[..fmt.len() - 1], 16),
        _ => (fmt, 10),
    };
    let width: usize = digits.parse().unwrap_or(0);
    match radix {
        16 => format!("{value:0width$x}"),
        _ => format!("{value:0width$}"),
    }
}

fn calculate_period_durations_if_missing(periods: &[Node], media_presentation_duration: i64) -> Result<Vec<i64>, DashParseError> {
    let mut list = vec![0i64; periods.len()];
    let mut last = media_presentation_duration;

    for i in (0..periods.len()).rev() {
        let node = periods[i];
        let sduration = node.attribute("duration");
        let sstart = node.attribute("start");
        if sstart.is_none() && sduration.is_none() {
            return Err(DashParseError::MissingPeriodTiming);
        }
        if let Some(sduration) = sduration {
            let duration = parse_xs_duration(sduration)?;
            list[i] = duration;
            last = media_presentation_duration - duration;
            continue;
        }
        let sstart = match sstart {
            Some(s) => s.to_string(),
            None if i == 0 => "PT0S".to_string(),
            None => return Err(DashParseError::MissingPeriodTiming),
        };
        let start = parse_xs_duration(&sstart)?;
        list[i] = last - start;
        last = start;
    }

    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://cdn.example/videos/manifest.mpd";

    #[test]
    fn rejects_dynamic_manifest() {
        let xml = r#"<MPD type="dynamic"><Period></Period></MPD>"#;
        let err = parse(xml, URL).unwrap_err();
        assert!(matches!(err, DashParseError::DynamicManifestUnsupported));
    }

    #[test]
    fn rejects_encrypted_manifest() {
        let xml = r#"<MPD><Period><AdaptationSet><ContentProtection/></AdaptationSet></Period></MPD>"#;
        let err = parse(xml, URL).unwrap_err();
        assert!(matches!(err, DashParseError::EncryptedManifestUnsupported));
    }

    #[test]
    fn parses_simple_addressing_video_and_audio() {
        let xml = r#"
        <MPD mediaPresentationDuration="PT10S">
          <Period>
            <AdaptationSet mimeType="video/mp4" width="1920" height="1080">
              <Representation id="v1" bandwidth="5000000" codecs="avc1">
                <SegmentTemplate timescale="1" duration="2" startNumber="1"
                    initialization="init-$RepresentationID$.m4s"
                    media="chunk-$RepresentationID$-$Number%03d$.m4s"/>
              </Representation>
            </AdaptationSet>
            <AdaptationSet mimeType="audio/mp4">
              <Representation id="a1" bandwidth="128000" codecs="mp4a">
                <SegmentTemplate timescale="1" duration="2" startNumber="1"
                    initialization="audio-init.m4s"
                    media="audio-$Number%03d$.m4s"/>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#;

        let periods = parse(xml, URL).unwrap();
        assert_eq!(periods.len(), 1);
        assert_eq!(periods[0].len(), 1);
        let (video, audio) = &periods[0][0];
        let video = video.as_ref().unwrap();
        let audio = audio.as_ref().unwrap();

        assert_eq!(video.width, 1920);
        assert_eq!(video.bandwidth, 5_000_000);
        assert_eq!(video.segments.len(), 6);
        assert_eq!(video.segments[0].as_str(), "https://cdn.example/videos/init-v1.m4s");
        assert_eq!(video.segments[1].as_str(), "https://cdn.example/videos/chunk-v1-001.m4s");
        assert_eq!(video.segments[5].as_str(), "https://cdn.example/videos/chunk-v1-005.m4s");

        assert_eq!(audio.mime_type, "audio/mp4");
        assert_eq!(audio.segments.len(), 6);
    }

    #[test]
    fn parses_explicit_addressing_with_repeat() {
        let xml = r#"
        <MPD mediaPresentationDuration="PT8S">
          <Period>
            <AdaptationSet mimeType="video/mp4">
              <Representation id="v1" bandwidth="1000000">
                <SegmentTemplate startNumber="1" media="seg-$Number$.m4s">
                  <SegmentTimeline>
                    <S t="0" d="2" r="3"/>
                  </SegmentTimeline>
                </SegmentTemplate>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#;

        let periods = parse(xml, URL).unwrap();
        let video = periods[0][0].0.as_ref().unwrap();
        assert_eq!(video.segments.len(), 4);
        assert_eq!(video.segments[0].as_str(), "https://cdn.example/videos/seg-1.m4s");
        assert_eq!(video.segments[3].as_str(), "https://cdn.example/videos/seg-4.m4s");
    }

    #[test]
    fn skips_trick_mode_adaptation_set() {
        let xml = r#"
        <MPD mediaPresentationDuration="PT4S">
          <Period>
            <AdaptationSet mimeType="video/mp4">
              <EssentialProperty schemeIdUri="http://dashif.org/guidelines/trickmode"/>
              <Representation id="v1" bandwidth="1000000">
                <SegmentTemplate startNumber="1" timescale="1" duration="2" media="seg-$Number$.m4s"/>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#;

        let periods = parse(xml, URL).unwrap();
        assert!(periods[0].is_empty());
    }

    #[test]
    fn expands_bandwidth_and_representation_id_tokens() {
        let resolved = expand_template("$RepresentationID$/$Bandwidth$/$Number%04d$.mp4", 7, 0, "500000", "v9");
        assert_eq!(resolved, "v9/500000/0007.mp4");
    }

    #[test]
    fn preserves_literal_dollar_signs() {
        let resolved = expand_template("price$$$Number$", 3, 0, "1", "v1");
        assert_eq!(resolved, "price$3");
    }
}
