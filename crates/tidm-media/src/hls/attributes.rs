use std::collections::HashMap;

pub fn parse_attributes(input: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut chars = input.chars().peekable();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut parts = Vec::new();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ',' if !in_quotes => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }

    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            let key = k.trim().to_string();
            let value = v.trim().trim_matches(|c| c == '"' || c == '\'' || c == ' ').to_string();
            map.insert(key, value);
        }
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_attributes() {
        let attrs = parse_attributes(r#"BANDWIDTH=1280000,RESOLUTION=1920x1080"#);
        assert_eq!(attrs.get("BANDWIDTH").unwrap(), "1280000");
        assert_eq!(attrs.get("RESOLUTION").unwrap(), "1920x1080");
    }

    #[test]
    fn respects_quoted_commas() {
        let attrs = parse_attributes(r#"NAME="720p, HQ",GROUP-ID="aud1""#);
        assert_eq!(attrs.get("NAME").unwrap(), "720p, HQ");
        assert_eq!(attrs.get("GROUP-ID").unwrap(), "aud1");
    }
}
