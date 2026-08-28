
use serde_json::{json, Value};

pub const HOST_NAME: &str = "com.luedd.nmhost";

pub fn manifest(exe_path: &str, chrome_extension_id: Option<&str>, firefox_extension_id: Option<&str>) -> Value {
    let mut m = json!({
        "name": HOST_NAME,
        "description": "Lüdd native messaging host",
        "path": exe_path,
        "type": "stdio",
    });

    if let Some(id) = chrome_extension_id {
        m["allowed_origins"] = json!([format!("chrome-extension://{id}/")]);
    }
    if let Some(id) = firefox_extension_id {
        m["allowed_extensions"] = json!([id]);
    }

    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_chrome_style_manifest() {
        let m = manifest("C:/luedd/luedd-nmhost.exe", Some("abcdefgh"), None);
        assert_eq!(m["name"], HOST_NAME);
        assert_eq!(m["type"], "stdio");
        assert_eq!(m["allowed_origins"][0], "chrome-extension://abcdefgh/");
        assert!(m.get("allowed_extensions").is_none());
    }

    #[test]
    fn builds_firefox_style_manifest() {
        let m = manifest("/usr/local/bin/luedd-nmhost", None, Some("luedd@example.com"));
        assert_eq!(m["allowed_extensions"][0], "luedd@example.com");
        assert!(m.get("allowed_origins").is_none());
    }
}
