//! Generates the native-messaging host manifest browsers require to launch
//! `tidm-nmhost`. Registering it (writing this file to the browser-specific
//! path, plus a registry key on Windows) is a system-wide, user-profile-level
//! change, so this module only *produces* the manifest content - actually
//! installing it is left to an explicit, user-run step.

use serde_json::{json, Value};

pub const HOST_NAME: &str = "com.tidm.nmhost";

/// Chrome/Edge/Firefox native messaging host manifest for `tidm-nmhost`.
/// `extension_id` is the packed extension's id (Chrome/Edge) or its
/// `browser_specific_settings.gecko.id` (Firefox); `allowed_origins`/
/// `allowed_extensions` differ by browser, so the caller picks the right key.
pub fn manifest(exe_path: &str, chrome_extension_id: Option<&str>, firefox_extension_id: Option<&str>) -> Value {
    let mut m = json!({
        "name": HOST_NAME,
        "description": "tidm native messaging host",
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
        let m = manifest("C:/tidm/tidm-nmhost.exe", Some("abcdefgh"), None);
        assert_eq!(m["name"], HOST_NAME);
        assert_eq!(m["type"], "stdio");
        assert_eq!(m["allowed_origins"][0], "chrome-extension://abcdefgh/");
        assert!(m.get("allowed_extensions").is_none());
    }

    #[test]
    fn builds_firefox_style_manifest() {
        let m = manifest("/usr/local/bin/tidm-nmhost", None, Some("tidm@example.com"));
        assert_eq!(m["allowed_extensions"][0], "tidm@example.com");
        assert!(m.get("allowed_origins").is_none());
    }
}
