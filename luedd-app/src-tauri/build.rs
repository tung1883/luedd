fn main() {
    // Force the resource bundle (../dist) to be re-embedded whenever a frontend
    // file changes. tauri_build watches the dist directory, but an in-place edit
    // to a file inside it doesn't always change the directory mtime on Windows,
    // so cargo can skip this build script and ship a stale libresource.a.
    for entry in [
        "../dist",
        "../dist/index.html",
        "../dist/detected.html",
        "../dist/viewer.html",
        "../dist/i18n.js",
        "../dist/fonts",
    ] {
        println!("cargo:rerun-if-changed={entry}");
    }
    // A per-build token appended to window URLs so a rebuilt frontend is never
    // served from the WebView2 HTTP cache under an unchanged URL.
    let build_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=LUEDD_ASSET_VER={build_id}");
    tauri_build::build()
}
