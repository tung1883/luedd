# Lüdd

<img src="tidm-app/src-tauri/icons/128x128.png" alt="Lüdd logo" width="96" />

A download manager (Rust rewrite of XDM) with a desktop app, CLI, and browser
extension. Supports plain HTTP, HLS, and DASH downloads, plus resolving
Facebook/Instagram post URLs to their direct media links.

## Structure

- `crates/tidm-net` — HTTP client
- `crates/tidm-media` — HLS/DASH parsing, muxing, quality probing, social-site extractors
- `crates/tidm-core` — download queue, jobs, naming
- `crates/tidm-cli` — CLI
- `crates/tidm-ipc` — local server the browser extension talks to
- `tidm-app` — Tauri desktop app (`dist/` = frontend, `src-tauri/` = backend)
- `extension` / `extension-firefox` — browser extensions

## Build

Requires the GNU toolchain — the default MSVC toolchain doesn't link on this
machine.

```
$env:PATH = "C:\Program Files\Git\usr\bin;C:\mingw64\bin;C:\Program Files\CMake\bin;" + $env:PATH
cargo +stable-x86_64-pc-windows-gnu build --release --workspace
```

Binaries land in `target/release/` (`tidm-app.exe`, `tidm-cli.exe`).

## Test

```
cargo +stable-x86_64-pc-windows-gnu test --workspace
```

## Extension

Load `extension/` (Chrome) or `extension-firefox/` (Firefox) unpacked. Reload
after any JS/HTML change.
