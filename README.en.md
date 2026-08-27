# <img src="tidm-app/src-tauri/icons/128x128.png" alt="Lüdd logo" width="32" align="left" /> Lüdd

*[🇩🇪 Deutsch](README.md) | 🇬🇧 English*

A download manager with a desktop app, CLI, and browser extension.
Supports plain HTTP, HLS, and DASH downloads

## Structure

- `crates/tidm-net` — HTTP client
- `crates/tidm-media` — HLS/DASH parsing, muxing, quality probing, social-site extractors
- `crates/tidm-core` — download queue, jobs, naming
- `crates/tidm-cli` — CLI
- `crates/tidm-ipc` — local server the browser extension talks to
- `tidm-app` — Tauri desktop app (`dist/` = frontend, `src-tauri/` = backend)
- `extension` / `extension-firefox` — browser extensions

## Build

```
cargo +stable-x86_64-pc-windows-gnu build --release --workspace
```

Binaries land in `target/release/` (`tidm-app.exe`, `tidm-cli.exe`).

## Test

```
cargo +stable-x86_64-pc-windows-gnu test --workspace
```

## Extension

Load `extension/` (Chrome/Edge) or `extension-firefox/` (Firefox)
