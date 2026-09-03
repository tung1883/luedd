# <img src="luedd-app/src-tauri/icons/128x128.png" alt="Lüdd logo" width="32" align="left" /> Lüdd

*[🇩🇪 Deutsch](README.md) | 🇬🇧 English*

A download manager with a desktop app, CLI, and browser extension.
Supports plain HTTP, HLS, and DASH downloads

## Structure

- `crates/luedd-net` - HTTP client
- `crates/luedd-media` - HLS/DASH parsing, muxing, quality probing
- `crates/luedd-core` - download queue, jobs, naming
- `crates/luedd-cli` - CLI
- `crates/luedd-ipc` - local server the browser extension talks to
- `luedd-app` - Tauri desktop app (`dist/` = frontend, `src-tauri/` = backend)
- `extension` / `extension-firefox` - browser extensions

## Build

```
cargo +stable-x86_64-pc-windows-gnu build --release --workspace
```

Binaries land in `target/release/` (`luedd-app.exe`, `luedd-cli.exe`).

## Test

```
cargo +stable-x86_64-pc-windows-gnu test --workspace
```

## Instagram

Lüdd-Insta has two download engines: the built-in engine (“Lüdd-Insta Default”) and
[instaloader](https://instaloader.github.io/) (`pip install "instaloader>=4.14"`).
Under Settings → Lüdd-Insta you pick a **main** engine and an optional
**fallback** engine (tried when the main one fails). Both reuse the browser
`sessionid` cookie captured by the extension, or the session cookie set in
Settings.

## Extension

Load `extension/` (Chrome/Edge) or `extension-firefox/` (Firefox)
