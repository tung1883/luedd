# <img src="luedd-app/src-tauri/icons/128x128.png" alt="Lüdd Logo" width="32" align="left" /> Lüdd

*🇩🇪 Deutsch | [🇬🇧 English](README.en.md)*

Ein Download-Manager mit Desktop-App, CLI und Browser-Erweiterung.
Unterstützt einfache HTTP-, HLS- und DASH-Downloads.

## Struktur

- `crates/luedd-net` - HTTP-Client
- `crates/luedd-media` - HLS/DASH-Parsing, Muxing, Qualitätsabfrage
- `crates/luedd-core` - Download-Warteschlange, Jobs, Benennung
- `crates/luedd-cli` - CLI
- `crates/luedd-ipc` - lokaler Server, mit dem die Browser-Erweiterung kommuniziert
- `luedd-app` - Tauri-Desktop-App (`dist/` = Frontend, `src-tauri/` = Backend)
- `extension` / `extension-firefox` - Browser-Erweiterungen

## Erstellen

```
cargo +stable-x86_64-pc-windows-gnu build --release --workspace
```

Die Binaries landen in `target/release/` (`luedd-app.exe`, `luedd-cli.exe`).

## Testen

```
cargo +stable-x86_64-pc-windows-gnu test --workspace
```

## Instagram

Lüdd-Insta hat zwei Download-Engines: die eingebaute Engine („Lüdd-Insta Standard")
und [instaloader](https://instaloader.github.io/) (`pip install "instaloader>=4.14"`).
Unter Einstellungen → Lüdd-Insta wählt man eine **Haupt-Engine** und optional eine
**Ausweich-Engine** (wird genutzt, wenn die Haupt-Engine fehlschlägt). Beide
verwenden das vom Add-on erfasste `sessionid`-Cookie des Browsers oder das in den
Einstellungen hinterlegte Cookie.

## Extension

`extension/` (Chrome/Edge) oder `extension-firefox/` (Firefox) laden.
