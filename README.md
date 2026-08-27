# <img src="tidm-app/src-tauri/icons/128x128.png" alt="Lüdd Logo" width="32" align="left" /> Lüdd

*🇩🇪 Deutsch | [🇬🇧 English](README.en.md)*

Ein Download-Manager mit Desktop-App, CLI und Browser-Erweiterung.
Unterstützt einfache HTTP-, HLS- und DASH-Downloads.

## Struktur

- `crates/tidm-net` — HTTP-Client
- `crates/tidm-media` — HLS/DASH-Parsing, Muxing, Qualitätsabfrage, Extraktoren für soziale Netzwerke
- `crates/tidm-core` — Download-Warteschlange, Jobs, Benennung
- `crates/tidm-cli` — CLI
- `crates/tidm-ipc` — lokaler Server, mit dem die Browser-Erweiterung kommuniziert
- `tidm-app` — Tauri-Desktop-App (`dist/` = Frontend, `src-tauri/` = Backend)
- `extension` / `extension-firefox` — Browser-Erweiterungen

## Erstellen

```
cargo +stable-x86_64-pc-windows-gnu build --release --workspace
```

Die Binaries landen in `target/release/` (`tidm-app.exe`, `tidm-cli.exe`).

## Testen

```
cargo +stable-x86_64-pc-windows-gnu test --workspace
```

## Erweiterung

`extension/` (Chrome/Edge) oder `extension-firefox/` (Firefox) laden.
