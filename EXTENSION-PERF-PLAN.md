# Plan: fix browser-extension performance (Firefox/Chrome slowdown)

## Context

Users report the browser (Firefox especially) becoming sluggish while the Lüdd
integration extension is installed. The extension is a media-detection helper: it
watches network requests, matches likely media URLs, shows them in the toolbar
popup, and POSTs them to the desktop app's local HTTP server
(`http://127.0.0.1:8597`) which does the downloading.

There are **two near-identical copies** of the extension that must be kept in
sync:

- `extension/` — Chrome, MV3, `background.service_worker = main.js`
- `extension-firefox/` — Firefox, MV3, `background.scripts = [main.js]`, module

Shared source files are **byte-identical** between the two folders except:
- `app.js` — Firefox guards `chrome.downloads.onDeterminingFilename` with an
  `if` (Chrome calls it unconditionally). Keep that difference.
- `manifest.json` — background key + `browser_specific_settings`. Keep.

`request-watcher.js`, `connector.js`, `logger.js`, `popup.js`, `main.js` are
identical — **every edit below must be applied to both copies verbatim.**

### Root causes (verified against current code)

1. **webRequest listeners always registered, never torn down.**
   `request-watcher.js:173 register()` adds `chrome.webRequest.onSendHeaders` +
   `onHeadersReceived` on `["http://*/*","https://*/*"]` with
   `requestHeaders`/`responseHeaders` (+ `extraHeaders` where supported). The
   browser marshals the headers of **every request in every tab** into the
   extension for the whole session. `unRegister()` (`request-watcher.js:208`)
   exists but is never called — the popup enable/disable toggle
   (`app.js:325` `type === "cmd"`) only flips an in-memory `userDisabled` bool
   and never removes the listeners.

2. **`requestMap` leak.** `onSendHeaders` (`request-watcher.js:120-126`) stores
   every GET request keyed by `requestId`; entries are deleted only in
   `onHeadersReceived` / `onErrorOccurred`. Requests that fire neither (served
   from cache, some aborts, navigations away) leak permanently → unbounded Map
   growth across a browsing session.

3. **HLS/DASH segment storm.** `DEFAULT_MEDIA_EXTS` (`request-watcher.js:8`)
   contains `.TS`; `.m4s` and bare segment URLs match via `Content-Type`
   `video/…`. One HLS video = hundreds of unique segment URLs per minute, each
   treated as a brand-new detection.

4. **Unbounded lists + O(n²).** Every detection runs
   `recordLocalDetection` (`app.js:127`) which does `this.videoList.some(v => v.url === data.url)`
   (linear scan) then pushes to `this.videoList` and `this.pendingMedia`. Under
   the segment storm `n` grows into the thousands → quadratic scans, plus
   `updateActionIcon` (`app.js:240`) rewriting the badge/icon on every hit.

5. **Fires even when the app is closed.** `onRequestDataReceived` (`app.js:142`)
   only checks `isDetectionEnabled()` (= `!userDisabled`). It does **not** check
   `appEnabled` or `connector.isConnected()`. With the desktop app shut, every
   detection still: records locally, grows `pendingMedia`, and POSTs `/media`
   (which fails). `pendingMedia` then retries 5/tick forever
   (`app.js:62-71`).

6. **Every `/media` POST response re-runs a full sync.**
   `connector.postMessage` (`connector.js:50`) pipes **all** responses through
   `onResponse` → `onMessage` (`app.js:41`), which re-runs `updateConfig`, the
   `pendingMedia` retry loop, and `updateActionIcon`. So each detected segment
   triggers a full reconcile.

7. **Verbose logging always on.** `logger.js:4` `this.loggingEnabled = true`;
   `app.js` / `request-watcher.js` `console.log` whole request and data objects
   on every request.

8. **`userDisabled` not persisted.** It lives only in memory (`app.js:14`). On a
   Chrome MV3 service-worker restart (happens constantly) it resets to `false`
   → monitoring silently re-enables itself.

---

## Changes

### A. Gate webRequest registration on actual monitoring state

**`request-watcher.js`**
- Add an idempotent guard so `register()` / `unRegister()` are safe to call
  repeatedly: track `this.registered` (bool); `register()` returns early if
  already registered, `unRegister()` returns early if not, each flips the flag.
- Add a `sweepRequestMap()` helper (see B) — not registration related, listed
  here for proximity.

**`app.js`**
- Add `syncWatcherRegistration()`:
  ```js
  syncWatcherRegistration() {
    if (this.isMonitoringEnabled()) this.requestWatcher.register();
    else this.requestWatcher.unRegister();
  }
  ```
- In `register()` (`app.js:197`) **stop** calling `this.requestWatcher.register()`
  unconditionally. Call `this.syncWatcherRegistration()` instead, after config
  is known.
- Call `syncWatcherRegistration()` from every place monitoring state can change:
  - end of `onMessage` (`app.js:78`, after `updateActionIcon`) — covers
    `appEnabled` and connection coming/going.
  - `onDisconnect` (`app.js:98`).
  - `onPopupMessage` `type === "cmd"` branch (`app.js:325`), after setting
    `userDisabled`.
- Keep `onDeterminingFilename` / `onTabUpdate` gated as they already are
  (`isMonitoringEnabled()` checks stay).

Result: with the desktop app closed or monitoring off, the extension registers
**zero** webRequest listeners and imposes no per-request cost.

### B. Bound `requestMap`

**`request-watcher.js`**
- In `onSendHeadersEvent` (`:120`), after `this.requestMap.set(...)`, cap size:
  if `this.requestMap.size > 1000`, delete oldest entries (Map keeps insertion
  order) down to ~800.
- Add a periodic sweep: store `info` with a timestamp; on each
  `onSendHeadersEvent` (cheap, already hot) drop entries older than 60s. Or run
  a `chrome.alarms` sweep every 1 min. Prefer the inline check to avoid another
  alarm.

### C. Never treat stream segments as detections

**`request-watcher.js`**
- Add module-level:
  ```js
  const SEGMENT_EXTS = ['.TS', '.M4S'];
  const SEGMENT_PATH_RE = /(?:^|\/)(?:seg(?:ment)?|chunk|frag(?:ment)?)[-_]?\d+/i;
  ```
- In `isMatchingRequest` (`:61`), immediately return `false` when the upper-cased
  path ends with a `SEGMENT_EXTS` entry, or `SEGMENT_PATH_RE.test(u.pathname)`,
  or the `Content-Type` header is `video/mp2t` / `application/octet-stream` on a
  numbered path. Do this **before** the `mediaExts` / `mediaTypes` checks.
- Leave `.M3U8` / `.MPD` (manifests) and progressive files (`.MP4`, `.MKV`, …)
  matching as today.
- Note: `mediaExts` is server-driven (`app.js:72` passes
  `mediaExts: msg.requestFileExts`), so also strip `ts`/`m4s` from the server's
  default request-ext list — see "Server-side note" below. The client filter is
  the real guard; the server change is hygiene.

### D. Bound the lists, drop the O(n) scan

**`app.js`**
- Add `this.seenUrls = new Set()` in the constructor. In `recordLocalDetection`
  (`:127`) replace `this.videoList.some(v => v.url === data.url)` with
  `this.seenUrls.has(data.url)`; add the url to the set when recording.
- Cap `this.videoList`: after pushing, if `length > 300`, slice to the last 300.
  Keep `seenUrls` from unbounded growth by rebuilding it from `videoList` +
  `pendingMedia` keys whenever you trim (or cap it at 2000 and clear on trim).
- Cap `this.pendingMedia`: it's a Map; if `size > 200` delete oldest keys down to
  ~150 before inserting.
- In `onMessage` (`:50`) where `localOnly` is computed, also cap that slice.

### E. Only record / POST when monitoring is actually on

**`app.js`**
- `onRequestDataReceived` (`:142`): change the guard from
  `if (!this.isDetectionEnabled()) return;` to
  `if (!this.isMonitoringEnabled()) return;`.
  (After change A the listeners won't even be registered when monitoring is off,
  but keep the guard as defense — a queued event can still arrive during
  teardown.)

### F. Stop full-sync-on-every-POST

**`connector.js`**
- Split the response paths:
  - Add `markConnected()` that just sets `this.connected = true`.
  - `postMessage` (`:50`): on success call `markConnected()` and
    `return res.json().catch(() => null)` — **do not** call `this.onMessage`.
    On failure call `this.disconnect()` and return `null` (unchanged).
  - `onTimer` (`:24`) / `onResponse` (`:39`): keep calling `this.onMessage(json)`
    — this is the only place a `/sync` payload is processed.
- Callers that read a POST response (`app.js` `queueVideo`, `probeQuality`,
  `previewImage`) already consume the returned json directly — unaffected.
- `/media`, `/download`, `/clear`, `/tab-update` become true fire-and-forget.

### G. Logging off by default

**`logger.js`**
- `this.loggingEnabled = false;`
- Optional: read `chrome.storage.local.get('debug')` once and enable when set, so
  debugging is still possible without a code edit.

### H. Persist `userDisabled`

**`app.js`**
- On `type === "cmd"` (`:325`): after setting `this.userDisabled`, write
  `chrome.storage.local.set({ userDisabled: this.userDisabled })`.
- In `start()` (`:30`): `await chrome.storage.local.get('userDisabled')` and
  seed `this.userDisabled` before `register()`. (Make `start()` async or chain a
  `.then`.)

### I. (Optional) adaptive poll cadence

**`connector.js` / `app.js`**
- The 12 staggered alarms in `connect()` (`:14-22`) produce a `/sync` roughly
  every 5s forever, which keeps the Chrome service worker alive continuously.
- Replace with: one `chrome.alarms.create("luedd-sync", { periodInMinutes: 1 })`
  for the idle baseline, plus a short 5s `setTimeout`-driven fast poll that runs
  **only while `app.pendingMedia.size > 0` or a detection happened in the last
  30s**. Fall back to the 1-min alarm when idle.
- This is behavior-visible (slower pickup of brand-new detections when idle);
  land A–H first, treat I as a follow-up if SW/CPU wakeups are still a concern.

---

## Server-side note (not required, hygiene)

`crates/luedd-ipc/src/server.rs` builds the ext/type lists sent to the extension
(the `SyncResponse` `requestFileExts` / `mediaExts` / `mediaTypes` fields — grep
`request_file_exts`, `DEFAULT_MEDIA_EXTS`, `default_media_exts`). If `ts` / `m4s`
appear there, drop them so the server never asks the extension to match raw
segments. The client-side filter (change C) is the actual fix; do this only if
it's a one-line list edit.

---

## Verification

No automated tests exist for the extensions. Manual:

1. **Load unpacked**
   - Firefox: `about:debugging` → This Firefox → Load Temporary Add-on →
     `extension-firefox/manifest.json`.
   - Chrome: `chrome://extensions` → Developer mode → Load unpacked →
     `extension/`.
2. **Listeners gated (change A)** — desktop app **closed**. Open the extension's
   background/service-worker console. Browse several sites. Confirm: no
   `onRequestDataReceived` logs (enable debug flag from G to see), and
   `chrome.webRequest.onSendHeaders.hasListeners()` is `false` in the SW console.
   Start the app, toggle monitoring on in the popup → `hasListeners()` becomes
   `true`. Toggle off → `false` again.
3. **Segment storm (changes C/D/E)** — app running, monitoring on. Play a HLS
   video (e.g. any site streaming `.m3u8`). Watch the popup: it should show the
   **manifest** entry (one, maybe a couple), **not** a growing list of segments.
   In the SW console check `app.videoList.length` and `app.pendingMedia.size`
   stay small (< ~10) over several minutes. Before the fix they climb into the
   hundreds/thousands.
4. **requestMap bound (B)** — after ~10 min of heavy browsing,
   `app.requestWatcher.requestMap.size` stays under ~1000.
5. **No full-sync per POST (F)** — add a temporary `console.count("onMessage")`
   in `app.js onMessage`. Trigger 5 detections quickly. Count should increase
   only on the ~5s poll cadence, not once per detection.
6. **Persistence (H)** — disable monitoring in the popup, then in
   `chrome://extensions` click the SW "terminate" link (or wait for idle).
   Reopen the popup → still disabled.
7. **Regression sweep** — with monitoring on and app running:
   - Detect a normal `.mp4`/`.m3u8` link → appears in popup.
   - Click "Add" on an item → downloads in the desktop app (POST `/vid`).
   - "Clear" in popup → list clears.
   - Right-click an image → "Download Image with Lüdd" still works.
8. **Perf sanity** — Firefox `about:performance` / Chrome Task Manager: the
   extension's CPU/energy impact while idle-browsing should be near zero (was
   continuously elevated).

## Files touched

- `extension/request-watcher.js` + `extension-firefox/request-watcher.js` (A, B, C)
- `extension/app.js` + `extension-firefox/app.js` (A, D, E, H) — mind the one
  allowed diff at `register()` around `onDeterminingFilename`
- `extension/connector.js` + `extension-firefox/connector.js` (F, optional I)
- `extension/logger.js` + `extension-firefox/logger.js` (G)
- (optional) `crates/luedd-ipc/src/server.rs` — server ext-list hygiene
- After editing, re-verify the two folders differ **only** in the known spots:
  `diff extension/<f> extension-firefox/<f>` for each shared file.
