"use strict";
// Injected on every http/https page. Catches clicks on `magnet:` links and
// hands them to Lüdd instead of letting the browser do nothing / pop an
// "open with…" dialog. Falls back to the OS handler if Lüdd can't take it.

(function () {
  const ext = (typeof browser !== "undefined" && browser.runtime) ? browser
    : (typeof chrome !== "undefined" && chrome.runtime) ? chrome : null;
  if (!ext) return;
  const DEBUG = false;
  const log = (...a) => { if (DEBUG) try { console.log("[luedd-magnet]", ...a); } catch (_) {} };

  let intercept = true;   // off only when the user disabled the extension
  try {
    ext.storage.local.get("userDisabled", (r) => {
      if (r && typeof r.userDisabled === "boolean") intercept = !r.userDisabled;
    });
    ext.storage.onChanged.addListener((ch, area) => {
      if (area === "local" && ch.userDisabled) {
        intercept = !(ch.userDisabled.newValue === true);
      }
    });
  } catch (_) {}

  function magnetOf(e) {
    const t = e.target;
    if (!t || !t.closest) return null;
    const a = t.closest('a[href^="magnet:"], a[href^="MAGNET:"]');
    return a ? a.getAttribute("href") : null;
  }

  function handOff(url) {
    log("hand off", url.slice(0, 60));
    let done = false;
    const fallback = (why) => {
      if (done) return;
      done = true;
      log("fallback to OS handler:", why);
      try { window.location.href = url; } catch (_) {}
    };
    try {
      const p = ext.runtime.sendMessage({ type: "magnet", url, pageUrl: location.href }, (res) => {
        void ext.runtime.lastError;
        done = true;
        log("bg replied", res);
        if (!res || res.ok !== true) fallback("bg said no");
      });
      if (p && typeof p.then === "function") {
        p.then((res) => { done = true; log("bg replied", res); if (!res || res.ok !== true) fallback("bg said no"); })
         .catch((e) => fallback("sendMessage rejected " + e));
      }
    } catch (e) { fallback("sendMessage threw " + e); }
    setTimeout(() => fallback("bg silent 3s"), 3000);
  }

  function onClick(e) {
    if (!intercept) return;
    if (e.type === "click" && e.button !== 0) return;
    if (e.type === "auxclick" && e.button !== 1) return;
    const url = magnetOf(e);
    if (!url) return;
    log("magnet click", e.type);
    e.preventDefault();
    e.stopPropagation();
    handOff(url);
  }

  document.addEventListener("click", onClick, true);
  document.addEventListener("auxclick", onClick, true);
  log("armed on", location.href.slice(0, 80));
})();
