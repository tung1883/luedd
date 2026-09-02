"use strict";
// Injected on instagram.com. Reports the feed post / reel currently centred in
// the viewport so scrolling the feed catches posts one at a time, without
// opening any of them. Real /p/ and /stories/ pages are handled by the
// background page-detection instead.

(function () {
    const DEBUG = true;
    const log = (...a) => { if (DEBUG) console.log("[luedd-ig]", ...a); };

    const ext = (typeof browser !== "undefined" && browser.runtime) ? browser
        : (typeof chrome !== "undefined" && chrome.runtime) ? chrome : null;
    if (!ext) { log("no extension runtime — not injected by the extension?"); return; }
    const alive = () => { try { return !!ext.runtime.id; } catch (e) { return false; } };

    let last = null;

    function onFeed() {
        const p = location.pathname;
        return p === "/" || p === "" || p.startsWith("/reels") || p.startsWith("/explore")
            || p.startsWith("/?");
    }

    function codeOf(href) {
        const m = (href || "").match(/\/(p|reel|reels|tv)\/([A-Za-z0-9_-]+)/);
        if (!m) return null;
        return { kind: m[1] === "reels" ? "reel" : m[1], code: m[2] };
    }

    // The tall container that visually is "the post" — walk up until the box is
    // clearly post-sized, so centring math uses the post, not a tiny link.
    function postBox(a) {
        let el = a;
        for (let i = 0; i < 8 && el; i++) {
            const r = el.getBoundingClientRect();
            if (r.height >= 320 && r.width >= 260) return r;
            el = el.parentElement;
        }
        return a.getBoundingClientRect();
    }

    function centredPost() {
        const links = document.querySelectorAll(
            'a[href*="/p/"], a[href*="/reel/"], a[href*="/reels/"], a[href*="/tv/"]'
        );
        const mid = window.innerHeight / 2;
        const seen = new Set();
        let best = null, bestDist = Infinity;
        for (const a of links) {
            const c = codeOf(a.getAttribute("href"));
            if (!c || seen.has(c.code)) continue;
            seen.add(c.code);
            const r = postBox(a);
            if (r.bottom < 40 || r.top > window.innerHeight - 40) continue;   // not really on screen
            const dist = Math.abs((r.top + r.bottom) / 2 - mid);
            if (dist < bestDist) { bestDist = dist; best = c; }
        }
        return best
            ? "https://www.instagram.com/" + best.kind + "/" + best.code + "/"
            : null;
    }

    async function scan() {
        if (!onFeed()) return;
        if (!alive()) { log("extension reloaded — reload this Instagram tab"); return; }
        const url = centredPost();
        if (!url || url === last) return;
        log("centre ->", url);
        try {
            // await so a dropped message (service worker still booting) leaves
            // `last` unset and the 2.5s interval retries the same post
            const r = ext.runtime.sendMessage({ type: "ig-feed-item", url });
            if (r && typeof r.then === "function") await r;
            last = url;
        } catch (e) {
            log("send failed, will retry", e);
        }
    }

    let t = null;
    const kick = () => { clearTimeout(t); t = setTimeout(scan, 400); };
    window.addEventListener("scroll", kick, { passive: true });
    window.addEventListener("resize", kick, { passive: true });
    setInterval(scan, 2500);
    setTimeout(scan, 1500);
    log("content script ready");
})();
