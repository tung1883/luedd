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
        // /reels/audio/<id>/ is a music page, not a reel
        if (!m || /^(audio|tags|locations|explore)$/.test(m[2])) return null;
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

    // ---- post / reel capture ------------------------------------------------
    // Instagram's API turns away anything that isn't the real browser, so the app
    // never calls it for posts. Instead this page loads the post's own page
    // (same-origin, the user's session, a normal page request), pulls the media
    // object out of the JSON embedded in it, and hands that to the app. The CDN
    // URLs inside are signed, so the app downloads them with no cookie.
    const capState = new Map();   // code -> { at, busy, tries }
    const CAP_TTL = 20 * 60 * 1000;

    function findMedia(node, code, out, depth) {
        if (!node || typeof node !== "object" || depth > 40 || out.length >= 6) return;
        if (Array.isArray(node)) { for (const n of node) findMedia(n, code, out, depth + 1); return; }
        if (node.code === code && (node.image_versions2 || node.carousel_media || node.video_versions)) {
            out.push(node);
            return;   // the whole post object; don't descend into it
        }
        for (const k in node) findMedia(node[k], code, out, depth + 1);
    }

    function mediaFromHtml(html, code) {
        const doc = new DOMParser().parseFromString(html, "text/html");
        const found = [];
        for (const s of doc.querySelectorAll('script[type="application/json"]')) {
            const txt = s.textContent || "";
            if (!txt.includes(code) || !/image_versions2|video_versions|carousel_media/.test(txt)) continue;
            try { findMedia(JSON.parse(txt), code, found, 0); } catch (e) { }
        }
        if (!found.length) return null;
        // the fullest copy wins (a thin stub of the same post may also be embedded)
        found.sort((a, b) => JSON.stringify(b).length - JSON.stringify(a).length);
        return found[0];
    }

    async function capturePost(url) {
        const m = (url || "").match(/\/(p|reel|reels|tv)\/([A-Za-z0-9_-]+)/);
        if (!m || /^(audio|tags|locations|explore)$/.test(m[2])) return false;
        const code = m[2];
        const st = capState.get(code) || { at: 0, busy: false, tries: 0 };
        if (st.busy || (st.at && Date.now() - st.at < CAP_TTL)) return true;
        if (st.tries >= 3) return false;
        st.busy = true; st.tries++;
        capState.set(code, st);
        try {
            const path = "/" + (m[1] === "reels" ? "reel" : m[1]) + "/" + code + "/";
            const r = await fetch(path, { credentials: "include", headers: { Accept: "text/html" } });
            if (!r.ok) { log("capture: page", r.status); return false; }
            const media = mediaFromHtml(await r.text(), code);
            if (!media) { log("capture: no media object in page", code); return false; }
            const res = await ext.runtime.sendMessage({ type: "ig-capture", url: location.origin + path, media });
            const ok = !!(res && res.ok);
            if (ok) { st.at = Date.now(); st.tries = 0; log("captured", code); }
            return ok;
        } catch (e) {
            log("capture failed", e);
            return false;
        } finally {
            st.busy = false;
        }
    }

    // Scroll a profile slowly so the page loads (and we capture) its posts.
    let scrolling = false;
    async function autoScroll() {
        if (scrolling) return;
        scrolling = true;
        try {
            const count = () => document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]').length;
            let idle = 0, prev = count();
            for (let i = 0; i < 250 && idle < 6 && alive(); i++) {
                window.scrollTo(0, document.documentElement.scrollHeight);
                await new Promise(r => setTimeout(r, 1800 + Math.random() * 1400));
                const c = count();
                if (c > prev) { prev = c; idle = 0; } else idle++;
            }
        } finally { scrolling = false; }
    }

    try {
        ext.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
            if (msg && msg.type === "ig-ping") { sendResponse({ ok: true }); return; }
            if (msg && msg.type === "ig-autoscroll") { autoScroll(); return; }
            if (!msg || msg.type !== "ig-capture-req" || !msg.url) return;
            capturePost(msg.url).then(ok => sendResponse({ ok }), () => sendResponse({ ok: false }));
            return true;
        });
    } catch (e) { }

    // JSON the page's own fetch/XHR received (copied by ig-hook.js in the page
    // world) -> Lüdd. Story reels, profile grid pages, feed responses.
    window.addEventListener("message", e => {
        const d = e.data;
        if (e.source !== window || !d || d.__ludd !== "ig-json" || !d.json) return;
        if (!alive()) return;
        try { ext.runtime.sendMessage({ type: "ig-json", url: d.url, json: d.json }); } catch (err) { }
    });

    // Story reels (and some profile / post data) arrive inside the page's own
    // embedded JSON, not through a request the hook could see. Scan those too.
    const scanned = new WeakSet();
    function scanEmbedded() {
        if (!alive()) return;
        for (const s of document.querySelectorAll('script[type="application/json"]')) {
            if (scanned.has(s)) continue;
            scanned.add(s);
            const txt = s.textContent || "";
            if (txt.length > 6 * 1024 * 1024 || !/image_versions2|video_versions/.test(txt)) continue;
            try { ext.runtime.sendMessage({ type: "ig-json", url: location.pathname, json: JSON.parse(txt) }); } catch (e) { }
        }
    }
    setInterval(scanEmbedded, 2500);
    setTimeout(scanEmbedded, 800);

    // Profile header + highlight tiles, read from the profile page itself (the
    // og: meta tags and the tray in the DOM), for Lüdd's profile viewer.
    let lastMeta = "";
    // profile picture from the visible header (the big round avatar)
    function picFromHeader(hdr, own) {
        if (!hdr || !own) return "";
        const imgs = [...hdr.querySelectorAll("img[src]")].filter(i => /cdninstagram|fbcdn/.test(i.src));
        const named = imgs.find(i => /profile picture/i.test(i.alt || ""));
        const best = named || imgs.sort((a, b) => (b.naturalWidth || 0) - (a.naturalWidth || 0))[0];
        return best ? best.src : "";
    }
    function num(txt) {
        const m = (txt || "").replace(/,/g, "").match(/([\d.]+)\s*([KMB])?/i);
        if (!m) return 0;
        const mult = { K: 1e3, M: 1e6, B: 1e9 }[(m[2] || "").toUpperCase()] || 1;
        return Math.round(parseFloat(m[1]) * mult);
    }
    function scanProfile() {
        if (!alive()) return;
        const seg = location.pathname.split("/").filter(Boolean);
        if (seg.length !== 1 || /^(explore|reels|direct|accounts|stories|p|reel|tv)$/i.test(seg[0])) return;
        const user = seg[0];
        const meta = n => ((document.querySelector('meta[property="' + n + '"]') || {}).content || "");
        const title = meta("og:title");
        // the meta tags belong to the first page loaded; only trust them for this profile
        const fresh = title.toLowerCase().includes("@" + user.toLowerCase());   // "Name (@user)" or just "@user"
        const desc = fresh ? meta("og:description") : "";
        // the visible header: always current, even after in-app navigation (when the
        // meta tags still describe the previous page)
        const hdr = document.querySelector("header");
        const hdrText = hdr ? (hdr.innerText || "") : "";
        const hdrOwn = !!hdr && hdrText.toLowerCase().includes(user.toLowerCase());
        const cnt = (re, re2) => {
            const m = desc.match(re) || (hdrOwn ? hdrText.replace(/\s+/g, " ").match(re2) : null);
            return m ? num(m[1]) : 0;
        };
        const highlights = [];
        for (const a of document.querySelectorAll('a[href*="/stories/highlights/"]')) {
            const id = ((a.getAttribute("href") || "").match(/highlights\/(\d+)/) || [])[1];
            if (!id || highlights.some(h => h.id === id)) continue;
            const img = a.querySelector("img");
            highlights.push({
                id,
                title: (a.innerText || (img && img.alt) || a.getAttribute("aria-label") || "").trim().split(/\r?\n/)[0],
                cover_url: img && img.src ? img.src : null,
            });
        }
        const body = document.body ? document.body.innerText.slice(0, 4000) : "";
        const payload = {
            username: user,
            full_name: fresh && title.includes(" (@") ? title.split(" (@")[0]
                : (/\(@/.test(document.title) && document.title.toLowerCase().includes("@" + user.toLowerCase()) ? document.title.split(" (@")[0] : ""),
            profile_pic_url: fresh ? meta("og:image") : picFromHeader(hdr, hdrOwn),
            follower_count: cnt(/([\d.,]+[KMB]?)\s+Followers/i, /([\d.,]+[KMB]?) followers/i),
            following_count: cnt(/([\d.,]+[KMB]?)\s+Following/i, /([\d.,]+[KMB]?) following/i),
            post_count: cnt(/([\d.,]+[KMB]?)\s+Posts/i, /([\d.,]+[KMB]?) posts/i),
            is_private: /This account is private/i.test(body),
            is_verified: !!document.querySelector('header svg[aria-label="Verified"]'),
            highlights,
        };
        // never send a half-loaded page: need this profile's own data, from the meta
        // tags or from its visible header
        if (!fresh && !(hdrOwn && (payload.follower_count || payload.profile_pic_url))) return;
        const sig = JSON.stringify(payload);
        if (sig === lastMeta) return;
        lastMeta = sig;
        try { ext.runtime.sendMessage({ type: "ig-profile-meta", payload }); } catch (e) { }
    }
    setInterval(scanProfile, 3000);
    setTimeout(scanProfile, 1500);

    let t = null;
    const kick = () => { clearTimeout(t); t = setTimeout(scan, 400); };
    window.addEventListener("scroll", kick, { passive: true });
    window.addEventListener("resize", kick, { passive: true });
    setInterval(scan, 2500);
    setTimeout(scan, 1500);
    log("content script ready");
})();
