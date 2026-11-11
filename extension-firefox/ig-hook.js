"use strict";
// Runs in the page's own JS world on instagram.com. Instagram's web app loads
// story reels, profile grids and feeds through fetch / XHR; this copies those
// JSON responses (read-only, no extra request) to the content script, which
// hands them to Lüdd. That is what lets stories, highlights and profile posts
// be previewed and downloaded without Lüdd ever calling Instagram's API.
(function () {
    if (window.__luddHook) return;
    window.__luddHook = true;

    const WANT = /\/api\/v1\/(feed\/(reels_media|user|timeline)|users\/web_profile_info|clips|highlights|discover|media)|\/graphql\/query|\/api\/graphql/;
    const MAX = 6 * 1024 * 1024;

    function emit(url, text) {
        try {
            if (!text || text.length > MAX || text.charCodeAt(0) !== 123 /* { */) return;
            window.postMessage({ __ludd: "ig-json", url: String(url).split("?")[0], json: JSON.parse(text) }, "*");
        } catch (e) { }
    }

    const origFetch = window.fetch;
    window.fetch = function () {
        const p = origFetch.apply(this, arguments);
        try {
            const a = arguments[0];
            const url = typeof a === "string" ? a : (a && a.url) || "";
            if (WANT.test(url)) {
                p.then(r => { try { r.clone().text().then(t => emit(url, t), () => { }); } catch (e) { } }, () => { });
            }
        } catch (e) { }
        return p;
    };

    const origOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function (method, url) {
        try {
            const u = String(url);
            if (WANT.test(u)) {
                this.addEventListener("load", function () {
                    try {
                        if (this.responseType === "" || this.responseType === "text") emit(u, this.responseText);
                        else if (this.responseType === "json" && this.response) emit(u, JSON.stringify(this.response));
                    } catch (e) { }
                });
            }
        } catch (e) { }
        return origOpen.apply(this, arguments);
    };
})();
