"use strict";
import Logger from './logger.js';
import RequestWatcher from './request-watcher.js';
import Connector from './connector.js';

export default class App {
    constructor() {
        this.logger = new Logger();
        this.videoList = [];
        this.blockedHosts = [];
        this.fileExts = [];
        this.requestWatcher = new RequestWatcher(this.onRequestDataReceived.bind(this));
        this.tabsWatcher = [];
        this.userDisabled = false;
        this.appEnabled = false;
        this.onDownloadCreatedCallback = this.onDownloadCreated.bind(this);
        this.onDeterminingFilenameCallback = this.onDeterminingFilename.bind(this);
        this.onTabUpdateCallback = this.onTabUpdate.bind(this);
        this.activeTabId = -1;
        this.connector = new Connector(this.onMessage.bind(this), this.onDisconnect.bind(this));
        // Detections whose /media POST hasn't been confirmed by the server
        // yet (the fetch itself failed, or fired during a moment the app
        // wasn't reachable) - keyed by URL, holding the full original
        // payload (headers/cookie/userAgent, not just the derived list item)
        // so a retry has everything it needs. Reconciled on every poll in
        // `onMessage` below.
        this.pendingMedia = new Map();
        // O(1) dedupe for detections - replaces a linear scan of videoList
        // that went quadratic under a stream-segment storm.
        this.seenUrls = new Set();
        // Hosts (from the app) whose page URL we offer as a detection directly
        // - yt-dlp watch pages etc, where there is no catchable media request.
        this.pageHosts = [];
        this.postedPages = new Set();
    }

    async start() {
        this.logger.log("starting...");
        try {
            const stored = await chrome.storage.local.get('userDisabled');
            if (stored && typeof stored.userDisabled === 'boolean') {
                this.userDisabled = stored.userDisabled;
            }
        } catch (e) { }
        this.starAppConnector();
        this.register();
        this.logger.log("started.");
    }

    syncWatcherRegistration() {
        if (this.isMonitoringEnabled()) this.requestWatcher.register();
        else this.requestWatcher.unRegister();
    }

    starAppConnector() {
        this.connector.connect();
    }

    onMessage(msg) {
        this.logger.log("message from Lüdd");
        this.logger.log(msg);
        this.appEnabled = msg.enabled === true;
        this.fileExts = msg.fileExts;
        this.blockedHosts = msg.blockedHosts;
        this.tabsWatcher = msg.tabsWatcher;
        this.pageHosts = msg.pageHosts || [];
        const serverList = msg.videoList || [];
        const serverUrls = new Set(serverList.map(v => v.url));
        const localOnly = (this.videoList || [])
            .filter(v => this.isLocalId(v.id) && !serverUrls.has(v.url))
            .slice(-300);
        this.videoList = [...serverList, ...localOnly];
        this.seenUrls = new Set(this.videoList.map(v => v.url));
        // Self-healing retry: anything still pending that the server has now
        // confirmed is done; anything still pending that the server hasn't
        // seen gets POSTed again, capped to a small batch per tick. Without
        // that cap, a backlog (e.g. built up while the app was closed) would
        // fire one concurrent fetch per pending item on *every* poll -
        // hundreds of simultaneous requests every ~5s, which is enough to
        // make the browser itself sluggish/unresponsive, not just slow to
        // sync. Confirmed items are always cleaned up immediately (cheap,
        // no network); only the actual retry POSTs are throttled, so a large
        // backlog drains gradually across several ticks instead of bursting.
        const MAX_MEDIA_RETRIES_PER_TICK = 5;
        let retriedThisTick = 0;
        for (const [url, data] of this.pendingMedia) {
            if (serverUrls.has(url)) {
                this.pendingMedia.delete(url);
            } else if (retriedThisTick < MAX_MEDIA_RETRIES_PER_TICK) {
                this.connector.postMessage("/media", data);
                retriedThisTick++;
            }
        }
        this.requestWatcher.updateConfig({
            mediaExts: msg.requestFileExts,
            blockedHosts: msg.blockedHosts,
            matchingHosts: msg.matchingHosts,
            mediaTypes: msg.mediaTypes
        });
        this.updateActionIcon();
        this.syncWatcherRegistration();
        this.maybePushIgCookie();

        // Let a page be re-detected once its item has left the panel.
        for (const k of this.postedPages) {
            if (!this.seenUrls.has(k)) this.postedPages.delete(k);
        }
        // Scan already-open tabs too - onTabUpdate only fires on navigation, so
        // a tab open before the extension (re)loaded would never be seen.
        if (this.isMonitoringEnabled() && this.pageHosts.length) {
            try {
                chrome.tabs.query({}, tabs => (tabs || []).forEach(t => { if (t && t.url) this.maybeDetectPage(t); }));
            } catch (e) { }
        }
    }

    async queueVideo(itemId, quality) {
        const body = { vid: itemId + "" };
        if (quality) body.quality = quality;
        const res = await this.connector.postMessage("/vid", body);
        return res ? res.vidQueued ?? null : null;
    }

    async probeQuality(itemId) {
        const res = await this.connector.postMessage("/probe-quality", { vid: itemId + "" });
        return (res && res.qualityVariants) || [];
    }

    async previewImage(itemId) {
        const res = await this.connector.postMessage("/preview", { vid: itemId + "" });
        return (res && res.previewDataUrl) || null;
    }

    onDisconnect() {
        this.logger.log("Disconnected from native host!");
        this.logger.log("Disconnected...");
        this.updateActionIcon();
        this.syncWatcherRegistration();
    }

    isMonitoringEnabled() {
        this.logger.log(this.appEnabled + " " + this.userDisabled);
        return this.appEnabled === true && this.userDisabled === false && this.connector.isConnected();
    }

    isDetectionEnabled() {
        return this.userDisabled === false;
    }

    isLocalId(id) {
        return typeof id === "string" && id.startsWith("local");
    }

    filenameFromUrl(url) {
        try {
            const path = new URL(url).pathname;
            const last = path.split('/').filter(Boolean).pop();
            return last || url;
        } catch {
            return url;
        }
    }

    recordLocalDetection(data) {
        if (this.seenUrls.has(data.url)) return;
        this.localDetectionCounter = (this.localDetectionCounter || 0) + 1;
        const item = {
            id: "local" + this.localDetectionCounter,
            text: data.tabUrl || data.file || data.url,
            info: data.file || this.filenameFromUrl(data.url),
            url: data.url,
            pageUrl: data.tabUrl || null,
        };
        this.seenUrls.add(data.url);
        this.videoList = [...this.videoList, item];
        if (this.videoList.length > 300) {
            this.videoList = this.videoList.slice(-300);
            this.seenUrls = new Set(this.videoList.map(v => v.url));
        }
        if (this.pendingMedia.size >= 200) {
            let excess = this.pendingMedia.size - 150;
            for (const key of this.pendingMedia.keys()) {
                if (excess-- <= 0) break;
                this.pendingMedia.delete(key);
            }
        }
        this.pendingMedia.set(data.url, data);
        this.updateActionIcon();
    }

    hostMatchesPageHost(u) {
        let host;
        try { host = new URL(u).host.toLowerCase(); } catch { return false; }
        return (this.pageHosts || []).some(h => host === h || host.endsWith("." + h));
    }

    onRequestDataReceived(data) {
        this.logger.log("onRequestDataReceived");
        this.logger.log(data);
        if (!this.isMonitoringEnabled()) return;
        // On a page a plugin owns (Instagram, a yt-dlp watch page…), the page
        // detection is the download - the dozens of thumbnail/segment/ad media
        // requests that page fires are noise. Offer the page instead.
        if (this.hostMatchesPageHost(data.tabUrl)) {
            const tid = parseInt(data.tabId, 10);
            this.maybeDetectPage({ url: data.tabUrl, title: data.file || null, id: Number.isNaN(tid) ? undefined : tid });
            return;
        }
        this.recordLocalDetection(data);
        this.connector.postMessage("/media", data);
    }

    onDeterminingFilename(download, suggest) {
        this.logger.log("onDeterminingFilename");
        if (!this.isMonitoringEnabled()) {
            return;
        }
        this.logger.log(download);
        let url = download.finalUrl || download.url;
        this.logger.log(url);
        if (this.isMonitoringEnabled() && this.shouldTakeOver(url, download.filename)) {
            chrome.downloads.cancel(
                download.id,
                () => chrome.downloads.erase({ id: download.id })
            );
            let referrer = download.referrer;
            if (!referrer && download.finalUrl !== download.url) {
                referrer = download.url;
            }
            this.triggerDownload(url, download.filename,
                referrer, download.fileSize, download.mime);
        }
    }

    onDownloadCreated(download) {
        this.logger.log("onDownloadCreated");
        this.logger.log(download);
    }

    onTabUpdate(tabId, changeInfo, tab) {
        if (!this.isMonitoringEnabled()) {
            return;
        }
        if (changeInfo.title) {
            if (this.tabsWatcher &&
                this.tabsWatcher.find(t => tab.url.indexOf(t) > 0)) {
                this.logger.log("Tab changed: " + changeInfo.title + " => " + tab.url);
                try {
                    this.connector.postMessage("/tab-update", {
                        tabUrl: tab.url,
                        tabTitle: changeInfo.title
                    });
                } catch (ex) {
                    console.log(ex);
                }
            }
        }
        // SPA sites (YouTube, Instagram) change the URL via history without a
        // full load, and settle the <title> a beat later. Trigger on url / title
        // / complete, debounced per-tab so we read the *settled* title.
        if ((changeInfo.status === "complete" || changeInfo.title || changeInfo.url) && tab && tab.url) {
            this.scheduleDetectPage(tabId);
        }
    }

    // Strip tracking / view-state params and the trailing slash so the same
    // post/profile isn't detected once per URL variant (`/p/X/`, `/p/X`,
    // `/p/X/?img_index=1`, `?igsh=…`). Identity-bearing params (YouTube's `v`)
    // are kept.
    canonicalPageUrl(raw) {
        try {
            const u = new URL(raw);
            u.hash = "";
            const noise = /^(img_index|igsh|igshid|hl|si|feature|utm_|fbclid|ref_src|ref_url|__|source|_r)$/i;
            for (const k of [...u.searchParams.keys()]) if (noise.test(k)) u.searchParams.delete(k);
            if (u.pathname.length > 1) u.pathname = u.pathname.replace(/\/+$/, "");
            return u.toString();
        } catch { return raw; }
    }

    scheduleDetectPage(tabId) {
        if (!this.pageDetectTimers) this.pageDetectTimers = new Map();
        clearTimeout(this.pageDetectTimers.get(tabId));
        this.pageDetectTimers.set(tabId, setTimeout(async () => {
            this.pageDetectTimers.delete(tabId);
            try {
                const tab = await chrome.tabs.get(tabId);
                if (tab && tab.url) this.maybeDetectPage(tab);
            } catch (e) { }
        }, 1500));
    }

    // A fresh page load (home -> video) sets the real <title> a few seconds
    // after `onUpdated` stops firing. Poll the tab a handful of times so the
    // title lands even without another navigation event.
    scheduleTitleRetry(tabId) {
        if (!this.titleRetries) this.titleRetries = new Map();
        if (this.titleRetries.has(tabId)) return;   // one chain per tab
        let tries = 0;
        const iv = setInterval(async () => {
            tries++;
            let tab;
            try { tab = await chrome.tabs.get(tabId); } catch (e) { tab = null; }
            const done = tries >= 8 || !tab || !tab.url;
            if (done) { clearInterval(iv); this.titleRetries.delete(tabId); }
            if (tab && tab.url) this.maybeDetectPage(tab);
        }, 2000);
        this.titleRetries.set(tabId, iv);
    }

    async maybeDetectPage(tab) {
        let url;
        try { url = new URL(tab.url); } catch { return; }
        if (url.protocol !== "https:" && url.protocol !== "http:") return;
        const host = url.host.toLowerCase();
        const match = (this.pageHosts || []).some(h => host === h || host.endsWith("." + h));
        if (!match) return;
        if (url.pathname === "/results" || url.pathname === "/search") return;

        const tabId = (typeof tab.id === "number" && tab.id >= 0) ? tab.id : null;
        // A synthetic tab (media request, or an ig-feed-item message) carries no
        // title — borrow the live tab's title, but KEEP this call's url (the feed
        // tab's url is the feed, not the post we were asked about).
        if ((tab.title == null || tab.title === "") && tabId != null) {
            try {
                const full = await chrome.tabs.get(tabId);
                if (full && full.title) tab = Object.assign({}, tab, { title: full.title });
            } catch (e) { }
        }

        const canon = this.canonicalPageUrl(tab.url);
        // Bare home / search page (checked on the *canonicalised* URL so
        // `instagram.com/?hl=en` is still treated as the feed and skipped).
        try {
            const cp = new URL(canon);
            if (cp.pathname.replace(/\/+$/, "").length <= 1 && !cp.search) return;
        } catch (e) { }
        const title = tab.title || null;
        const host0 = url.host.replace(/^www\./, "").split(".")[0];
        const GENERIC = new Set(["watch", "video", "videos", "home", "youtube", "shorts", host0]);
        const generic = t => !t || t.trim().length < 4 || GENERIC.has(t.trim().toLowerCase())
            || /^https?:\/\//i.test(t.trim());
        if (!this.postedPageTitles) this.postedPageTitles = new Map();

        if (this.postedPages.has(canon) || this.seenUrls.has(canon)) {
            // already offered — re-send once the SPA settles a real title
            const prev = this.postedPageTitles.get(canon);
            if (title && title !== prev && !generic(title)) {
                this.postedPageTitles.set(canon, title);
                let ck;
                try {
                    const cs = await chrome.cookies.getAll({ url: tab.url });
                    if (cs && cs.length) ck = cs.map(c => `${c.name}=${c.value}`).join("; ");
                } catch (e) { }
                this.connector.postMessage("/page", { url: canon, title, cookie: ck });
                if (this.titleRetries && this.titleRetries.has(tabId)) {
                    clearInterval(this.titleRetries.get(tabId));
                    this.titleRetries.delete(tabId);
                }
            } else if (generic(title) && !this.postedPageTitles.get(canon) && tabId != null) {
                this.scheduleTitleRetry(tabId);   // still waiting for the real title
            }
            return;
        }

        this.postedPages.add(canon);
        // only remember a real title — leave a generic one unset so any later
        // proper title triggers the refresh above
        if (!generic(title)) this.postedPageTitles.set(canon, title);
        if (this.postedPages.size > 300) {
            this.postedPages = new Set([...this.postedPages].slice(-150));
        }
        let cookie;
        try {
            const cookies = await chrome.cookies.getAll({ url: tab.url });
            if (cookies && cookies.length) {
                cookie = cookies.map(c => `${c.name}=${c.value}`).join("; ");
            }
        } catch (e) { }
        this.logger.log("page detection: " + canon);
        this.connector.postMessage("/page", { url: canon, title: generic(title) ? null : title, cookie });
        if (generic(title) && tabId != null) this.scheduleTitleRetry(tabId);
    }

    // Push the current instagram.com session cookie to Lüdd so the profile
    // viewer's /ig/* calls work without first visiting an IG page this run.
    // Throttled to ~once a minute; the app keeps it in memory only.
    async maybePushIgCookie() {
        const now = Date.now();
        if (this._igCookieAt && now - this._igCookieAt < 60000) return;
        this._igCookieAt = now;
        try {
            const cookies = await chrome.cookies.getAll({ url: "https://www.instagram.com/" });
            if (!cookies || !cookies.some(c => c.name === "sessionid" && c.value)) return;
            const cookie = cookies.map(c => `${c.name}=${c.value}`).join("; ");
            this.connector.postMessage("/ig/cookie", { cookie });
        } catch (e) { }
    }

    register() {
        chrome.downloads.onCreated.addListener(
            this.onDownloadCreatedCallback
        );
        chrome.downloads.onDeterminingFilename.addListener(
            this.onDeterminingFilenameCallback
        );
        chrome.tabs.onUpdated.addListener(
            this.onTabUpdateCallback
        );
        // SPA route changes (Instagram feed -> /p/..., YouTube home -> /watch)
        // are not always reported by tabs.onUpdated — webNavigation is reliable.
        if (chrome.webNavigation && chrome.webNavigation.onHistoryStateUpdated) {
            const onNav = d => { if (d && d.frameId === 0 && d.tabId >= 0) this.scheduleDetectPage(d.tabId); };
            chrome.webNavigation.onHistoryStateUpdated.addListener(onNav);
            chrome.webNavigation.onCommitted.addListener(onNav);
        }
        chrome.runtime.onMessage.addListener(this.onPopupMessage.bind(this));
        this.syncWatcherRegistration();
        this.attachContextMenu();
        chrome.tabs.onActivated.addListener(this.onTabActivated.bind(this));
    }

    isSupportedProtocol(url) {
        if (!url) return false;
        let u = new URL(url);
        return u.protocol === 'http:' || u.protocol === 'https:';
    }

    shouldTakeOver(url, file) {
        let u = new URL(url);
        if (!this.isSupportedProtocol(url)) {
            return false;
        }
        let hostName = u.host;
        if (this.blockedHosts.find(item => hostName.indexOf(item) >= 0)) {
            return false;
        }
        let path = file || u.pathname;
        let upath = path.toUpperCase();
        if (this.fileExts.find(ext => upath.endsWith(ext))) {
            return true;
        }
        return false;
    }

    updateActionIcon() {
        chrome.action.setIcon({ path: this.getActionIcon() });
        let vc = "";
        if (this.videoList && this.videoList.length > 0) {
            let len = this.videoList.length;
            if (len > 0) {
                vc = len + "";
            }
        }
        chrome.action.setBadgeText({ text: vc });
        if (!this.connector.isConnected()) {
            this.logger.log("Not connected...");
            const hasDetections = this.videoList && this.videoList.length > 0;
            chrome.action.setPopup({ popup: hasDetections ? "./popup.html" : "./error.html" });
            return;
        }
        if (!this.appEnabled) {
            chrome.action.setPopup({ popup: "./disabled.html" });
            return;
        }
        else {
            chrome.action.setPopup({ popup: "./popup.html" });
            return;
        }
    }

    getActionIconName(icon) {
        return this.isMonitoringEnabled() ? icon + ".png" : icon + "-mono.png";
    }

    getActionIcon() {
        return {
            "16": this.getActionIconName("icon16"),
            "48": this.getActionIconName("icon48"),
            "128": this.getActionIconName("icon128")
        }
    }

    triggerDownload(url, file, referer, size, mime) {
        chrome.cookies.getAll({ "url": url }, cookies => {
            let cookieStr = undefined;
            if (cookies) {
                cookieStr = cookies.map(cookie => cookie.name + "=" + cookie.value).join("; ");
            }
            let requestHeaders = { "User-Agent": [navigator.userAgent] };
            if (referer) {
                requestHeaders["Referer"] = [referer];
            }
            let responseHeaders = {};
            if (size) {
                let fz = +size;
                if (fz > 0) {
                    responseHeaders["Content-Length"] = [fz];
                }
            }
            if (mime) {
                responseHeaders["Content-Type"] = [mime];
            }
            let data = {
                url: url,
                cookie: cookieStr,
                requestHeaders: requestHeaders,
                responseHeaders: responseHeaders,
                filename: file,
                fileSize: size,
                mimeType: mime
            };
            this.logger.log(data);
            this.connector.postMessage("/download", data);
        });
    }

    diconnect() {
        this.onDisconnect();
    }

    onPopupMessage(request, sender, sendResponse) {
        this.logger.log(request.type);
        if (request.type === "stat") {
            let resp = {
                enabled: this.isMonitoringEnabled(),
                list: this.videoList
            };
            sendResponse(resp);
        }
        else if (request.type === "cmd") {
            this.userDisabled = request.enabled === false;
            this.logger.log("request.enabled:" + request.enabled);
            try {
                chrome.storage.local.set({ userDisabled: this.userDisabled });
            } catch (e) { }
            if (request.enabled && !this.connector.isConnected()) {
                this.connector.launchApp();
                return;
            }
            this.updateActionIcon();
            this.syncWatcherRegistration();
        }
        else if (request.type === "vid") {
            this.queueVideo(request.itemId, request.quality).then(success => sendResponse({ success }));
            return true;
        }
        else if (request.type === "probe-quality") {
            this.probeQuality(request.itemId).then(variants => sendResponse({ variants }));
            return true;
        }
        else if (request.type === "preview") {
            this.previewImage(request.itemId).then(previewDataUrl => sendResponse({ previewDataUrl }));
            return true;
        }
        else if (request.type === "clear") {
            this.connector.postMessage("/clear", {});
        }
        // Content script on instagram.com reports the feed post currently in
        // centre view (and active stories) so browsing the feed catches posts
        // without opening each one.
        else if (request.type === "ig-feed-item" && request.url) {
            if (this.isMonitoringEnabled()) {
                const tid = sender && sender.tab ? sender.tab.id : undefined;
                this.maybeDetectPage({ url: request.url, title: request.title || null, id: tid });
            }
        }
    }

    sendImageToXDM(info, tab) {
        let url = info.srcUrl;
        if (!this.isSupportedProtocol(url))
            url = info.linkUrl;
        if (!this.isSupportedProtocol(url)) {
            url = info.pageUrl;
        }
        if (!this.isSupportedProtocol(url)) {
            return;
        }
        this.triggerDownload(url, null, info.pageUrl, null, null);
    }

    onMenuClicked(info, tab) {
        if (info.menuItemId == "download-image-link") {
            this.sendImageToXDM(info, tab);
        }
    }

    attachContextMenu() {
        chrome.contextMenus.removeAll(() => {
            chrome.contextMenus.create({
                id: 'download-image-link',
                title: "Download Image with Lüdd",
                contexts: ["image"]
            });
        });

        chrome.contextMenus.onClicked.addListener(this.onMenuClicked.bind(this));
    }

    onTabActivated(activeInfo) {
        this.activeTabId = activeInfo.tabId + "";
        this.logger.log("Active tab: " + this.activeTabId);
        this.updateActionIcon();
    }
}
