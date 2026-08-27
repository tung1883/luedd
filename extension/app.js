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
        // A detection can now be reported twice for the same URL - once
        // locally the instant it's seen (`recordLocalDetection`), and again
        // when/if the app confirms it (`onMessage`'s `newDetection`) - this
        // dedups the resulting OS notification to once per URL per session.
        this.notifiedUrls = new Set();
    }

    start() {
        this.logger.log("starting...");
        this.starAppConnector();
        this.register();
        this.logger.log("started.");
    }

    starAppConnector() {
        this.connector.connect();
    }

    onMessage(msg) {
        this.logger.log("message from tidm");
        this.logger.log(msg);
        this.appEnabled = msg.enabled === true;
        this.fileExts = msg.fileExts;
        this.blockedHosts = msg.blockedHosts;
        this.tabsWatcher = msg.tabsWatcher;
        // The server's list is authoritative for anything it already knows
        // about, but keep any locally-tracked item (see `recordLocalDetection`)
        // it doesn't yet know about - e.g. something detected while the app
        // was closed, or between this response and the request that produced
        // it - rather than dropping it the instant a server response arrives.
        const serverList = msg.videoList || [];
        const serverUrls = new Set(serverList.map(v => v.url));
        const localOnly = (this.videoList || []).filter(v => this.isLocalId(v.id) && !serverUrls.has(v.url));
        this.videoList = [...serverList, ...localOnly];
        this.requestWatcher.updateConfig({
            mediaExts: msg.requestFileExts,
            blockedHosts: msg.blockedHosts,
            matchingHosts: msg.matchingHosts,
            mediaTypes: msg.mediaTypes
        });
        this.updateActionIcon();
        // `newDetection` is only present on the one /media response that just
        // added a genuinely new item (server-side dedup by URL) - never on
        // /sync polls or repeat detections of something already known. This
        // is usually already covered by `recordLocalDetection`'s own
        // notification for the same URL (deduped in `notifyNewDetection`
        // below) - kept as a fallback for the case where the app confirms a
        // detection this extension instance didn't record locally itself
        // (e.g. reported by another window/tab sharing the same app).
        if (msg.newDetection) {
            this.notifyNewDetection(msg.newDetection);
        }
    }

    notifyNewDetection(item) {
        if (!chrome.notifications) return; // permission not granted/unsupported
        if (!item || !item.url || this.notifiedUrls.has(item.url)) return;
        this.notifiedUrls.add(item.url);
        chrome.notifications.create("tidm-video-" + item.id, {
            type: "basic",
            iconUrl: "icon128.png",
            title: "Lüdd found a video",
            message: item.text || item.info,
            priority: 1
        });
    }

    // Queues a detected video for download by id, the same request the popup
    // sends when a list entry is clicked - shared so a notification click can
    // trigger the same download without opening the popup at all. Returns
    // `true`/`false` (from the GUI's `vidQueued`) or `null` if the app never
    // responded, so the popup can show whether the click actually worked.
    // `quality` (a `variant_key` from `probeQuality`) is only set when the
    // user expanded the row's details panel and picked a specific rendition
    // there - the plain one-click path always omits it, keeping the app's
    // own auto-best fallback for the fast path.
    async queueVideo(itemId, quality) {
        const body = { vid: itemId + "" };
        if (quality) body.quality = quality;
        const res = await this.connector.postMessage("/vid", body);
        return res ? res.vidQueued ?? null : null;
    }

    // Fetches the selectable quality variants for a detected item (empty for
    // plain HTTP, or an HLS/DASH URL with only one rendition) - called when
    // a row's details panel is expanded, not eagerly for every detected item.
    // The response is still the full state blob (see `onMessage`'s doc) with
    // a `qualityVariants` field added on top, not a bespoke shape.
    async probeQuality(itemId) {
        const res = await this.connector.postMessage("/probe-quality", { vid: itemId + "" });
        return (res && res.qualityVariants) || [];
    }

    onNotificationClicked(notificationId) {
        const prefix = "tidm-video-";
        if (!notificationId.startsWith(prefix)) return;
        this.queueVideo(notificationId.slice(prefix.length));
        chrome.notifications.clear(notificationId);
    }

    onDisconnect() {
        this.logger.log("Disconnected from native host!");
        this.logger.log("Disconnected...");
        this.updateActionIcon();
    }

    isMonitoringEnabled() {
        this.logger.log(this.appEnabled + " " + this.userDisabled);
        return this.appEnabled === true && this.userDisabled === false && this.connector.isConnected();
    }

    // Whether detection should run at all - unlike `isMonitoringEnabled()`,
    // this doesn't require the app to be reachable: the whole point is that
    // detected links keep showing up in the popup even with the app closed,
    // so only the user's own toggle gates this, not connectivity or the
    // app's own enabled/disabled state (which defaults to false/unknown
    // until a connection is actually made).
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

    // Adds a freshly-detected item to `videoList` immediately, under a
    // locally-generated id, so it shows in the popup right away regardless
    // of whether `/media` below ever reaches the app - `onMessage` reconciles
    // this against the server's own list once/if a response does arrive
    // (matching by URL), replacing the local id with the server's real one.
    recordLocalDetection(data) {
        if (this.videoList.some(v => v.url === data.url)) return;
        this.localDetectionCounter = (this.localDetectionCounter || 0) + 1;
        const item = {
            id: "local" + this.localDetectionCounter,
            text: data.tabUrl || data.file || data.url,
            info: data.file || this.filenameFromUrl(data.url),
            url: data.url,
            pageUrl: data.tabUrl || null,
        };
        this.videoList = [...this.videoList, item];
        this.updateActionIcon();
        this.notifyNewDetection(item);
    }

    onRequestDataReceived(data) {
        //Streaming video data received, send to native messaging application
        this.logger.log("onRequestDataReceived");
        this.logger.log(data);
        if (!this.isDetectionEnabled()) return;
        this.recordLocalDetection(data);
        if (this.connector.isConnected()) {
            this.connector.postMessage("/media", data);
        }
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
        chrome.runtime.onMessage.addListener(this.onPopupMessage.bind(this));
        this.requestWatcher.register();
        this.attachContextMenu();
        chrome.tabs.onActivated.addListener(this.onTabActivated.bind(this));
        if (chrome.notifications) {
            chrome.notifications.onClicked.addListener(this.onNotificationClicked.bind(this));
        }
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
        // if (this.videoList && this.videoList.length > 0) {
        //     let len = this.videoList.filter(vid => {
        //         if (!vid.tabId) {
        //             return true;
        //         }
        //         if (vid.tabId == '-1') {
        //             return true;
        //         }
        //         return (vid.tabId == this.activeTabId);
        //     }).length;
        //     if (len > 0) {
        //         vc = len + "";
        //     }
        // }
        chrome.action.setBadgeText({ text: vc });
        if (!this.connector.isConnected()) {
            this.logger.log("Not connected...");
            // Still show whatever's been detected locally (see
            // `recordLocalDetection`) rather than the "can't connect" page -
            // only fall back to that when there's genuinely nothing to show
            // yet, since it's more useful than an empty list either way.
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
            // if (this.videoList && this.videoList.length > 0) {
            //     chrome.action.setBadgeText({ text: this.videoList.length + "" });
            // }
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
                // list: this.videoList.filter(vid => {
                //     if (!vid.tabId) {
                //         return true;
                //     }
                //     return (vid.tabId == this.activeTabId);
                // })
            };
            sendResponse(resp);
        }
        else if (request.type === "cmd") {
            this.userDisabled = request.enabled === false;
            this.logger.log("request.enabled:" + request.enabled);
            if (request.enabled && !this.connector.isConnected()) {
                this.connector.launchApp();
                return;
            }
            this.updateActionIcon();
        }
        else if (request.type === "vid") {
            this.queueVideo(request.itemId, request.quality).then(success => sendResponse({ success }));
            return true;
        }
        else if (request.type === "probe-quality") {
            this.probeQuality(request.itemId).then(variants => sendResponse({ variants }));
            return true;
        }
        else if (request.type === "clear") {
            this.connector.postMessage("/clear", {});
        }
    }

    sendLinkToXDM(info, tab) {
        let url = info.linkUrl;
        if (!this.isSupportedProtocol(url)) {
            url = info.srcUrl;
        }
        if (!this.isSupportedProtocol(url)) {
            url = info.pageUrl;
        }
        if (!this.isSupportedProtocol(url)) {
            return;
        }
        this.triggerDownload(url, null, info.pageUrl, null, null);
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
        if (info.menuItemId == "download-any-link") {
            this.sendLinkToXDM(info, tab);
        }
        if (info.menuItemId == "download-image-link") {
            this.sendImageToXDM(info, tab);
        }
    }

    attachContextMenu() {
        chrome.contextMenus.create({
            id: 'download-any-link',
            title: "Download with Lüdd",
            contexts: ["link", "video", "audio", "all"]
        });

        chrome.contextMenus.create({
            id: 'download-image-link',
            title: "Download Image with Lüdd",
            contexts: ["image"]
        });

        chrome.contextMenus.onClicked.addListener(this.onMenuClicked.bind(this));
    }

    onTabActivated(activeInfo) {
        this.activeTabId = activeInfo.tabId + "";
        this.logger.log("Active tab: " + this.activeTabId);
        this.updateActionIcon();
    }
}
