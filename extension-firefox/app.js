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
        this.logger.log("message from Lüdd");
        this.logger.log(msg);
        this.appEnabled = msg.enabled === true;
        this.fileExts = msg.fileExts;
        this.blockedHosts = msg.blockedHosts;
        this.tabsWatcher = msg.tabsWatcher;
        const serverList = msg.videoList || [];
        const serverUrls = new Set(serverList.map(v => v.url));
        const localOnly = (this.videoList || []).filter(v => this.isLocalId(v.id) && !serverUrls.has(v.url));
        this.videoList = [...serverList, ...localOnly];
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
        this.pendingMedia.set(data.url, data);
        this.updateActionIcon();
    }

    onRequestDataReceived(data) {
        this.logger.log("onRequestDataReceived");
        this.logger.log(data);
        if (!this.isDetectionEnabled()) return;
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
    }

    register() {
        chrome.downloads.onCreated.addListener(
            this.onDownloadCreatedCallback
        );
        if (chrome.downloads.onDeterminingFilename) {
            chrome.downloads.onDeterminingFilename.addListener(
                this.onDeterminingFilenameCallback
            );
        } else {
            this.logger.log("downloads.onDeterminingFilename unavailable (Firefox) - browser-initiated download takeover disabled");
        }
        chrome.tabs.onUpdated.addListener(
            this.onTabUpdateCallback
        );
        chrome.runtime.onMessage.addListener(this.onPopupMessage.bind(this));
        this.requestWatcher.register();
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
        else if (request.type === "preview") {
            this.previewImage(request.itemId).then(previewDataUrl => sendResponse({ previewDataUrl }));
            return true;
        }
        else if (request.type === "clear") {
            this.connector.postMessage("/clear", {});
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
