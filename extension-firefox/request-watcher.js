"use strict";
import Logger from './logger.js';

const IMAGE_EXTS = ['.JPG', '.JPEG', '.PNG', '.GIF', '.WEBP', '.BMP', '.SVG'];
const MIN_IMAGE_BYTES = 20000;

const DEFAULT_MEDIA_EXTS = [
    '.M3U8', '.MPD', '.MP4', '.M4V', '.M4A', '.WEBM', '.MKV', '.MOV', '.AVI', '.FLV', '.TS', '.MP3', '.AAC', '.WAV',
    '.OGG', '.FLAC', '.PDF', '.JPG', '.JPEG', '.PNG', '.GIF', '.WEBP', '.BMP', '.SVG',
];
const DEFAULT_MEDIA_TYPES = [
    'video/', 'audio/', 'application/vnd.apple.mpegurl', 'application/x-mpegurl', 'application/dash+xml',
    'application/vnd.ms-sstr+xml', 'application/pdf', 'image/',
];

// HLS/DASH media segments: never treated as detections (a single stream
// produces hundreds of unique segment URLs per minute). Manifests
// (.M3U8 / .MPD) and progressive files still match normally.
const SEGMENT_EXTS = ['.TS', '.M4S'];
const SEGMENT_PATH_RE = /(?:^|\/)(?:seg(?:ment)?|chunk|frag(?:ment)?)[-_]?\d+/i;

const REQUEST_MAP_MAX = 1000;
const REQUEST_MAP_TARGET = 800;
const REQUEST_MAP_TTL_MS = 60000;

export default class RequestWatcher {
    constructor(callback) {
        this.logger = new Logger();
        this.blockedHosts = [];
        this.mediaExts = DEFAULT_MEDIA_EXTS;
        this.mediaTypes = DEFAULT_MEDIA_TYPES;
        this.fileExts = [];
        this.requestMap = new Map();
        this.callback = callback;
        this.matchingHosts = [];
        this.onSendHeadersEventCallback = this.onSendHeadersEvent.bind(this);
        this.onHeadersReceivedEventCallback = this.onHeadersReceivedEvent.bind(this);
        this.onErrorOccurredEventCallback = this.onErrorOccurredEvent.bind(this);
        this.urlPatterns = [];
        this.requestFileExts = [];
        this.registered = false;
    }

    updateConfig(config) {
        if (config.blockedHosts) {
            this.blockedHosts = config.blockedHosts
        }
        if (config.fileExts) {
            this.fileExts = config.fileExts
        }
        if (config.mediaExts) {
            this.mediaExts = config.mediaExts
        }
        if (config.matchingHosts) {
            this.matchingHosts = config.matchingHosts
        }
        if (config.mediaTypes) {
            this.mediaTypes = config.mediaTypes
        }
        if (config.requestFileExts) {
            this.requestFileExts = config.requestFileExts
        }
        if (config.urlPatterns) {
            this.urlPatterns = config.urlPatterns.map(pattern => {
                try {
                    return new RegExp(pattern, "i");
                } catch { }
            }).filter(item => item || false);
        }
    }

    isMatchingRequest(res) {
        let u = new URL(res.url);

        let hostName = u.host;
        if (this.blockedHosts.find(h => hostName.indexOf(h) >= 0)) {
            return false;
        }

        let path = u.pathname;
        let upath = path.toUpperCase();

        if (SEGMENT_EXTS.find(e => upath.endsWith(e)) || SEGMENT_PATH_RE.test(path)) {
            return false;
        }
        let ctHeader = res.responseHeaders && res.responseHeaders.find(h => h["name"].toUpperCase() === "CONTENT-TYPE");
        let ctVal = ctHeader ? ctHeader["value"].toLowerCase() : "";
        if ((ctVal.indexOf("video/mp2t") >= 0 || ctVal.indexOf("application/octet-stream") >= 0) && /\d/.test(path)) {
            return false;
        }

        let extMatch = this.mediaExts.find(e => upath.endsWith(e));
        if (extMatch) {
            return !this.isSmallImage(extMatch, res);
        }

        let reqExtMatch = this.requestFileExts.find(e => upath.endsWith(e));
        if (reqExtMatch) {
            return !this.isSmallImage(reqExtMatch, res);
        }

        try {
            if (this.urlPatterns.find(re => re.test(res.url))) {
                return true;
            }
        } catch { }

        let mediaType = res.responseHeaders.find(h => h["name"].toUpperCase() === "CONTENT-TYPE");
        if (mediaType && this.mediaTypes.find(m => mediaType["value"].indexOf(m) >= 0)) {
            return !this.isSmallImage(null, res, mediaType["value"]);
        }

        if (this.fileExts.find(e => upath.endsWith("." + e))) {
            return true;
        }

        let contentDisposition = res.responseHeaders.find(h => h["name"].toUpperCase() === "CONTENT-DISPOSITION");
        if (contentDisposition && this.fileExts.find(ext => contentDisposition["value"].toUpperCase().indexOf("." + ext) >= 0)) {
            return true;
        }

        if (this.matchingHosts.find(h => hostName.indexOf(h) >= 0)) {
            return true;
        }
    }

    isSmallImage(matchedExt, res, contentType) {
        let looksLikeImage = (matchedExt && IMAGE_EXTS.includes(matchedExt))
            || (contentType && contentType.toUpperCase().startsWith("IMAGE/"));
        if (!looksLikeImage) {
            return false;
        }
        let contentLength = res.responseHeaders.find(h => h["name"].toUpperCase() === "CONTENT-LENGTH");
        if (!contentLength) {
            return false;
        }
        let bytes = parseInt(contentLength["value"], 10);
        return !isNaN(bytes) && bytes < MIN_IMAGE_BYTES;
    }

    onSendHeadersEvent(info) {
        if (info.method !== "GET" && !(this.matchingHosts
            && this.matchingHosts.find(matchingHost => info.url.indexOf(matchingHost) > 0))) {
            return;
        }
        info.__ts = Date.now();
        this.requestMap.set(info.requestId, info);
        this.sweepRequestMap();
    }

    sweepRequestMap() {
        const now = Date.now();
        for (const [id, info] of this.requestMap) {
            if (now - (info.__ts || 0) > REQUEST_MAP_TTL_MS) {
                this.requestMap.delete(id);
            } else {
                break;
            }
        }
        if (this.requestMap.size > REQUEST_MAP_MAX) {
            let excess = this.requestMap.size - REQUEST_MAP_TARGET;
            for (const id of this.requestMap.keys()) {
                if (excess-- <= 0) break;
                this.requestMap.delete(id);
            }
        }
    }

    async getCookieHeaderForUrl(url) {
        try {
            const cookies = await chrome.cookies.getAll({ url });
            if (!cookies || cookies.length === 0) {
                return undefined;
            }
            return cookies.map(c => `${c.name}=${c.value}`).join('; ');
        } catch (e) {
            return undefined;
        }
    }

    onHeadersReceivedEvent(res) {
        let reqId = res.requestId;
        let req = this.requestMap.get(reqId);
        if (req) {
            this.requestMap.delete(reqId);
            if (this.callback && this.isMatchingRequest(res)) {
                const finish = async (title, tabUrl) => {
                    const data = this.createRequestData(req, res, title, tabUrl, req.tabId);
                    const cookieFromJar = await this.getCookieHeaderForUrl(res.url);
                    if (cookieFromJar) {
                        data.cookie = cookieFromJar;
                    }
                    this.callback(data);
                };
                if (req.tabId !== -1) {
                    chrome.tabs.get(
                        req.tabId,
                        tab => {
                            finish(tab.title, tab.url);
                        }
                    );
                } else {
                    finish(null, null);
                }
            }
        }
    }

    onErrorOccurredEvent(info) {
        let reqId = info.requestId;
        this.requestMap.delete(reqId);
    }

    register() {
        if (this.registered) return;
        this.registered = true;
        try {
            chrome.webRequest.onSendHeaders.addListener(
                this.onSendHeadersEventCallback,
                { urls: ["http://*/*", "https://*/*"] },
                ["extraHeaders", "requestHeaders"]
            );
        } catch (e) {
            chrome.webRequest.onSendHeaders.addListener(
                this.onSendHeadersEventCallback,
                { urls: ["http://*/*", "https://*/*"] },
                ["requestHeaders"]
            );
        }

        try {
            chrome.webRequest.onHeadersReceived.addListener(
                this.onHeadersReceivedEventCallback,
                { urls: ["http://*/*", "https://*/*"] },
                ["extraHeaders", "responseHeaders"]
            );
        } catch (e) {
            chrome.webRequest.onHeadersReceived.addListener(
                this.onHeadersReceivedEventCallback,
                { urls: ["http://*/*", "https://*/*"] },
                ["responseHeaders"]
            );
        }

        chrome.webRequest.onErrorOccurred.addListener(
            this.onErrorOccurredEventCallback,
            { urls: ["http://*/*", "https://*/*"] }
        );
    }

    unRegister() {
        if (!this.registered) return;
        this.registered = false;
        chrome.webRequest.onSendHeaders.removeListener(this.onSendHeadersEventCallback);
        chrome.webRequest.onHeadersReceived.removeListener(this.onHeadersReceivedEventCallback);
        chrome.webRequest.onErrorOccurred.removeListener(this.onErrorOccurredEventCallback);
    }

    createRequestData(req, res, title, tabUrl, tabId) {
        let data = {
            url: res.url,
            file: title,
            requestHeaders: {},
            responseHeaders: {},
            cookie: undefined,
            method: req.method,
            userAgent: navigator.userAgent,
            tabUrl: tabUrl,
            tabId: tabId + ""
        };

        let cookies = [];

        if (req.extraHeaders) {
            req.extraHeaders.forEach(h => {
                if (h.name === 'Cookie' || h.name === 'cookie') {
                    cookies.push(h.value);
                }
                this.addToDict(data.requestHeaders, h.name, h.value);
            });
        }
        if (req.requestHeaders) {
            req.requestHeaders.forEach(h => {
                if (h.name === 'Cookie' || h.name === 'cookie') {
                    cookies.push(h.value);
                }
                this.addToDict(data.requestHeaders, h.name, h.value);
            });
        }
        if (res.responseHeaders) {
            res.responseHeaders.forEach(h => {
                this.addToDict(data.responseHeaders, h.name, h.value);
            });
        }
        if (cookies.length > 0) {
            data.cookie = cookies.join(";");
        }
        return data;
    }

    addToDict(dict, key, value) {
        let values = dict[key];
        if (values) {
            values.push(value);
        } else {
            dict[key] = [value];
        }
    }
}