"use strict";
import Logger from './logger.js';

// Image extensions we deliberately watch for (see server.rs's
// DEFAULT_MEDIA_EXTS/DEFAULT_MEDIA_TYPES), but every page loads dozens of tiny
// ones - icons, tracking pixels, ad thumbnails - that nobody would ever want a
// download notification for. Below this size (bytes), an otherwise-matching
// image request is treated as a false positive rather than a real download.
const IMAGE_EXTS = ['.JPG', '.JPEG', '.PNG', '.GIF', '.WEBP', '.BMP', '.SVG'];
const MIN_IMAGE_BYTES = 20000;

// Mirrors server.rs's DEFAULT_MEDIA_EXTS/DEFAULT_MEDIA_TYPES - the real
// config normally arrives from the app's /sync response, but detection would
// never match anything at all before that first successful connection (or
// for as long as the app stays closed), which defeats "still show links
// without the app running." These are the starting values, overwritten as
// soon as a real config does arrive (see App.onMessage).
const DEFAULT_MEDIA_EXTS = [
    '.M3U8', '.MPD', '.MP4', '.M4V', '.M4A', '.WEBM', '.MKV', '.MOV', '.AVI', '.FLV', '.TS', '.MP3', '.AAC', '.WAV',
    '.OGG', '.FLAC', '.PDF', '.JPG', '.JPEG', '.PNG', '.GIF', '.WEBP', '.BMP', '.SVG',
];
const DEFAULT_MEDIA_TYPES = [
    'video/', 'audio/', 'application/vnd.apple.mpegurl', 'application/x-mpegurl', 'application/dash+xml',
    'application/vnd.ms-sstr+xml', 'application/pdf', 'image/',
];

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

    // Returns true if this looks like an image match (by extension or
    // content-type) that's smaller than MIN_IMAGE_BYTES - a favicon,
    // tracking pixel, or ad thumbnail rather than something worth flagging as
    // a download. Unknown size (no Content-Length) is let through rather than
    // filtered, since that's the normal case for a genuinely large image too.
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
        this.requestMap.set(info.requestId, info);
    }

    // The page's own fetch()/XHR for this URL may have been made with a
    // credentials mode that omits cookies entirely (the default for a
    // cross-origin fetch), so webRequest's captured requestHeaders can show
    // no Cookie at all even though the browser genuinely has one stored for
    // that site (e.g. a Cloudflare `cf_clearance` cookie obtained on an
    // earlier navigation). Reading straight from the cookie jar via
    // chrome.cookies - already permitted via the "cookies" manifest
    // permission, previously unused - is authoritative regardless of what
    // that specific request did or didn't attach.
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
        // "extraHeaders" is a Chrome-only extraInfoSpec value; on a browser that
        // rejects it (e.g. Firefox) this would throw and, uncaught, abort every
        // listener registration below it. Falling back keeps this file usable
        // unmodified across browsers instead of silently breaking detection.
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