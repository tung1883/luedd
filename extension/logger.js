"use strict";
export default class Logger {
    constructor() {
        this.loggingEnabled = false;
        try {
            chrome.storage.local.get('debug', v => {
                if (v && v.debug) this.loggingEnabled = true;
            });
        } catch (e) { }
    }

    log(content) {
        if (this.loggingEnabled) {
            console.log(content);
        }
    }
}