class VideoPopup {
    run() {
        document.addEventListener('DOMContentLoaded', this.onLoad.bind(this), false);
    }

    onLoad() {
        document.getElementById('content').style.display = 'none';

        // id -> { url, kind } | null. Survives re-renders so a thumbnail is
        // fetched from the app at most once per popup session.
        this.previewCache = new Map();
        this.previewObserver = new IntersectionObserver(entries => {
            for (const e of entries) {
                if (e.isIntersecting) {
                    this.previewObserver.unobserve(e.target);
                    if (e.target._loadPreview) e.target._loadPreview();
                }
            }
        }, { root: document.getElementById("list"), rootMargin: "150px" });

        document.getElementById("chk").addEventListener('click', (e) => {
            chrome.runtime.sendMessage({ type: "cmd", enabled: document.getElementById("chk").checked });
            window.close();
        });

        document.getElementById('clear').addEventListener('click', e => {
            chrome.runtime.sendMessage({ type: "clear" });
            window.close();
        });
        document.getElementById('format').addEventListener('click', e => {
            alert("Please play the video in desired format in web player")
        });

        document.getElementById("search-input").addEventListener('input', e => {
            this.applyFilter(e.target.value);
        });

        document.addEventListener("click", () => document.querySelectorAll(".dd.open").forEach(o => o.classList.remove("open")));
        this.initDropdown(document.getElementById("filter-type"), this.reapplyFilter.bind(this));
        {
            let drawer = document.getElementById("filter-drawer");
            let toggle = document.getElementById("filter-toggle");
            toggle.addEventListener("click", () => {
                let open = drawer.classList.toggle("open");
                toggle.classList.toggle("on", open);
            });
            document.getElementById("filter-hide").addEventListener("input", this.reapplyFilter.bind(this));
            document.getElementById("filter-reset").addEventListener("click", () => {
                let dd = document.getElementById("filter-type");
                dd.dataset.value = "all";
                dd._sync();
                document.getElementById("filter-hide").value = "";
                this.reapplyFilter();
            });
        }

        chrome.runtime.onMessage.addListener(msg => {
            if (msg && msg.type === "refresh") this.fetchAndRender();
        });

        this.fetchAndRender();
    }

    fetchAndRender() {
        chrome.runtime.sendMessage({ type: "stat" }, this.onMsg.bind(this));
    }

    onMsg(response) {
        document.getElementById("chk").checked = response.enabled;
        if (response.list.length > 0) {
            document.getElementById('content').style.display = 'block';
        }
        this.allItems = response.list;
        this.applyFilter(document.getElementById("search-input").value);
    }

    itemKind(item) {
        if (item.isImage) return "image";
        let src = (item.url || item.info || item.text || "").toLowerCase().split("?")[0];
        let ext = src.slice(src.lastIndexOf(".") + 1);
        if (/m3u8/.test(src)) return "hls";
        if (/mpd$/.test(src)) return "dash";
        if (["jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "avif", "ico"].includes(ext)) return "image";
        if (["mp4", "mkv", "webm", "mov", "m4v", "avi", "ts", "m2ts", "flv", "mpg", "mpeg"].includes(ext)) return "video";
        if (["mp3", "m4a", "aac", "ogg", "opus", "flac", "wav", "weba"].includes(ext)) return "audio";
        if (["pdf", "zip", "rar", "7z", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "csv", "epub"].includes(ext)) return "doc";
        return "other";
    }

    reapplyFilter() {
        this.applyFilter(document.getElementById("search-input").value);
    }

    applyFilter(query) {
        let terms = (query || "").trim().toLowerCase().split(/\s+/).filter(t => t);
        let type = document.getElementById("filter-type").dataset.value;
        let hideTerms = document.getElementById("filter-hide").value.trim().toLowerCase().split(/[\s,]+/).filter(t => t);

        let items = (this.allItems || []).filter(item => {
            let haystack = [item.text, item.info, item.url, item.pageUrl].filter(Boolean).join(String.fromCharCode(10)).toLowerCase();
            if (!terms.every(term => haystack.includes(term))) return false;
            if (hideTerms.some(term => haystack.includes(term))) return false;
            if (type !== "all") {
                let k = this.itemKind(item);
                if (type === "video") { if (k !== "video" && k !== "hls" && k !== "dash") return false; }
                else if (k !== type) return false;
            }
            return true;
        });

        let active = type !== "all" || hideTerms.length > 0;
        document.getElementById("filter-toggle").classList.toggle("active", active);
        let hidden = (this.allItems || []).length - items.length;
        let status = document.getElementById("filter-status");
        if (active && hidden > 0) {
            status.style.display = "flex";
            document.getElementById("filter-count").textContent = items.length + " shown · " + hidden + " hidden";
        } else {
            status.style.display = "none";
        }
        this.renderList(items);
    }

    initDropdown(dd, onChange) {
        let btn = dd.querySelector(".dd-btn");
        let cur = dd.querySelector(".dd-cur");
        let items = [...dd.querySelectorAll(".dd-list li")];
        dd._sync = () => {
            let v = dd.dataset.value;
            items.forEach(li => {
                let on = li.dataset.value === v;
                li.setAttribute("aria-selected", on ? "true" : "false");
                if (on) cur.textContent = li.textContent;
            });
        };
        btn.addEventListener("click", e => {
            e.stopPropagation();
            document.querySelectorAll(".dd.open").forEach(o => { if (o !== dd) o.classList.remove("open"); });
            dd.classList.toggle("open");
        });
        items.forEach(li => li.addEventListener("click", () => {
            dd.dataset.value = li.dataset.value;
            dd.classList.remove("open");
            dd._sync();
            if (onChange) onChange();
        }));
        dd._sync();
    }

    // Split a URL so the origin (scheme + host) always stays fully visible; the
    // middle path collapses first, the trailing segment (filename) second. The
    // query string is dropped here — the full URL is in the tooltip / on copy.
    splitUrl(u) {
        try {
            const url = new URL(u);
            const path = url.pathname || "/";
            const slash = path.lastIndexOf("/");
            return { origin: url.origin, mid: path.slice(0, slash + 1), tail: path.slice(slash + 1) };
        } catch {
            const base = (u.split("?")[0]) || u;
            const slash = base.lastIndexOf("/");
            if (slash === -1) return { origin: "", mid: "", tail: base };
            return { origin: "", mid: base.slice(0, slash + 1), tail: base.slice(slash + 1) };
        }
    }

    hostOf(u) { try { return new URL(u).host; } catch { return ""; } }

    // The backend's `text` is sometimes the page URL and `info` the real name.
    rowTitleMeta(item) {
        const txt = (item.text || "").trim();
        const looksUrl = /^https?:\/\//i.test(txt);
        if (looksUrl && item.info) return { title: item.info, meta: this.hostOf(item.pageUrl || item.url) || txt };
        return {
            title: txt || item.info || "—",
            meta: (item.info && item.info !== txt) ? item.info : (this.hostOf(item.pageUrl || item.url) || ""),
        };
    }

    makeCopyableLine(label, value) {
        let line = document.createElement('div');
        line.className = 'copyable-line';
        const s = this.splitUrl(value);
        const lab = document.createElement('span'); lab.className = 'cl-label'; lab.textContent = label;
        const origin = document.createElement('span'); origin.className = 'cl-origin'; origin.textContent = s.origin;
        const mid = document.createElement('span'); mid.className = 'cl-mid'; mid.textContent = s.mid;
        const tail = document.createElement('span'); tail.className = 'cl-tail'; tail.textContent = s.tail;
        line.append(lab, origin, mid, tail);
        line.title = value + "\n(double-click to copy)";
        line.addEventListener('dblclick', async e => {
            e.stopPropagation();
            try {
                await navigator.clipboard.writeText(value);
                let original = line.innerHTML;
                line.textContent = "Copied!";
                setTimeout(() => { line.innerHTML = original; }, 700);
            } catch (err) {
                console.error("clipboard write failed", err);
            }
        });
        return line;
    }

    async loadPreview(listItem, col) {
        if (col.dataset.loaded) return;
        col.dataset.loaded = "1";

        let data = this.previewCache.get(listItem.id);
        if (data === undefined) {
            data = await new Promise(resolve => {
                chrome.runtime.sendMessage({ type: "preview", itemId: listItem.id }, response => {
                    const url = response && response.previewDataUrl;
                    resolve(url ? { url, kind: response.previewKind || "image" } : null);
                });
            });
            this.previewCache.set(listItem.id, data);
        }
        // The popup's extension CSP (object-src 'self') can't embed a data: PDF.
        if (!data || data.kind === "pdf") return;

        let img = document.createElement('img');
        img.className = 'thumb';
        img.alt = '';
        img.title = 'Open in a new tab';
        img.src = data.url;
        img.addEventListener('click', e => { e.stopPropagation(); chrome.tabs.create({ url: listItem.url }); });
        col.appendChild(img);
    }

    renderList(arr) {
        let list = document.getElementById("list");
        list.innerHTML = "";
        this.previewObserver.disconnect();

        if (arr.length === 0) {
            let empty = document.createElement('div');
            empty.className = 'empty';
            empty.innerText = (this.allItems && this.allItems.length > 0) ? "No matches." : "Nothing detected yet.";
            list.appendChild(empty);
            return;
        }

        arr.forEach(listItem => {
            let row = document.createElement('div');
            row.className = 'row';

            let toggle = document.createElement('button');
            toggle.className = 'toggle-btn';
            toggle.innerText = "▸";
            toggle.title = "Details";

            let previewCol = document.createElement('div');
            previewCol.className = 'row-preview';
            previewCol._loadPreview = () => this.loadPreview(listItem, previewCol);
            this.previewObserver.observe(previewCol);

            const tm = this.rowTitleMeta(listItem);
            let button = document.createElement('button');
            button.className = 'title-btn';
            button.id = listItem.id;
            button.innerText = tm.title;
            button.title = tm.title;

            let infoText = document.createElement('div');
            infoText.className = 'info-text';
            infoText.innerText = tm.meta;

            let body = document.createElement('div');
            body.className = 'row-body';
            body.append(button, infoText);

            let actions = document.createElement('div');
            actions.className = 'row-actions';
            let addBtn = document.createElement('button');
            addBtn.className = 'add-btn';
            addBtn.innerText = "Add";
            actions.appendChild(addBtn);

            let detailsPanel = document.createElement('div');
            detailsPanel.className = 'details-panel';
            detailsPanel.appendChild(this.makeCopyableLine("Site: ", listItem.pageUrl || "(unknown)"));
            detailsPanel.appendChild(this.makeCopyableLine("Full link: ", listItem.url));

            let qualityBox = document.createElement('div');
            qualityBox.className = 'quality-box';
            detailsPanel.appendChild(qualityBox);
            let qualityProbed = false;

            row.append(toggle, previewCol, body, actions, detailsPanel);
            list.appendChild(row);

            const queue = (btn, quality) => {
                const original = btn.innerText;
                const msg = { type: "vid", itemId: listItem.id };
                if (quality) msg.quality = quality;
                chrome.runtime.sendMessage(msg, response => {
                    const success = response && response.success;
                    btn.innerText = success === true ? "Added to Lüdd!"
                        : success === false ? "Failed - link may have expired"
                        : "Lüdd app not running";
                    setTimeout(() => { btn.innerText = original; }, 1500);
                });
            };
            button.addEventListener('click', () => queue(button));
            addBtn.addEventListener('click', () => queue(addBtn));

            toggle.addEventListener('click', e => {
                e.stopPropagation();
                let nowOpen = !detailsPanel.classList.contains('open');
                detailsPanel.classList.toggle('open', nowOpen);
                toggle.innerText = nowOpen ? "▾" : "▸";
                if (nowOpen && !qualityProbed) {
                    qualityProbed = true;
                    chrome.runtime.sendMessage({ type: "probe-quality", itemId: listItem.id }, response => {
                        const variants = (response && response.variants) || [];
                        qualityBox.innerHTML = "";
                        if (variants.length === 0) {
                            return;
                        }
                        let label = document.createElement('div');
                        label.innerText = "Choose a quality:";
                        label.style.marginBottom = "2px";
                        qualityBox.appendChild(label);
                        variants.forEach(variant => {
                            let qBtn = document.createElement('button');
                            qBtn.innerText = variant.label;
                            qBtn.addEventListener('click', ev => { ev.stopPropagation(); queue(qBtn, variant.variant_key); });
                            qualityBox.appendChild(qBtn);
                        });
                    });
                }
            });
        });
    }
}

var popup = new VideoPopup();
popup.run();
