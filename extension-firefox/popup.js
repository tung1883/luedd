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

    applyFilter(query) {
        let terms = (query || "").trim().toLowerCase().split(/\s+/).filter(t => t);
        if (terms.length === 0) {
            this.renderList(this.allItems || []);
            return;
        }
        let items = (this.allItems || []).filter(item => {
            let haystack = [item.text, item.info, item.url, item.pageUrl].filter(Boolean).join(String.fromCharCode(10)).toLowerCase();
            return terms.every(term => haystack.includes(term));
        });
        this.renderList(items);
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

            let button = document.createElement('button');
            button.className = 'title-btn';
            button.innerText = listItem.text;
            button.id = listItem.id;

            let titleRow = document.createElement('div');
            titleRow.className = 'row-title';
            titleRow.appendChild(toggle);
            titleRow.appendChild(button);

            let infoText = document.createElement('span');
            infoText.className = 'info-text';
            infoText.innerText = listItem.info;

            let infoRow = document.createElement('div');
            infoRow.className = 'info-row';
            infoRow.appendChild(infoText);

            let detailsPanel = document.createElement('div');
            detailsPanel.className = 'details-panel';
            detailsPanel.appendChild(this.makeCopyableLine("Site: ", listItem.pageUrl || "(unknown)"));
            detailsPanel.appendChild(this.makeCopyableLine("Full link: ", listItem.url));

            let qualityBox = document.createElement('div');
            qualityBox.className = 'quality-box';
            detailsPanel.appendChild(qualityBox);
            let qualityProbed = false;

            let body = document.createElement('div');
            body.className = 'row-body';
            body.appendChild(titleRow);
            body.appendChild(infoRow);
            body.appendChild(detailsPanel);

            // Preview thumbnail column on the right; populated lazily when the row
            // scrolls into view, empty when there's nothing to show.
            let previewCol = document.createElement('div');
            previewCol.className = 'row-preview';
            previewCol._loadPreview = () => this.loadPreview(listItem, previewCol);
            this.previewObserver.observe(previewCol);

            row.appendChild(body);
            row.appendChild(previewCol);
            list.appendChild(row);

            button.addEventListener('click', e => {
                const original = button.innerText;
                chrome.runtime.sendMessage({ type: "vid", itemId: e.target.id }, response => {
                    const success = response && response.success;
                    button.innerText = success === true ? "Added to Lüdd!"
                        : success === false ? "Failed - link may have expired"
                        : "Lüdd app not running";
                    setTimeout(() => { button.innerText = original; }, 1500);
                });
            });
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
                            qBtn.addEventListener('click', ev => {
                                ev.stopPropagation();
                                const original = qBtn.innerText;
                                chrome.runtime.sendMessage({ type: "vid", itemId: listItem.id, quality: variant.variant_key }, res => {
                                    const success = res && res.success;
                                    qBtn.innerText = success === true ? "Added to Lüdd!"
                                        : success === false ? "Failed - link may have expired"
                                        : "Lüdd app not running";
                                    setTimeout(() => { qBtn.innerText = original; }, 1500);
                                });
                            });
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
