class VideoPopup {
    run() {
        document.addEventListener('DOMContentLoaded', this.onLoad.bind(this), false);
    }

    onLoad() {
        document.getElementById('content').style.display = 'none';

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

    truncateMiddle(str, headLen = 28, tailLen = 18) {
        if (str.length <= headLen + tailLen + 3) return str;
        return str.slice(0, headLen) + "..." + str.slice(-tailLen);
    }

    makeCopyableLine(label, value) {
        let line = document.createElement('div');
        line.className = 'copyable-line';
        line.innerText = label + this.truncateMiddle(value);
        line.title = value + "\n(double-click to copy)";
        line.addEventListener('dblclick', async e => {
            e.stopPropagation();
            try {
                await navigator.clipboard.writeText(value);
                let original = line.innerText;
                line.innerText = "Copied!";
                setTimeout(() => { line.innerText = original; }, 700);
            } catch (err) {
                console.error("clipboard write failed", err);
            }
        });
        return line;
    }

    renderList(arr) {
        let list = document.getElementById("list");
        list.innerHTML = "";

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

            let preview = document.createElement('button');
            preview.className = 'preview-link';
            preview.innerText = "Preview";
            preview.title = "Open this link in a new tab to view it before downloading";

            let infoRow = document.createElement('div');
            infoRow.className = 'info-row';
            infoRow.appendChild(infoText);
            infoRow.appendChild(preview);

            let detailsPanel = document.createElement('div');
            detailsPanel.className = 'details-panel';
            detailsPanel.appendChild(this.makeCopyableLine("Site: ", listItem.pageUrl || "(unknown)"));
            detailsPanel.appendChild(this.makeCopyableLine("Full link: ", listItem.url));

            let qualityBox = document.createElement('div');
            qualityBox.className = 'quality-box';
            detailsPanel.appendChild(qualityBox);
            let qualityProbed = false;

            row.appendChild(titleRow);
            row.appendChild(infoRow);
            row.appendChild(detailsPanel);
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
            preview.addEventListener('click', e => {
                e.stopPropagation();
                chrome.tabs.create({ url: listItem.url });
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
