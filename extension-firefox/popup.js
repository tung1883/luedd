class VideoPopup {
    run() {
        document.addEventListener('DOMContentLoaded', this.onLoad.bind(this), false);
    }

    onLoad() {
        document.getElementById('content').style.display = 'none';
        chrome.runtime.sendMessage({ type: "stat" }, this.onMsg.bind(this));

        document.getElementById("chk").addEventListener('click', (e) => {
            chrome.runtime.sendMessage({ type: "cmd", enabled: document.getElementById("chk").checked });
            window.close();
        });

        // Best-effort: only works once the tidm+app:// protocol handler is
        // registered on this machine (a manual, one-time OS-level step, not
        // something the extension can do for you) - if it isn't, this is a
        // silent no-op rather than an error.
        document.getElementById("open-app").addEventListener('click', () => {
            window.open("tidm+app://launch");
        });

        document.getElementById("search-input").addEventListener('input', e => {
            this.applyFilter(e.target.value);
        });
    }

    onMsg(response) {
        document.getElementById("chk").checked = response.enabled;
        let button = document.getElementById('clear');
        button.addEventListener('click', e => {
            chrome.runtime.sendMessage({ type: "clear" });
            window.close();
        });
        document.getElementById('format').addEventListener('click', e => {
            alert("Please play the video in desired format in web player")
        });
        if (response.list.length > 0) {
            document.getElementById('content').style.display = 'block';
        }
        // Kept so typing in the search box can re-filter without another
        // round-trip to the background page.
        this.allItems = response.list;
        this.applyFilter(document.getElementById("search-input").value);
    }

    // Matches against the visible filename/title, the site address, and the
    // raw link - not just the displayed text, since a search for a distinctive
    // part of the URL (a video id, say) should still find it even though the
    // list shows a friendlier filename instead of the link (see server.rs's
    // `suggest_filename`). Split into whitespace-separated terms and require
    // each to appear *somewhere* in the combined text - typing "playlist
    // m3u8" should still find "playlist.m3u8" even though the query's own
    // space doesn't literally appear in the filename; treating the whole
    // query as one exact substring (the previous behavior) missed this.
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

    renderList(arr) {
        let table = document.getElementById("table");
        table.innerHTML = "";

        if (arr.length === 0 && this.allItems && this.allItems.length > 0) {
            let row = table.insertRow(0);
            let cell = row.insertCell(0);
            cell.innerText = "No matches.";
            cell.setAttribute("style", "padding: 15px; color: #888; font-family:helvetica,arial,courier; font-size: 12px;");
            return;
        }

        arr.forEach(listItem => {
            let text = listItem.text;

            // `info` is the filename this would be saved as (see server.rs's
            // `suggest_filename`), not the raw source link - a CDN token URL
            // tells the user nothing useful to look at.
            let info = listItem.info;
            let id = listItem.id;

            let row = table.insertRow(0);
            let cell = row.insertCell(0);

            let border = "";

            let div = document.createElement('div');
            div.setAttribute("style", "padding: 10px; display: flex; flex-direction: column;" + border);

            let details = document.createElement('button');
            details.setAttribute("style", "font-family:helvetica,arial,courier; font-size: 12px; cursor: pointer; border: none; background: rgba(0,0,0,0); color: #888; padding: 0px; flex-shrink: 0; width: 14px;");
            details.innerText = "▸";
            details.title = "Details";

            let button = document.createElement('button');
            button.setAttribute("style", "font-family:helvetica,arial,courier; font-size: 14px; cursor: pointer; text-align: left; border: none; background: rgba(0,0,0,0); padding: 0px; padding-bottom: 5px; padding-top: 5px; flex: 1;");
            button.innerText = text;
            button.id = listItem.id;

            let titleRow = document.createElement('div');
            titleRow.setAttribute("style", "display: flex; align-items: center; gap: 6px;");
            titleRow.appendChild(details);
            titleRow.appendChild(button);

            let infoRow = document.createElement('div');
            infoRow.setAttribute("style", "display: flex; align-items: center; justify-content: space-between; gap: 8px; padding-left: 20px;");

            let p2 = document.createElement('span');
            p2.setAttribute("style", "font-family:helvetica,arial,courier; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;");
            let node = document.createTextNode(info);
            p2.appendChild(node);

            let preview = document.createElement('button');
            preview.setAttribute("style", "font-family:helvetica,arial,courier; font-size: 11px; cursor: pointer; border: none; background: rgba(0,0,0,0); color: #06c; padding: 0px; flex-shrink: 0;");
            preview.innerText = "Preview";
            preview.title = "Open this link in a new tab to view it before downloading";

            infoRow.appendChild(p2);
            infoRow.appendChild(preview);

            let detailsPanel = document.createElement('div');
            detailsPanel.setAttribute("style", "display: none; font-family:helvetica,arial,courier; font-size: 11px; color: #aaa; background: rgba(255,255,255,0.05); border-radius: 4px; padding: 6px 8px; margin-top: 4px; margin-left: 20px;");
            let siteLine = document.createElement('div');
            siteLine.innerText = "Site: " + (listItem.pageUrl || "(unknown)");
            siteLine.setAttribute("style", "word-break: break-all; margin-bottom: 4px;");
            let linkLine = document.createElement('div');
            linkLine.innerText = "Full link: " + listItem.url;
            linkLine.setAttribute("style", "word-break: break-all; user-select: text;");
            detailsPanel.appendChild(siteLine);
            detailsPanel.appendChild(linkLine);

            // Quality picker - probed lazily the first time this row's
            // details panel is opened, not eagerly for every detected item
            // (that would mean fetching every manifest on the page just to
            // show a badge count). The plain title button above keeps
            // queuing the auto-best variant instantly with no probe at all -
            // this is purely an opt-in slower path for someone who expands
            // the row on purpose.
            let qualityBox = document.createElement('div');
            qualityBox.setAttribute("style", "margin-top: 6px; display: flex; flex-direction: column; gap: 3px;");
            detailsPanel.appendChild(qualityBox);
            let qualityProbed = false;

            div.appendChild(titleRow);
            div.appendChild(infoRow);
            div.appendChild(detailsPanel);

            cell.appendChild(div);

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
            details.addEventListener('click', e => {
                e.stopPropagation();
                let nowOpen = detailsPanel.style.display === "none";
                detailsPanel.style.display = nowOpen ? "block" : "none";
                details.innerText = nowOpen ? "▾" : "▸";
                if (nowOpen && !qualityProbed) {
                    qualityProbed = true;
                    qualityBox.innerText = "Checking quality options…";
                    chrome.runtime.sendMessage({ type: "probe-quality", itemId: listItem.id }, response => {
                        const variants = (response && response.variants) || [];
                        qualityBox.innerHTML = "";
                        if (variants.length === 0) {
                            // Nothing to pick from (plain HTTP, or only one
                            // rendition) - leave the box empty rather than
                            // showing a message for the common case.
                            return;
                        }
                        let label = document.createElement('div');
                        label.innerText = "Choose a quality:";
                        label.setAttribute("style", "margin-bottom: 2px;");
                        qualityBox.appendChild(label);
                        variants.forEach(variant => {
                            let qBtn = document.createElement('button');
                            qBtn.innerText = variant.label;
                            qBtn.setAttribute("style", "font-family:helvetica,arial,courier; font-size: 11px; cursor: pointer; text-align: left; border: none; background: rgba(0,0,0,0); color: #06c; padding: 1px 0;");
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


