"use strict";
// On-page download button (IDM style). Hovering a <video>/<audio> shows a small
// "Download" pill at its top-left corner; the background script knows which
// media URLs the tab has loaded and queues the pick in Lüdd. In "all" mode a
// pill also sits at the viewport's top-left for any detected download (files,
// PDFs, images) on pages without a player. Mode lives in storage.local
// `barMode`: "video" (default) | "all" | "off".

(function () {
  if (window.__luddBar) return;
  window.__luddBar = true;
  const ext = (typeof browser !== "undefined" && browser.runtime) ? browser
    : (typeof chrome !== "undefined" && chrome.runtime) ? chrome : null;
  if (!ext) return;
  const isTop = window === window.top;

  let mode = "video";
  let disabled = false;
  let items = [];
  let itemsAt = 0;

  const send = msg => new Promise(resolve => {
    try {
      const p = ext.runtime.sendMessage(msg, r => { void ext.runtime.lastError; resolve(r); });
      if (p && typeof p.then === "function") p.then(resolve, () => resolve(null));
    } catch (_) { resolve(null); }
  });

  try {
    ext.storage.local.get(["barMode", "userDisabled"], r => {
      if (r && typeof r.barMode === "string") mode = r.barMode;
      if (r && typeof r.userDisabled === "boolean") disabled = r.userDisabled;
      refreshPill();
    });
    ext.storage.onChanged.addListener((ch, area) => {
      if (area !== "local") return;
      if (ch.barMode) mode = ch.barMode.newValue || "video";
      if (ch.userDisabled) disabled = ch.userDisabled.newValue === true;
      if (!active()) { videoBar.hide(); pillBar.hide(); } else refreshPill();
    });
  } catch (_) {}

  const active = () => mode !== "off" && !disabled;
  const isMedia = it => it.kind === "video" || it.kind === "audio" || it.kind === "stream";

  async function query(force) {
    if (!force && Date.now() - itemsAt < 2500) return;
    itemsAt = Date.now();
    const r = await send({ type: "bar-query" });
    items = (r && r.items) || [];
  }

  const CSS = `
    .bar { display:inline-flex; align-items:stretch; font:600 12px/1 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
      background:#16171A; color:#E7E8EA; border:1px solid #34373E; border-radius:7px; box-shadow:0 4px 14px rgba(0,0,0,.45);
      overflow:visible; position:relative; user-select:none; }
    svg { display:block; }
    button { all:unset; cursor:pointer; padding:7px 10px; display:inline-flex; align-items:center; gap:6px; color:inherit; }
    button:hover { background:#26282E; }
    .dl { color:#4FA3D8; border-radius:6px 0 0 6px; }
    .dl.ok { color:#4FD08A; } .dl.err { color:#F26A63; }
    .more, .x { color:#9A9DA5; padding:7px 7px; }
    .more { border-left:1px solid #34373E; } .x:hover, .more:hover { color:#E7E8EA; }
    .menu { position:absolute; left:0; top:calc(100% + 4px); min-width:220px; max-width:320px; background:#1E2024;
      border:1px solid #34373E; border-radius:8px; padding:4px; box-shadow:0 12px 30px rgba(0,0,0,.5); display:none; }
    .menu.open { display:block; }
    .hd { padding:6px 9px 3px; font-size:10px; font-weight:600; letter-spacing:.06em; text-transform:uppercase; color:#9A9DA5; }
    .note { padding:6px 9px; font-weight:400; color:#9A9DA5; }
    .menu button.sel { color:#4FA3D8; }
    .menu button { display:block; width:100%; box-sizing:border-box; padding:7px 9px; font-weight:400; border-radius:5px;
      white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  `;

  // One draggable-free pill. `pick()` returns the candidate list to offer.
  function makeBar(pick) {
    const host = document.createElement("div");
    host.style.cssText = "all:initial;position:fixed;left:0;top:0;z-index:2147483647;display:none;";
    const root = host.attachShadow({ mode: "closed" });
    root.innerHTML = `<style>${CSS}</style><div class="bar">
      <button class="dl" title="Download with Lüdd"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 4v10"/><path d="M7 10l5 5 5-5"/><path d="M5 20h14"/></svg> <span class="lbl">Download</span></button>
      <button class="more" title="Choose" style="display:none"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg></button>
      <button class="x" title="Hide"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 6l12 12"/><path d="M18 6L6 18"/></svg></button>
      <div class="menu"></div></div>`;
    const $ = s => root.querySelector(s);
    const dl = $(".dl"), more = $(".more"), menu = $(".menu"), lbl = $(".lbl");
    let cands = [];
    let cur = null;
    const probed = new Map();
    let hovered = false;
    let busy = false;

    async function download(it, quality) {
      if (busy) return;
      busy = true;
      menu.classList.remove("open");
      lbl.textContent = "Sending...";
      const r = await send({ type: "bar-download", url: it.url, quality });
      const ok = !!(r && r.ok);
      dl.classList.add(ok ? "ok" : "err");
      lbl.textContent = ok ? "Queued" : "Failed";
      setTimeout(() => { dl.classList.remove("ok", "err"); lbl.textContent = "Download"; busy = false; }, 1600);
    }

    dl.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); if (cur) download(cur); });
    more.addEventListener("click", e => {
      e.preventDefault(); e.stopPropagation();
      const open = menu.classList.toggle("open");
      if (open) renderMenu();
    });
    $(".x").addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); api.dismissed = true; api.hide(); });
    host.addEventListener("mouseenter", () => { hovered = true; });
    host.addEventListener("mouseleave", () => { hovered = false; });
    for (const ev of ["mousedown", "mouseup", "pointerdown", "dblclick"]) {
      host.addEventListener(ev, e => e.stopPropagation());
    }

    const mk = (tag, cls, text) => {
      const n = document.createElement(tag);
      if (cls) n.className = cls;
      if (text != null) n.textContent = text;
      return n;
    };

    // Menu = quality list for the current source, plus the other sources
    // (when the tab has several candidates) to switch to.
    async function renderMenu() {
      const it = cur;
      menu.textContent = "";
      if (!it) return;
      menu.appendChild(mk("div", "hd", "Quality"));
      const best = mk("button", "", "Best available");
      best.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); download(it); });
      menu.appendChild(best);
      const slot = mk("div", "note", probed.has(it.url) ? "" : "Checking qualities...");
      menu.appendChild(slot);
      if (cands.length > 1) {
        menu.appendChild(mk("div", "hd", "Source"));
        for (const c of cands) {
          const b = mk("button", c.url === it.url ? "sel" : "", c.name || c.url);
          b.title = c.url;
          b.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); cur = c; renderMenu(); });
          menu.appendChild(b);
        }
      }
      if (!probed.has(it.url)) {
        const r = await send({ type: "bar-probe", url: it.url });
        probed.set(it.url, (r && r.variants) || []);
        if (cur !== it || !menu.classList.contains("open")) return;
      }
      const variants = probed.get(it.url);
      if (!variants.length) { slot.remove(); return; }
      slot.remove();
      let ref = best;
      for (const v of variants) {
        const b = mk("button", "", v.label);
        b.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); download(it, v.variant_key); });
        ref.after(b);
        ref = b;
      }
    }

    function setCands(list) {
      const changed = list.length !== cands.length || list.some((c, i) => c.url !== cands[i].url);
      cands = list;
      if (!cur || !list.some(c => c.url === cur.url)) cur = list[0] || null;
      more.style.display = cur ? "" : "none";
      if (changed && menu.classList.contains("open")) renderMenu();
    }

    const api = {
      dismissed: false,
      el: host,
      get hovered() { return hovered; },
      visible: false,
      show(x, y, list) {
        if (api.dismissed || !list.length) return api.hide();
        if (!host.isConnected) (document.documentElement || document).appendChild(host);
        setCands(list);
        host.style.left = Math.max(0, Math.round(x)) + "px";
        host.style.top = Math.max(0, Math.round(y)) + "px";
        host.style.display = "block";
        api.visible = true;
      },
      hide() {
        menu.classList.remove("open");
        host.style.display = "none";
        api.visible = false;
      },
    };
    return api;
  }

  const videoBar = makeBar();
  const pillBar = makeBar();

  // ---- <video>/<audio> overlay -------------------------------------------
  function mediaElements() {
    const out = [];
    for (const el of document.querySelectorAll("video, audio")) {
      const r = el.getBoundingClientRect();
      const big = el.tagName === "VIDEO" ? (r.width >= 160 && r.height >= 90) : (el.controls && r.width >= 100 && r.height >= 20);
      if (!big || r.bottom < 0 || r.right < 0 || r.top > innerHeight || r.left > innerWidth) continue;
      const cs = getComputedStyle(el);
      if (cs.visibility === "hidden" || cs.display === "none") continue;
      out.push({ el, r });
    }
    return out;
  }

  function candidatesFor(el) {
    const list = items.filter(isMedia);
    const cur = el && el.currentSrc;
    const i = cur ? list.findIndex(it => it.url === cur) : -1;
    if (i > 0) list.unshift(list.splice(i, 1)[0]);
    return list;
  }

  let hideTimer = 0;
  let currentEl = null;
  let ticking = false;
  let lastX = -1, lastY = -1;

  function overVideo() {
    for (const m of mediaElements()) {
      const r = m.r;
      if (lastX >= r.left && lastX <= r.right && lastY >= r.top && lastY <= r.bottom) return m;
    }
    return null;
  }

  function place(m) {
    videoBar.show(m.r.left + 8, m.r.top + 8, candidatesFor(m.el));
  }

  async function tick() {
    ticking = false;
    if (!active()) return;
    const m = overVideo();
    if (m) {
      clearTimeout(hideTimer);
      if (currentEl !== m.el) { currentEl = m.el; videoBar.dismissed = false; }
      await query(false);
      const again = overVideo();
      if (again) place(again);
    } else if (videoBar.visible && !videoBar.hovered && !hideTimer) {
      hideTimer = setTimeout(() => { hideTimer = 0; if (!videoBar.hovered && !overVideo()) { videoBar.hide(); currentEl = null; } }, 700);
    }
  }

  document.addEventListener("mousemove", e => {
    lastX = e.clientX; lastY = e.clientY;
    if (!ticking) { ticking = true; requestAnimationFrame(tick); }
  }, { capture: true, passive: true });

  const follow = () => {
    if (!videoBar.visible || !currentEl) return;
    const r = currentEl.getBoundingClientRect();
    if (r.bottom < 0 || r.top > innerHeight) return videoBar.hide();
    place({ el: currentEl, r });
  };
  addEventListener("scroll", () => requestAnimationFrame(follow), { capture: true, passive: true });
  addEventListener("resize", () => requestAnimationFrame(follow), { passive: true });

  // ---- "all" mode corner pill --------------------------------------------
  function refreshPill() {
    if (!isTop || !active()) return pillBar.hide();
    if (mode !== "all") return pillBar.hide();
    if (!items.length || mediaElements().length) return pillBar.hide();
    pillBar.show(12, 12, items);
  }

  try {
    ext.runtime.onMessage.addListener(msg => {
      if (msg && msg.type === "bar-changed") {
        query(true).then(refreshPill);
      }
    });
  } catch (_) {}

  if (isTop) {
    const boot = () => query(true).then(refreshPill);
    if (document.readyState === "complete") boot(); else addEventListener("load", boot, { once: true });
  }
})();
