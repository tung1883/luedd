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
  let monitoring = false;

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
    monitoring = !!(r && r.enabled);
  }

  const CSS = `
    .bar { display:inline-flex; align-items:stretch; font:600 12px/1 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
      background:#16171A; color:#E7E8EA; border:1px solid #34373E; border-radius:7px; box-shadow:0 4px 14px rgba(0,0,0,.45);
      overflow:visible; position:relative; user-select:none; }
    svg { display:block; }
    .logo { width:16px; height:16px; display:block; border-radius:4px; }
    button { all:unset; cursor:pointer; padding:7px 10px; display:inline-flex; align-items:center; gap:6px; color:inherit; }
    button:hover { background:#26282E; }
    .dl { color:#4FA3D8; border-radius:6px 0 0 6px; }
    .dl.ok { color:#4FD08A; } .dl.err { color:#F26A63; }
    .more, .x { color:#9A9DA5; padding:7px 7px; }
    .more { border-left:1px solid #34373E; } .x:hover, .more:hover { color:#E7E8EA; }
    .menu { position:absolute; left:0; top:calc(100% + 4px); width:max-content; min-width:100%; max-width:320px; background:#1E2024;
      border:1px solid #34373E; border-radius:8px; padding:4px; box-shadow:0 12px 30px rgba(0,0,0,.5); display:none; }
    .menu.open { display:block; }
    .hd { padding:6px 9px 3px; font-size:10px; font-weight:600; letter-spacing:.06em; text-transform:uppercase; color:#9A9DA5; }
    .note { padding:6px 9px; font-weight:400; color:#9A9DA5; }
    .menu button.sel { color:#4FA3D8; }
    .menu button { display:block; position:relative; width:100%; box-sizing:border-box; padding:7px 9px 7px 22px; font-weight:400; border-radius:5px;
      white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .menu button::before { content:""; position:absolute; left:10px; top:50%; width:5px; height:5px; margin-top:-2.5px;
      border-radius:50%; background:#4FA3D8; }
  `;

  // One draggable-free pill. `pick()` returns the candidate list to offer.
  const probeCache = new Map();
  const IS_IG = /(^|\.)instagram\.com$/.test(location.hostname);
  let iconUrl = "";
  try { iconUrl = ext.runtime.getURL("icon48.png"); } catch (_) {}

  function makeBar(pick) {
    const host = document.createElement("div");
    host.style.cssText = "all:initial;position:fixed;left:0;top:0;z-index:2147483647;display:none;";
    const root = host.attachShadow({ mode: "closed" });
    root.innerHTML = `<style>${CSS}</style><div class="bar">
      <button class="dl" title="Download with Lüdd"><img class="logo" alt="" src="${iconUrl}"> <span class="lbl">Download</span></button>
      <button class="more" title="Choose" style="display:none"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg></button>
      <button class="x" title="Hide"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 6l12 12"/><path d="M18 6L6 18"/></svg></button>
      <div class="menu"></div></div>`;
    const $ = s => root.querySelector(s);
    const dl = $(".dl"), more = $(".more"), menu = $(".menu"), lbl = $(".lbl");
    let cands = [];
    let cur = null;
    const probed = probeCache;
    let hovered = false;
    let busy = false;

    async function download(it, quality, scope) {
      if (busy) return;
      busy = true;
      menu.classList.remove("open");
      lbl.textContent = "Sending...";
      const r = await send({ type: "bar-download", url: it.url, quality, slide: scope === "this" ? it.slide : undefined });
      const ok = !!(r && r.ok);
      dl.classList.add(ok ? "ok" : "err");
      lbl.textContent = ok ? "Queued" : "Failed";
      setTimeout(() => { dl.classList.remove("ok", "err"); lbl.textContent = "Download"; busy = false; }, 1600);
    }

    // The menu only exists when it has something beyond "Best available":
    // carousel choices, real quality variants, or several sources.
    const noQuality = it => IS_IG || (probed.has(it.url) && !probed.get(it.url).length);
    function updateCaret() {
      more.style.display = cur && (cur.slide || cands.length > 1 || !noQuality(cur)) ? "" : "none";
    }

    dl.addEventListener("click", e => {
      e.preventDefault(); e.stopPropagation();
      if (!cur) return;
      if (cur.slide) {
        const open = menu.classList.toggle("open");
        if (open) renderMenu();
        return;
      }
      download(cur);
    });
    more.addEventListener("click", e => {
      e.preventDefault(); e.stopPropagation();
      const open = menu.classList.toggle("open");
      if (open) renderMenu();
    });
    $(".x").addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); api.dismissed = true; api.hide(); });
    for (const ev of ["pointerdown", "mousedown"]) {
      window.addEventListener(ev, e => {
        if (menu.classList.contains("open") && !e.composedPath().includes(host)) menu.classList.remove("open");
      }, true);
    }
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
    const addBtn = (parent, label, fn, cls) => {
      const b = mk("button", cls || "", label);
      b.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); fn(); });
      parent.appendChild(b);
      return b;
    };

    // Menu = carousel choices, quality list (only when variants exist) and the
    // other sources (when the tab has several candidates).
    async function renderMenu() {
      const it = cur;
      menu.textContent = "";
      if (!it) return;
      if (it.slide) {
        addBtn(menu, "Download this", () => download(it, undefined, "this"));
        addBtn(menu, "Download all", () => download(it, undefined, "all"));
      }
      const qslot = mk("div", "");
      menu.appendChild(qslot);
      if (cands.length > 1) {
        menu.appendChild(mk("div", "hd", "Source"));
        for (const c of cands) {
          const b = addBtn(menu, c.name || c.url, () => { cur = c; renderMenu(); }, c.url === it.url ? "sel" : "");
          b.title = c.url;
        }
      }
      const fill = variants => {
        qslot.textContent = "";
        if (!variants.length) return;
        qslot.appendChild(mk("div", "hd", "Quality"));
        addBtn(qslot, "Best available", () => download(it));
        for (const v of variants) addBtn(qslot, v.label, () => download(it, v.variant_key));
      };
      if (!it.slide && cands.length <= 1 && noQuality(it)) {
        menu.classList.remove("open");
        updateCaret();
        return;
      }
      if (IS_IG) return;
      if (probed.has(it.url)) return fill(probed.get(it.url));
      qslot.appendChild(mk("div", "note", "Checking qualities..."));
      const r = await send({ type: "bar-probe", url: it.url });
      probed.set(it.url, (r && r.variants) || []);
      if (cur !== it) return;
      fill(probed.get(it.url));
      if (!probed.get(it.url).length && !it.slide && cands.length <= 1) {
        menu.classList.remove("open");
      }
      updateCaret();
    }

    function setCands(list) {
      const changed = list.length !== cands.length || list.some((c, i) => c.url !== cands[i].url);
      cands = list;
      // keep the chosen source, but take the freshest object (slide info moves)
      cur = (cur && list.find(c => c.url === cur.url)) || list[0] || null;
      updateCaret();
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
  // A large open dialog (Instagram's post overlay, a lightbox) sits above the
  // page: media underneath it must not get a pill.
  function modalRoot() {
    let found = null;
    for (const d of document.querySelectorAll('[role="dialog"], [aria-modal="true"], dialog[open]')) {
      const r = d.getBoundingClientRect();
      if (r.width * r.height < innerWidth * innerHeight * 0.25) continue;
      if (getComputedStyle(d).visibility === "hidden") continue;
      found = d;
    }
    return found;
  }

  function mediaElements() {
    const out = [];
    const modal = modalRoot();
    for (const el of document.querySelectorAll(IS_IG ? "video, audio, img" : "video, audio")) {
      const r = el.getBoundingClientRect();
      let big;
      if (el.tagName === "IMG") {
        // post / story / grid images only; not avatars or icons
        big = r.width >= 200 && r.height >= 200 && !/profile picture/i.test(el.alt || "");
      } else {
        big = el.tagName === "VIDEO" ? (r.width >= 160 && r.height >= 90) : (el.controls && r.width >= 100 && r.height >= 20);
      }
      if (!big || r.bottom < 0 || r.right < 0 || r.top > innerHeight || r.left > innerWidth) continue;
      if (modal && !modal.contains(el)) continue;
      const cs = getComputedStyle(el);
      if (cs.visibility === "hidden" || cs.display === "none") continue;
      out.push({ el, r });
    }
    return out;
  }

  // Feed sites show many videos on one URL, so the tab-level item is the wrong
  // download for the hovered one. Find that video's own post link instead.
  const plain = u => u.origin + u.pathname;
  const PERMALINKS = [
    { host: /(^|\.)instagram\.com$/, path: /^\/(?:[^/]+\/)?(?:p|reel|reels|tv)\/(?!(?:audio|tags|locations|explore)(?:\/|$))[\w-]+|^\/stories\/(?:highlights\/\d+|[^/]+)/, clean: plain },
    { host: /(^|\.)(x|twitter)\.com$/, path: /^\/[^/]+\/status\/\d+/, clean: plain },
    { host: /(^|\.)tiktok\.com$/, path: /^\/@[^/]+\/video\/\d+/, clean: plain },
    { host: /(^|\.)facebook\.com$/, path: /^\/(?:[^/]+\/)?(?:reel|videos)\/\d+|^\/watch\/?$/,
      clean: u => u.pathname.startsWith("/watch") ? u.origin + u.pathname + u.search : plain(u) },
    // Only the main player (and Shorts) use the tab URL; hover previews on
    // home/search/sidebar thumbnails resolve to the thumbnail's own link.
    { host: /(^|\.)youtube\.com$/, path: /^\/(?:watch$|shorts\/[\w-]+)/, mainSel: "#movie_player, ytd-shorts, ytd-reel-video-renderer",
      ok: u => u.pathname.startsWith("/shorts/") || u.searchParams.has("v"),
      clean: u => u.pathname.startsWith("/shorts/") ? plain(u) : u.origin + u.pathname + "?v=" + u.searchParams.get("v") },
  ];

  function permalinkFor(el) {
    const rule = PERMALINKS.find(r => r.host.test(location.hostname));
    if (!rule) return null;
    const match = u => rule.path.test(u.pathname) && (!rule.ok || rule.ok(u));
    const here = new URL(location.href);
    if (match(here) && (!rule.mainSel || (el.closest && el.closest(rule.mainSel)))) return rule.clean(here);
    const find = root => {
      for (const a of root.querySelectorAll("a[href]")) {
        let u;
        try { u = new URL(a.getAttribute("href"), location.href); } catch (_) { continue; }
        if (u.hostname === location.hostname && match(u)) return rule.clean(u);
      }
      return null;
    };
    // The post container first: players sit ~20 levels below it, so a short
    // ancestor walk misses the link in the header/timestamp.
    const scope = el.closest && el.closest('article, [role="dialog"]');
    if (scope) {
      const hit = find(scope);
      if (hit) return hit;
    }
    let node = el;
    for (let i = 0; i < 40 && node && node !== document.body; i++) {
      node = node.parentElement;
      if (!node) break;
      const hit = find(node);
      if (hit) return hit;
    }
    return null;
  }

  // Instagram carousel: describe the hovered slide and its rendered
  // neighbours (the track is virtualised, so DOM position alone is not an
  // index; the background matches file names against the post's media list).
  function carouselSlide(el) {
    const scope = el.closest && el.closest('article, [role="dialog"], main');
    if (!scope || !scope.querySelector('[aria-label="Next"], [aria-label="Go back"]')) return null;
    const li = el.closest("li");
    const ul = li && li.parentElement;
    if (!ul) return null;
    const slides = [...ul.children].filter(x => x.querySelector("img, video"));
    const idx = slides.indexOf(li);
    if (idx < 0) return null;
    const baseName = u => (u || "").split("?")[0].split("/").pop();
    return {
      slides: slides.map((sl, i) => {
        const v = sl.querySelector("video");
        const im = sl.querySelector("img");
        return { offset: i - idx, video: !!v, base: im && !v ? baseName(im.currentSrc || im.src) : "" };
      }),
    };
  }

  function candidatesFor(el) {
    const perma = monitoring && el && permalinkFor(el);
    if (perma) return [{ url: perma, name: "This post", kind: "video", page: true, slide: IS_IG ? carouselSlide(el) : null }];
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

  // The part of `el` actually on screen: carousels render neighbouring slides
  // under an overflow:hidden parent (e.g. beneath the caption panel), and their
  // raw rects would otherwise swallow hovers over unrelated UI.
  function visibleRect(el) {
    const r = el.getBoundingClientRect();
    let left = r.left, top = r.top, right = r.right, bottom = r.bottom;
    let n = el.parentElement;
    for (let i = 0; n && n !== document.documentElement && i < 40; i++, n = n.parentElement) {
      const cs = getComputedStyle(n);
      if (cs.overflowX === "visible" && cs.overflowY === "visible") continue;
      const p = n.getBoundingClientRect();
      left = Math.max(left, p.left); top = Math.max(top, p.top);
      right = Math.min(right, p.right); bottom = Math.min(bottom, p.bottom);
    }
    return { left, top, right, bottom, width: right - left, height: bottom - top };
  }

  function overVideo() {
    for (const m of mediaElements()) {
      const raw = m.r;
      if (!(lastX >= raw.left && lastX <= raw.right && lastY >= raw.top && lastY <= raw.bottom)) continue;
      const r = visibleRect(m.el);
      if (lastX >= r.left && lastX <= r.right && lastY >= r.top && lastY <= r.bottom) return { el: m.el, r };
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
    const modal = modalRoot();
    if (modal && !modal.contains(currentEl)) return videoBar.hide();
    const r = visibleRect(currentEl);
    if (r.bottom < 0 || r.top > innerHeight || r.width <= 0 || r.height <= 0) return videoBar.hide();
    place({ el: currentEl, r });
  };
  addEventListener("scroll", () => requestAnimationFrame(follow), { capture: true, passive: true });
  addEventListener("resize", () => requestAnimationFrame(follow), { passive: true });

  // ---- Instagram profile page pill ----------------------------------------
  // On /<user>/ a pill at the avatar offers the profile picture and a
  // newest / full archive of the account.
  const IG_RESERVED = new Set(["explore", "reels", "direct", "accounts", "stories", "p", "reel", "tv", "about", "legal", "web"]);

  function profileUser() {
    if (!IS_IG) return null;
    const m = location.pathname.match(/^\/([A-Za-z0-9._]+)\/?$/);
    return m && !IG_RESERVED.has(m[1].toLowerCase()) ? m[1] : null;
  }

  function makeActionBar(actions) {
    const host = document.createElement("div");
    host.style.cssText = "all:initial;position:fixed;left:0;top:0;z-index:2147483647;display:none;";
    const root = host.attachShadow({ mode: "closed" });
    root.innerHTML = `<style>${CSS}
      .menu button.sub { padding-left:34px; }
      .menu button.sub::before { left:22px; }
      .menu button.parent { display:flex; align-items:center; }
      .menu button.parent::after { content:""; display:block; flex:none; margin-left:auto; width:5px; height:5px; border-right:2px solid #9A9DA5;
        border-bottom:2px solid #9A9DA5; transform:rotate(-45deg); }
      .menu button.parent.open::after { transform:rotate(45deg); }
    </style><div class="bar">
      <button class="dl" title="Download with L\u00fcdd"><img class="logo" alt="" src="${iconUrl}"> <span class="lbl">Download</span></button>
      <button class="more" title="Options"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg></button>
      <button class="x" title="Hide"><svg viewBox="0 0 24 24" width="10" height="10" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 6l12 12"/><path d="M18 6L6 18"/></svg></button>
      <div class="menu"></div></div>`;
    const more = root.querySelector(".more");
    const dl = root.querySelector(".dl"), menu = root.querySelector(".menu"), lbl = root.querySelector(".lbl");
    let busy = false;
    const run = async action => {
      if (busy) return;
      busy = true;
      menu.classList.remove("open");
      lbl.textContent = "Sending...";
      const r = await send({ type: "ig-profile", username: api.user, action });
      const ok = !!(r && r.ok);
      dl.classList.add(ok ? "ok" : "err");
      lbl.textContent = ok ? "Queued" : "Failed";
      setTimeout(() => { dl.classList.remove("ok", "err"); lbl.textContent = "Download"; busy = false; }, 1600);
    };
    const btn = (label, cls, fn) => {
      const b = document.createElement("button");
      b.textContent = label;
      if (cls) b.className = cls;
      b.addEventListener("click", e => { e.preventDefault(); e.stopPropagation(); fn(b); });
      menu.appendChild(b);
      return b;
    };
    for (const a of actions) {
      if (!a.sub) { btn(a.label, "", () => run(a.action)); continue; }
      const subs = [];
      btn(a.label, "parent", self => {
        const open = self.classList.toggle("open");
        subs.forEach(x => { x.style.display = open ? "" : "none"; });
      });
      for (const sa of a.sub) {
        const sb = btn(sa.label, "sub", () => run(sa.action));
        sb.style.display = "none";
        subs.push(sb);
      }
    }
    for (const ev of ["pointerdown", "mousedown"]) {
      window.addEventListener(ev, e => {
        if (menu.classList.contains("open") && !e.composedPath().includes(host)) menu.classList.remove("open");
      }, true);
    }
    const toggle = e => { e.preventDefault(); e.stopPropagation(); menu.classList.toggle("open"); };
    dl.addEventListener("click", toggle);
    more.addEventListener("click", toggle);
    root.querySelector(".x").addEventListener("click", e => {
      e.preventDefault(); e.stopPropagation();
      api.dismissedFor = api.user;
      api.hide();
    });
    for (const ev of ["mousedown", "mouseup", "pointerdown", "dblclick"]) host.addEventListener(ev, e => e.stopPropagation());
    const api = {
      user: null,
      dismissedFor: null,
      visible: false,
      show(x, y) {
        if (!host.isConnected) (document.documentElement || document).appendChild(host);
        host.style.left = Math.max(0, Math.round(x)) + "px";
        host.style.top = Math.max(0, Math.round(y)) + "px";
        host.style.display = "block";
        api.visible = true;
      },
      hide() { menu.classList.remove("open"); host.style.display = "none"; api.visible = false; },
    };
    return api;
  }

  const profileBar = IS_IG && isTop ? makeActionBar([
    { label: "Profile picture", action: "pic" },
    { label: "Profile download", sub: [
      { label: "Newest posts", action: "recent" },
      { label: "All posts", action: "all" },
    ] },
  ]) : null;

  function refreshProfileBar() {
    if (!profileBar) return;
    const user = active() && monitoring ? profileUser() : null;
    const av = user && document.querySelector('header img[alt*="profile picture" i], header img');
    const r = av && av.getBoundingClientRect();
    if (!r || r.width < 40 || r.bottom < 0 || r.top > innerHeight) return profileBar.hide();
    if (profileBar.dismissedFor === user) return profileBar.hide();
    profileBar.user = user;
    profileBar.show(r.left + 6, r.top + 6);
  }

  if (profileBar) {
    setInterval(() => { if (document.visibilityState === "visible") query(false).then(refreshProfileBar); }, 1500);
    addEventListener("scroll", () => requestAnimationFrame(refreshProfileBar), { capture: true, passive: true });
  }

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
