// Lüdd-Docs page adapters: Google Drive viewer / Slides, Scribd, Studocu.
// Loaded before content-bar.js (same isolated world); exposes window.__ludddDocs.
// An adapter knows how to recognise its viewer and how to list the page images.
// Page items are { url } (the background fetches it with the browser session) or
// { data } (a JPEG data URL rasterized here, for SVG / canvas pages).
(() => {
  if (window.__ludddDocs) return;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const TARGET_W = 2400;

  const cleanTitle = (t, tails) => {
    let s = (t || "").trim();
    for (const re of tails) s = s.replace(re, "");
    return s.trim() || "document";
  };

  // ---- scrolling: lazy pages only exist once they were on screen -----------
  function scrollables() {
    const out = [document.scrollingElement || document.documentElement];
    for (const el of document.querySelectorAll("div, section, main, ul")) {
      if (el.scrollHeight > el.clientHeight + 200 && el.clientHeight > 150) {
        const oy = getComputedStyle(el).overflowY;
        if (oy === "auto" || oy === "scroll") out.push(el);
      }
    }
    return out;
  }

  // Walk every scroller to the bottom in steps until `count()` stops growing.
  async function scrollAll(count) {
    let stable = 0, last = -1;
    const els = scrollables();
    for (let round = 0; round < 600 && stable < 4; round++) {
      let moved = false;
      for (const el of els) {
        const before = el.scrollTop;
        el.scrollTop = before + Math.max(300, el.clientHeight * 0.8);
        if (el.scrollTop !== before) moved = true;
      }
      await sleep(moved ? 250 : 500);
      const c = count();
      if (c === last && !moved) stable++; else if (c !== last) stable = 0;
      last = c;
    }
    for (const el of els) el.scrollTop = 0;
    await sleep(200);
  }

  // ---- rasterize an SVG element (Slides filmstrip) at TARGET_W ------------
  async function svgToJpeg(svg) {
    let vw = 364, vh = 205;
    const vb = (svg.getAttribute("viewBox") || "").trim().split(/[\s,]+/).map(Number);
    if (vb.length >= 4 && vb[2] > 0 && vb[3] > 0) { vw = vb[2]; vh = vb[3]; }
    else {
      const w = Number(svg.getAttribute("width")), h = Number(svg.getAttribute("height"));
      if (w > 0 && h > 0) { vw = w; vh = h; }
    }
    const tw = TARGET_W, th = Math.max(2, Math.round(tw * vh / vw));
    const clone = svg.cloneNode(true);
    if (!clone.getAttribute("xmlns")) clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    clone.setAttribute("width", String(tw));
    clone.setAttribute("height", String(th));
    const xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + new XMLSerializer().serializeToString(clone);
    const url = URL.createObjectURL(new Blob([xml], { type: "image/svg+xml;charset=utf-8" }));
    try {
      const img = new Image();
      await new Promise((res, rej) => { img.onload = res; img.onerror = () => rej(new Error("svg")); img.src = url; });
      const c = document.createElement("canvas");
      c.width = tw; c.height = th;
      const g = c.getContext("2d", { alpha: false });
      g.fillStyle = "#fff"; g.fillRect(0, 0, tw, th);
      g.drawImage(img, 0, 0, tw, th);
      return c.toDataURL("image/jpeg", 0.95);
    } finally { URL.revokeObjectURL(url); }
  }

  // ---- Google Drive viewer (/viewer/img thumbnails) + Slides filmstrip -----
  const driveThumbs = () => [...document.querySelectorAll('img[src*="/viewer/img"]')];
  const slideThumbs = () => [...document.querySelectorAll("g.punch-filmstrip-thumbnail svg")];
  const drive = {
    site: "Google Drive",
    detect: () => /(^|\.)(drive|docs)\.google\.com$/.test(location.hostname) && (driveThumbs().length > 0 || slideThumbs().length > 0),
    title: () => cleanTitle(document.title, [/\s*-\s*Google (Drive|Slides|Docs)\s*$/i]),
    async collect() {
      await scrollAll(() => driveThumbs().length + slideThumbs().length);
      const byKey = new Map();
      for (const img of driveThumbs()) {
        let u;
        try { u = new URL(img.src, location.href); } catch (_) { continue; }
        u.searchParams.set("w", String(TARGET_W));
        const key = (u.searchParams.get("id") || "") + "\n" + (u.searchParams.get("page") || "");
        if (!byKey.has(key)) byKey.set(key, { url: u.href, page: parseInt(u.searchParams.get("page") || "0", 10) });
      }
      const pages = [...byKey.values()].sort((a, b) => a.page - b.page).map(p => ({ url: p.url }));
      if (pages.length) return pages;
      const out = [];
      for (const svg of slideThumbs()) {
        try { out.push({ data: await svgToJpeg(svg) }); } catch (_) { }
      }
      return out;
    },
    count: () => driveThumbs().length + slideThumbs().length,
  };

  // ---- generic big-image viewers (Scribd, Studocu) -------------------------
  // Page images are the large <img>/<canvas> nodes of the document body. Scribd
  // scans and Studocu pages render this way; text-only Scribd pages have no
  // page image, which is what the "text" (print) mode is for.
  const bigImgs = () => [...document.querySelectorAll("img")].filter(i =>
    (i.naturalWidth >= 500 && i.naturalHeight >= 500) || (i.width >= 500 && i.height >= 500)).filter(i => i.currentSrc || i.src);
  const bigCanvases = () => [...document.querySelectorAll("canvas")].filter(c => c.width >= 500 && c.height >= 500);
  const imagePages = async () => {
    await scrollAll(() => bigImgs().length + bigCanvases().length);
    const nodes = [...bigImgs(), ...bigCanvases()].sort((a, b) =>
      (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) ? -1 : 1);
    const seen = new Set(), out = [];
    for (const n of nodes) {
      if (n.tagName === "CANVAS") {
        try { out.push({ data: n.toDataURL("image/jpeg", 0.95) }); } catch (_) { }
        continue;
      }
      const src = n.currentSrc || n.src;
      if (!src || seen.has(src)) continue;
      seen.add(src);
      if (src.startsWith("data:image/")) out.push({ data: src });
      else out.push({ url: src });
    }
    return out;
  };

  const scribd = {
    site: "Scribd",
    detect: () => /(^|\.)scribd\.com$/.test(location.hostname) && /^\/(document|doc|presentation|book|read)\//.test(location.pathname),
    title: () => cleanTitle(document.title, [/\s*\|.*$/, /\s*-\s*Scribd\s*$/i]),
    collect: imagePages,
    count: () => bigImgs().length + bigCanvases().length,
  };

  const studocu = {
    site: "Studocu",
    detect: () => /(^|\.)studocu\.com$/.test(location.hostname) && /\/(document|course|summary)\//.test(location.pathname),
    title: () => cleanTitle(document.title, [/\s*[-|]\s*Studocu\s*$/i]),
    collect: imagePages,
    count: () => bigImgs().length + bigCanvases().length,
  };

  window.__ludddDocs = {
    detect: () => [drive, scribd, studocu].find(a => { try { return a.detect(); } catch (_) { return false; } }) || null,
    scrollAll: ad => scrollAll(ad.count),
  };
})();
