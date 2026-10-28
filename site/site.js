/* Celestia blob archive, direction D: shared helpers, page chrome and the time axis (logic from direction B). */
"use strict";

const DAY = 86400e3, HOUR = 3600e3;
const NETWORK_WINDOW = 30 * DAY;   // light/bridge nodes prune blob data after about a month
const FIBRE_WINDOW = 4 * HOUR;     // Fibre shard_retention

// Block times are not constant, so heights map to dates through anchors read from block headers.
// The live head is appended at load time.
const NETS = {
  mainnet: {
    label: "Mainnet Beta", short: "Mainnet", chain: "celestia",
    api: "https://celestia-blobs-rpc.noders.services", files: "https://celestia-blobs.noders.services",
    anchors: [[1, "2023-10-31T14:00:00Z"], [500000, "2024-01-07T06:32:36Z"], [1000000, "2024-03-15T06:29:24Z"],
      [1500000, "2024-05-22T06:43:09Z"], [2000000, "2024-07-29T18:41:31Z"], [2500000, "2024-10-06T00:47:34Z"],
      [3000000, "2024-12-13T00:15:09Z"], [3500000, "2025-01-12T13:36:58Z"], [4000000, "2025-02-13T02:26:10Z"],
      [5000000, "2025-04-19T17:23:01Z"], [6000000, "2025-06-18T05:08:08Z"], [7000000, "2025-08-16T10:13:53Z"],
      [7500000, "2025-09-14T21:07:06Z"], [8000000, "2025-10-14T13:14:59Z"], [9000000, "2025-12-18T12:32:32Z"],
      [10000000, "2026-02-27T02:03:30Z"], [11000000, "2026-05-08T11:14:31Z"], [12000000, "2026-07-09T06:41:40Z"],
      [13000000, "2026-08-11T02:28:52Z"], [14000000, "2026-09-13T02:36:25Z"], [15000000, "2026-10-16T06:39:22Z"],
      [15300000, "2026-10-26T04:57:42Z"]],
  },
  mocha: {
    label: "Mocha testnet", short: "Mocha", chain: "mocha-5",
    api: "https://celestia-t-blobs-rpc.noders.services", files: "https://celestia-t-blobs.noders.services",
    anchors: [[1, "2026-08-18T12:00:00Z"], [100000, "2026-08-22T17:28:57Z"], [200000, "2026-08-26T00:13:15Z"],
      [266000, "2026-08-28T08:44:19Z"], [300000, "2026-08-29T15:59:13Z"], [400000, "2026-09-02T04:14:00Z"],
      [500000, "2026-09-05T11:54:59Z"], [600000, "2026-09-08T19:57:08Z"], [800000, "2026-09-15T11:22:52Z"],
      [1000000, "2026-09-22T00:57:42Z"], [1200000, "2026-09-28T17:34:11Z"], [1400000, "2026-10-05T08:25:38Z"],
      [1600000, "2026-10-11T23:07:27Z"], [1800000, "2026-10-18T14:16:39Z"], [2000000, "2026-10-25T05:22:56Z"],
      [2060000, "2026-10-27T05:07:39Z"]],
  },
};

/* ---------- small utilities ---------- */
const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const nf = new Intl.NumberFormat("en-US");
const fmtN = (n) => nf.format(n);
const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const pad2 = (n) => String(n).padStart(2, "0");
function fmtDate(ms) { const d = new Date(ms); return `${d.getUTCDate()} ${MON[d.getUTCMonth()]} ${d.getUTCFullYear()}`; }
function fmtTime(ms) { const d = new Date(ms); return `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`; }
function fmtDateTime(ms) { return `${fmtDate(ms)}, ${fmtTime(ms)} UTC`; }
function fmtBytes(b) {
  if (b < 1024) return `${b} B`;
  const u = ["KB", "MB", "GB", "TB", "PB"]; let i = -1;
  do { b /= 1024; i++; } while (b >= 1024 && i < u.length - 1);
  return `${b >= 100 ? b.toFixed(0) : b.toFixed(1)} ${u[i]}`;
}
function fmtSpan(ms) {
  const a = Math.abs(ms);
  if (a < HOUR) return `${Math.max(1, Math.round(a / 60e3))} min`;
  if (a < 2 * DAY) return `${Math.round(a / HOUR)} h`;
  if (a < 90 * DAY) return `${Math.round(a / DAY)} days`;
  if (a < 700 * DAY) return `${Math.round(a / (30.4 * DAY))} months`;
  return `${(a / (365.25 * DAY)).toFixed(1)} years`;
}
function b64ToBytes(s) { const bin = atob(s); const u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i); return u; }
function bytesToB64(u) { let s = ""; for (let i = 0; i < u.length; i++) s += String.fromCharCode(u[i]); return btoa(s); }
function bytesToHex(u) { let s = ""; for (let i = 0; i < u.length; i++) s += u[i].toString(16).padStart(2, "0"); return s; }
function hexToBytes(h) { const u = new Uint8Array(h.length / 2); for (let i = 0; i < u.length; i++) u[i] = parseInt(h.substr(i * 2, 2), 16); return u; }
/* A namespace's readable part: the trailing 10-byte ID, as text when it is printable. */
function nsLabel(nsBytes) {
  const id = nsBytes.slice(19);
  let start = 0; while (start < id.length && id[start] === 0) start++;
  const tail = id.slice(start);
  if (tail.length >= 3 && [...tail].every((c) => c >= 0x20 && c < 0x7f)) return String.fromCharCode(...tail);
  return null;
}
function net() {
  const n = new URLSearchParams(location.search).get("net");
  return NETS[n] ? n : "mainnet";
}
function withNet(href, n = net()) {
  const u = new URL(href, location.href); u.searchParams.set("net", n); return u.pathname.split("/").pop() + u.search + u.hash;
}

/* ---------- data access ---------- */
async function getJSON(url, { allow404 = false } = {}) {
  for (let attempt = 0; ; attempt++) {
    const r = await fetch(url);
    if (allow404 && r.status === 404) return null;
    if (r.ok) return r.json();
    // object storage answers 409/429/5xx briefly while an open file is being rewritten
    if (attempt < 3 && (r.status === 409 || r.status === 429 || r.status >= 500)) { await new Promise((ok) => setTimeout(ok, 400 * (attempt + 1))); continue; }
    throw new Error(`${url} answered HTTP ${r.status}`);
  }
}
async function getRange(url, start, len) {
  const r = await fetch(url, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
  if (r.status === 404) return null;
  if (r.status !== 206) throw new Error(`${url} did not answer the range request (HTTP ${r.status})`);
  const b = new Uint8Array(await r.arrayBuffer());
  if (b.length !== len) throw new Error(`${url} returned ${b.length} bytes, expected ${len}`);
  return b;
}
class RpcError extends Error { constructor(m, code) { super(m); this.code = code; } }
async function rpc(n, method, params) {
  const r = await fetch(`/api/${n}`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  let j; try { j = await r.json(); } catch { throw new RpcError(`The read API answered HTTP ${r.status} without JSON.`, -1); }
  if (j.error) throw new RpcError(j.error.message, j.error.code);
  return j.result;
}

const cache = {};
function once(key, fn) { return (cache[key] ||= fn()); }

function manifest(n) { return once(`m:${n}`, () => getJSON(`/data/${n}/manifest.json`)); }
function fibreManifest(n) { return once(`f:${n}`, () => getJSON(`/data/${n}/fibre.json`, { allow404: true })); }
function tombstones(n) {
  return once(`t:${n}`, async () => (await getJSON(`/data/${n}/tombstones.json`, { allow404: true })) || { tombstones: [], missing: true });
}
function head(n) {
  return once(`h:${n}`, async () => {
    try {
      const r = await rpc(n, "header.NetworkHead", []);
      return { height: +r.header.height, time: Date.parse(r.header.time) };
    } catch {
      const m = await manifest(n);
      return { height: m.highest_height, time: Date.parse(m.updated_at) };
    }
  });
}
/* All catalog chunks, probed upward until the first 404 (the manifest pointer may lag). */
function catalog(n) {
  return once(`c:${n}`, async () => {
    const m = await manifest(n);
    const guess = m.last_catalog_chunk + 1;
    const urls = (i) => `/data/${n}/catalog/${String(i).padStart(8, "0")}.json`;
    const chunks = await Promise.all(Array.from({ length: guess }, (_, i) => getJSON(urls(i), { allow404: true })));
    for (let i = guess; ; i++) { const c = await getJSON(urls(i), { allow404: true }); if (!c) break; chunks.push(c); }
    const segs = [];
    for (const c of chunks) if (c) for (const s of c.segments) segs.push([s.first, s.last, s.size || 0]);
    segs.sort((a, b) => a[0] - b[0]);
    const ranges = []; let bytes = 0;
    for (const [f, l, sz] of segs) {
      bytes += sz;
      const last = ranges[ranges.length - 1];
      if (last && f <= last[1] + 1) last[1] = Math.max(last[1], l); else ranges.push([f, l]);
    }
    const heights = ranges.reduce((a, [f, l]) => a + l - f + 1, 0);
    return { segs, ranges, bytes, heights };
  });
}
function inRanges(ranges, h) {
  let lo = 0, hi = ranges.length - 1;
  while (lo <= hi) { const m = (lo + hi) >> 1; if (h < ranges[m][0]) hi = m - 1; else if (h > ranges[m][1]) lo = m + 1; else return true; }
  return false;
}

/* Height <-> time, piecewise linear between anchors, extrapolated past the ends. */
function makeClock(n, hd) {
  const pts = NETS[n].anchors.map(([h, t]) => [h, Date.parse(t)]).filter(([h]) => h < hd.height);
  pts.push([hd.height, hd.time]);
  const seg = (i) => pts[Math.max(0, Math.min(pts.length - 2, i))];
  const rate = (i) => { const a = seg(i), b = pts[Math.max(1, Math.min(pts.length - 1, i + 1))]; return (b[1] - a[1]) / (b[0] - a[0]); };
  return {
    genesis: pts[0][1],
    t(h) {
      let i = 0; while (i < pts.length - 2 && h > pts[i + 1][0]) i++;
      const a = pts[i]; return a[1] + (h - a[0]) * rate(i);
    },
    h(t) {
      let i = 0; while (i < pts.length - 2 && t > pts[i + 1][1]) i++;
      const a = pts[i]; return Math.round(a[0] + (t - a[1]) / rate(i));
    },
  };
}
/* A first, approximate state from the manifest alone, so the axis can draw before the catalog arrives. */
function quickState(n) {
  return once(`q:${n}`, async () => {
    const [m, hd] = await Promise.all([manifest(n), head(n)]);
    const clock = makeClock(n, hd), now = Date.now();
    const ranges = [[1, m.contiguous_until]];
    if (m.highest_height > m.contiguous_until) ranges.push([m.highest_height, m.highest_height]);
    return { n, m, hd, clock, now, chainHead: clock.h(now), provisional: true,
      cat: { segs: [], ranges, bytes: 0, heights: m.contiguous_until } };
  });
}
/* Everything the axis needs for one network. */
function netState(n) {
  return once(`s:${n}`, async () => {
    const [m, hd, cat] = await Promise.all([manifest(n), head(n), catalog(n)]);
    const q = cache[`q:${n}`] ? await cache[`q:${n}`].catch(() => null) : null;
    const clock = q ? q.clock : makeClock(n, hd);
    const now = q ? q.now : Date.now();
    return { n, m, hd, cat, clock, now, chainHead: clock.h(now) };
  });
}

/* ---------- height directory and row index (format 2) ---------- */
async function readHeight(n, h) {
  const b = Math.floor((h - 1) / 10000), slot = (h - 1) % 10000;
  const e = await getRange(`/data/${n}/dir/${String(b).padStart(8, "0")}.dir`, 16 + slot * 24, 24);
  if (!e) return null;
  const dv = new DataView(e.buffer);
  const segFirst = Number(dv.getBigUint64(0, true)), segLast = Number(dv.getBigUint64(8, true));
  const rowStart = dv.getUint32(16, true), rowCount = dv.getUint32(20, true);
  if (rowCount === 0) return null;
  const key = `${String(segFirst).padStart(12, "0")}-${String(segLast).padStart(12, "0")}`;
  const rows = await getRange(`/data/${n}/segments/${key}.idx2`, 32 + rowStart * 88, rowCount * 88);
  if (!rows) throw new Error(`Row index ${key}.idx2 is missing although the directory points at it.`);
  const out = { segment: key, blobs: [] };
  const rv = new DataView(rows.buffer);
  for (let i = 0; i < rowCount; i++) {
    const o = i * 88;
    const rh = Number(rv.getBigUint64(o, true)), kind = rows[o + 8];
    if (rh !== h) throw new Error(`Row index ${key}.idx2 row ${rowStart + i} is for height ${rh}, not ${h}.`);
    const ns = rows.slice(o + 9, o + 38), cm = rows.slice(o + 38, o + 70);
    const len = rv.getUint32(o + 78, true);
    if (kind === 1) out.headerLen = len;
    else if (kind === 2) out.blobs.push({ ns, cm, nsB64: bytesToB64(ns), cmB64: bytesToB64(cm), recordLen: len, label: nsLabel(ns) });
  }
  return out;
}

/* ---------- page chrome ---------- */
const PAGES = [["block.html", "Block"], ["coverage.html", "Coverage"], ["fibre.html", "Fibre"], ["use.html", "Use it"], ["removals.html", "Removals"]];
/* The archive's mark: a sphere holding a square of shares. */
const MARK = `<svg viewBox="0 0 32 32" aria-hidden="true" fill="none">
  <path d="M3 8h7m12 0h7M16 13v6M5 17v9h22v-9" stroke="currentColor" stroke-width="2" stroke-linecap="square"/>
  <path d="M12 4h8v8h-8z" fill="#7C68F2"/>
  <path d="M10 26v3h12v-3" stroke="currentColor" stroke-width="2"/></svg>`;
function chrome(current) {
  const n = net();
  const top = document.createElement("header");
  top.className = "top";
  top.innerHTML = `<div class="wrap">
    <a class="brand" href="${withNet("index.html")}">${MARK}<span>Blob Archive<small>Celestia, kept by NODERS</small></span></a>
    <nav aria-label="Site" id="site-nav-wrap"><ul class="nav" id="site-nav">${PAGES.map(([h, t]) =>
      `<li><a href="${withNet(h)}"${h === current ? ' aria-current="page"' : ""}>${t}</a></li>`).join("")}</ul></nav>
    <div class="top-right">
      <div class="netswitch" role="group" aria-label="Network">${Object.entries(NETS).map(([k, v]) =>
        `<button type="button" data-net="${k}" aria-pressed="${k === n}">${v.short}</button>`).join("")}</div>
      ${current === "block.html" ? "" : `<a class="pill small" href="${withNet("block.html")}">Look up a block</a>`}
      <button class="menu-btn" type="button" aria-expanded="false" aria-controls="site-nav" aria-label="Menu">
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true" stroke="currentColor" stroke-width="1.4"><path d="M2 6h14M2 12h14"/></svg></button>
    </div>
  </div>`;
  const ph = document.querySelector("header.top[data-ph]");
  if (ph) ph.replaceWith(top); else document.body.prepend(top);
  const nav = top.querySelector(".nav"), mb = top.querySelector(".menu-btn");
  mb.addEventListener("click", () => { const o = !nav.classList.contains("open"); nav.classList.toggle("open", o); mb.setAttribute("aria-expanded", String(o)); });
  top.querySelectorAll(".netswitch button").forEach((b) => b.addEventListener("click", () => {
    if (b.dataset.net === n) return;
    const u = new URL(location.href); u.searchParams.set("net", b.dataset.net);
    if (current === "block.html") { u.searchParams.delete("h"); u.searchParams.delete("ns"); u.searchParams.delete("c"); }
    location.href = u.toString();
  }));
  document.querySelectorAll("a[data-net-link]").forEach((a) => (a.href = withNet(a.getAttribute("href"))));
  const N = NETS[n];
  const foot = document.createElement("footer");
  foot.className = "foot";
  foot.innerHTML = `<div class="foot-wave" aria-hidden="true">${waveSVG()}</div>
  <div class="wrap">
    <div class="foot-grid">
      <h2>Every blob Celestia ever carried, kept and checkable.</h2>
      <div><h3>Archive</h3><ul>
        <li><a href="${withNet("block.html")}">Look up a block</a></li>
        <li><a href="${withNet("coverage.html")}">Coverage</a></li>
        <li><a href="${withNet("fibre.html")}">Fibre captures</a></li></ul></div>
      <div><h3>Build on it</h3><ul>
        <li><a href="${withNet("use.html")}#api">Read API</a></li>
        <li><a href="${withNet("use.html")}#op">op-alt-da fallback</a></li>
        <li><a href="${withNet("use.html")}#cli">cba verify and export</a></li></ul></div>
      <div><h3>Project</h3><ul>
        <li><a href="https://github.com/noders-team/celestia-blob-archive">Source on GitHub</a></li>
        <li><a href="${withNet("removals.html")}">Removal policy</a></li>
        <li><a href="mailto:abuse@noders.team">abuse@noders.team</a></li></ul></div>
    </div>
    <div class="foot-base"><span>Run by NODERS, a Celestia validator. Free to read, no account, no keys.</span><span>Showing ${esc(N.label)}</span></div>
  </div>`;
  document.body.append(foot);
}
/* Static ribbon of light lines, Celestia-style, for the footer. */
function waveSVG() {
  const W = 1440, H = 220, lines = 34;
  let paths = "";
  for (let i = 0; i < lines; i++) {
    const ph = i * 0.19, amp = 26 + 18 * Math.sin(i * 0.7), y0 = 150 + (i - lines / 2) * 1.6;
    let d = "";
    for (let x = 0; x <= W; x += 24) {
      const y = y0 + amp * Math.sin(x / 260 + ph) * Math.sin(x / 900 + 0.6) - x * 0.035;
      d += (x ? "L" : "M") + x + "," + y.toFixed(1);
    }
    paths += `<path d="${d}" stroke="url(#fw)" stroke-width="${i % 5 === 0 ? 1 : 0.6}" opacity="${(0.25 + 0.6 * ((i * 37) % 11) / 11).toFixed(2)}"/>`;
  }
  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" fill="none"><defs>
    <linearGradient id="fw" x1="0" x2="1" y1="0" y2="0"><stop offset="0" stop-color="#fdfcff" stop-opacity=".9"/>
    <stop offset=".35" stop-color="#a597ff" stop-opacity=".55"/><stop offset=".75" stop-color="#7c68f2" stop-opacity="0"/></linearGradient></defs>${paths}</svg>`;
}
function codebox(text) {
  return `<div class="codebox"><pre><code>${esc(text)}</code></pre><button class="copy" type="button">Copy</button></div>`;
}
document.addEventListener("click", (e) => {
  const b = e.target.closest(".codebox .copy"); if (!b) return;
  const t = b.parentElement.querySelector("code").textContent;
  navigator.clipboard?.writeText(t).then(() => { b.textContent = "Copied"; setTimeout(() => (b.textContent = "Copy"), 1500); },
    () => { b.textContent = "Select and copy"; });
});

/* ---------- the time axis ---------- */
const SVGNS = "http://www.w3.org/2000/svg";
const LANES = {
  archive: { label: "The archive holds", color: "var(--archive)" },
  network: { label: "Celestia nodes still hold", color: "var(--network)" },
  fibre: { label: "Fibre validators hold", color: "var(--fibre)" },
};
function ticksFor(from, to, W) {
  const span = to - from, out = [];
  const maxLabels = Math.max(2, Math.floor(W / 70));
  const push = (t, label, major) => { if (t >= from && t <= to) out.push({ t, label, major }); };
  if (span > 500 * DAY) {
    const y0 = new Date(from).getUTCFullYear(), y1 = new Date(to).getUTCFullYear();
    for (let y = y0; y <= y1; y++) for (let m = 0; m < 12; m += 3) push(Date.UTC(y, m, 1), m === 0 ? String(y) : "", m === 0);
  } else if (span > 45 * DAY) {
    const d = new Date(from); let y = d.getUTCFullYear(), m = d.getUTCMonth();
    const step = Math.max(1, Math.ceil((span / (30.4 * DAY)) / maxLabels));
    for (let i = 0; Date.UTC(y, m + i, 1) <= to; i++) {
      const t = Date.UTC(y, m + i, 1), mm = new Date(t).getUTCMonth();
      push(t, i % step === 0 ? (mm === 0 ? `${MON[mm]} ${new Date(t).getUTCFullYear()}` : MON[mm]) : "", true);
    }
  } else if (span > 2 * DAY) {
    const stepD = [1, 2, 7, 14].find((s) => span / (s * DAY) <= maxLabels) || 14;
    const start = Math.ceil(from / DAY) * DAY;
    for (let t = start; t <= to; t += DAY) {
      const k = Math.round((t - start) / DAY);
      push(t, k % stepD === 0 ? `${new Date(t).getUTCDate()} ${MON[new Date(t).getUTCMonth()]}` : "", k % stepD === 0);
    }
  } else {
    const stepH = [1, 2, 3, 6, 12].find((s) => span / (s * HOUR) <= maxLabels) || 12;
    for (let t = Math.ceil(from / HOUR) * HOUR; t <= to; t += HOUR) {
      const hh = new Date(t).getUTCHours();
      push(t, hh % stepH === 0 ? (hh === 0 ? `${new Date(t).getUTCDate()} ${MON[new Date(t).getUTCMonth()]}` : `${pad2(hh)}:00`) : "", hh % stepH === 0);
    }
  }
  return out;
}
function el(tag, attrs = {}, parent) {
  const e = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.append(e);
  return e;
}
/* Where a height stands today, in one phrase. */
function heightStatus(S, h) {
  const t = S.clock.t(h);
  const archived = inRanges(S.cat.ranges, h);
  const parts = [];
  if (h > S.chainHead) return "not produced yet";
  parts.push(archived ? "archived" : h > S.m.highest_height ? "not archived yet (ahead of the archive head)" : "not archived yet");
  if (S.now - t < FIBRE_WINDOW) parts.push("inside the Fibre window");
  if (S.now - t < NETWORK_WINDOW) parts.push("nodes still hold it");
  else parts.push("pruned by nodes");
  return parts.join(", ");
}

/**
 * Draw a time band. opts: S (netState), from, to (ms), lanes (keys of LANES), fibre (entries),
 * marks [{h, label}], notes [{t, text, lane}], pick (click opens the block), zoom (callback(from,to)).
 */
function timeline(host, opts) {
  host.classList.add("tl");
  host._tlAbort?.abort(); host._tlRO?.disconnect();
  const ac = new AbortController(); host._tlAbort = ac;
  const sig = { signal: ac.signal };
  const o = { lanes: ["archive", "network"], laneH: 14, notes: [], marks: [], fibre: [], pick: true, ...opts };
  const S = o.S;
  let svg, tip, W, cursorT = null, x, tAt;
  const LABEL = 17, GAP = 9, AX = 26;
  const H = () => o.lanes.length * (LABEL + o.laneH + GAP) + AX + (o.marks.length ? 18 : 0);

  function draw() {
    W = Math.max(280, host.clientWidth);
    const top = o.marks.length ? 18 : 0;
    x = (t) => ((t - o.from) / (o.to - o.from)) * W;
    tAt = (px) => o.from + (px / W) * (o.to - o.from);
    host.innerHTML = "";
    svg = el("svg", { viewBox: `0 0 ${W} ${H()}`, height: H(), "aria-hidden": "true", focusable: "false" }, host);
    const defs = el("defs", {}, svg);
    const pid = `hatch-${Math.random().toString(36).slice(2, 8)}`;
    const p = el("pattern", { id: pid, width: 6, height: 6, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" }, defs);
    el("rect", { width: 1.6, height: 6, fill: "var(--gap)" }, p);
    const clipId = `clip-${pid}`;
    el("rect", { x: 0, y: 0, width: W, height: H() }, el("clipPath", { id: clipId }, defs));
    const g = el("g", { "clip-path": `url(#${clipId})` }, svg);
    const clampX = (a, b) => [Math.max(0, x(a)), Math.min(W, x(b))];

    o.lanes.forEach((lane, i) => {
      const y0 = top + i * (LABEL + o.laneH + GAP), yb = y0 + LABEL;
      const lab = el("text", { x: 0, y: y0 + 12, "font-size": 12.5, fill: "var(--muted)", "font-weight": 400 }, svg);
      lab.textContent = LANES[lane].label;
      el("line", { x1: 0, x2: W, y1: yb + o.laneH / 2, y2: yb + o.laneH / 2, stroke: "var(--rule)", "stroke-width": 1 }, g);
      if (lane === "archive") {
        const [ga, gb] = clampX(S.clock.genesis, S.now);
        if (gb > ga) el("rect", { x: ga, y: yb, width: gb - ga, height: o.laneH, fill: `url(#${pid})`, opacity: 0.55 }, g);
        for (const [f, l] of S.cat.ranges) {
          const tf = S.clock.t(f), tl = S.clock.t(l + 1);
          if (tl < o.from || tf > o.to) continue;
          const [a, b] = clampX(tf, tl);
          el("rect", { x: a, y: yb, width: Math.max(1, b - a), height: o.laneH, fill: LANES.archive.color }, g);
        }
      } else if (lane === "network") {
        const [a, b] = clampX(Math.max(S.now - NETWORK_WINDOW, S.clock.genesis), S.now);
        if (b > 0) el("rect", { x: a, y: yb, width: Math.max(1, b - a), height: o.laneH, fill: LANES.network.color }, g);
      } else if (lane === "fibre") {
        const [a, b] = clampX(S.now - FIBRE_WINDOW, S.now);
        if (b > 0) el("rect", { x: a, y: yb, width: Math.max(1.5, b - a), height: o.laneH, fill: LANES.fibre.color }, g);
        for (const f of o.fibre) {
          const t = S.clock.t(f.height); if (t < o.from || t > o.to) continue;
          const c = f.data === "archived" ? "var(--ink)" : "var(--gap)";
          el("rect", { x: x(t) - 1, y: yb - 4, width: 2, height: o.laneH + 8, fill: c }, g);
        }
      }
    });

    // notes: a thin rule through the lanes at t, text in the label row of its lane
    for (const nt of o.notes) {
      if (nt.t < o.from || nt.t > o.to) continue;
      const px = x(nt.t), li = Math.max(0, o.lanes.indexOf(nt.lane));
      const y0 = top + li * (LABEL + o.laneH + GAP);
      el("line", { x1: px, x2: px, y1: y0 + 2, y2: top + o.lanes.length * (LABEL + o.laneH + GAP) - GAP + 4, stroke: "var(--ink)", "stroke-width": 1, "stroke-dasharray": "2 3", opacity: 0.7 }, svg);
      // try the full text, then the short one, on the preferred side first; never over the lane label
      const labelW = LANES[o.lanes[li]].label.length * 6.6 + 10;
      const pref = px > W * 0.55;
      let placed = null;
      for (const text of [nt.text, nt.short].filter(Boolean)) {
        for (const right of [pref, !pref]) {
          const w = text.length * 6.6, x0 = right ? px - 6 - w : px + 6;
          if (x0 >= labelW && x0 + w <= W) { placed = { text, right }; break; }
        }
        if (placed) break;
      }
      if (!placed) continue;
      const txt = el("text", { x: placed.right ? px - 6 : px + 6, y: y0 + 12, "font-size": 12.5, fill: "var(--ink)", "text-anchor": placed.right ? "end" : "start", "font-weight": 500 }, svg);
      txt.textContent = placed.text;
    }
    for (const mk of o.marks) {
      const t = S.clock.t(mk.h); if (t < o.from || t > o.to) continue;
      const px = x(t);
      el("path", { d: `M${px - 6},2 L${px + 6},2 L${px},11 Z`, fill: "var(--ink)" }, svg);
      el("line", { x1: px, x2: px, y1: 11, y2: H() - AX, stroke: "var(--ink)", "stroke-width": 1.5 }, svg);
      if (mk.label) {
        const right = px > W * 0.6;
        const tt = el("text", { x: right ? px - 10 : px + 10, y: 11, "font-size": 12.5, "font-weight": 600, "text-anchor": right ? "end" : "start", fill: "var(--ink)" }, svg);
        tt.textContent = mk.label;
      }
    }

    // axis
    const ya = H() - AX + 4;
    el("line", { x1: 0, x2: W, y1: ya, y2: ya, stroke: "var(--axis)", "stroke-width": 1 }, svg);
    let lastLabelX = -1e9;
    for (const tk of ticksFor(o.from, o.to, W)) {
      const px = x(tk.t);
      el("line", { x1: px, x2: px, y1: ya, y2: ya + (tk.major ? 6 : 3), stroke: "var(--axis)", "stroke-width": 1 }, svg);
      if (tk.label && px - lastLabelX > 44 && px < W - 34 - tk.label.length * 6) {
        const t = el("text", { x: px + 3, y: ya + 17, "font-size": 12, fill: "var(--muted)" }, svg);
        t.textContent = tk.label; lastLabelX = px + tk.label.length * 6.5;
      }
    }
    const nowT = el("text", { x: W, y: ya + 17, "font-size": 12, "text-anchor": "end", "font-weight": 600, fill: "var(--ink)" }, svg);
    nowT.textContent = "now";

    if (o.pick || o.zoom) {
      svg.cursorLine = el("line", { y1: 0, y2: H() - AX + 4, stroke: "var(--ink)", "stroke-width": 1, visibility: "hidden" }, svg);
      svg.brush = el("rect", { y: 0, height: H() - AX + 4, fill: "var(--archive)", opacity: 0.12, visibility: "hidden" }, svg);
      tip = document.createElement("div"); tip.className = "tip"; tip.hidden = true; host.append(tip);
    }
    if (cursorT != null) showCursor(cursorT);
  }

  function hAt(t) { return Math.max(1, Math.min(S.chainHead, S.clock.h(t))); }
  function showCursor(t) {
    cursorT = Math.max(o.from, Math.min(o.to, t));
    const px = x(cursorT), h = hAt(cursorT);
    svg.cursorLine.setAttribute("x1", px); svg.cursorLine.setAttribute("x2", px); svg.cursorLine.setAttribute("visibility", "visible");
    tip.innerHTML = `<b>Height ${fmtN(h)}</b><br>${fmtDateTime(S.clock.t(h))}<br>${esc(heightStatus(S, h))}`;
    tip.hidden = false;
    const tw = tip.offsetWidth;
    tip.style.left = `${Math.max(tw / 2, Math.min(W - tw / 2, px))}px`;
    // Below the band: above it the tip would cover the band's title row and the network switch.
    tip.style.top = `${host.offsetHeight + 6}px`;
    host.setAttribute("aria-valuenow", h);
    host.setAttribute("aria-valuetext", `Height ${fmtN(h)}, ${fmtDateTime(S.clock.t(h))}, ${heightStatus(S, h)}`);
  }
  function hideCursor() { if (!svg?.cursorLine) return; svg.cursorLine.setAttribute("visibility", "hidden"); tip.hidden = true; }
  function open(t) { location.href = withNet(`block.html?h=${hAt(t)}`, S.n); }

  draw();
  if (o.pick || o.zoom) {
    host.tabIndex = 0;
    host.setAttribute("role", "slider");
    host.setAttribute("aria-label", o.ariaLabel || `${NETS[S.n].label} timeline. Arrow keys move through time, Enter opens the block.`);
    host.setAttribute("aria-valuemin", 1); host.setAttribute("aria-valuemax", S.chainHead);
    let downX = null;
    const px = (e) => e.clientX - host.getBoundingClientRect().left;
    host.addEventListener("pointermove", (e) => {
      showCursor(tAt(px(e)));
      if (downX != null && o.zoom) {
        const a = Math.min(downX, px(e)), b = Math.max(downX, px(e));
        svg.brush.setAttribute("x", a); svg.brush.setAttribute("width", b - a); svg.brush.setAttribute("visibility", "visible");
      }
    }, sig);
    host.addEventListener("pointerleave", () => { if (document.activeElement !== host) hideCursor(); }, sig);
    host.addEventListener("pointerdown", (e) => { downX = px(e); }, sig);
    host.addEventListener("pointerup", (e) => {
      const up = px(e), d = downX; downX = null;
      if (d == null) return;
      if (o.zoom && Math.abs(up - d) > 8) {
        const a = tAt(Math.min(d, up)), b = tAt(Math.max(d, up));
        o.zoom(a, b); return;
      }
      if (o.pick) open(tAt(up));
    }, sig);
    host.addEventListener("focus", () => showCursor(cursorT ?? o.to - (o.to - o.from) * 0.02), sig);
    host.addEventListener("blur", hideCursor, sig);
    host.addEventListener("keydown", (e) => {
      const span = o.to - o.from;
      const steps = { ArrowLeft: -span / 100, ArrowRight: span / 100, PageUp: -span / 10, PageDown: span / 10 };
      if (e.key in steps) { showCursor((cursorT ?? o.to) + steps[e.key] * (e.shiftKey ? 10 : 1)); e.preventDefault(); }
      else if (e.key === "Home") { showCursor(o.from); e.preventDefault(); }
      else if (e.key === "End") { showCursor(o.to); e.preventDefault(); }
      else if (e.key === "Enter" && cursorT != null && o.pick) open(cursorT);
      else if (e.key === "Escape" && o.onEscape) o.onEscape();
    }, sig);
  }
  let rw = 0;
  host._tlRO = new ResizeObserver(() => { if (host.clientWidth !== rw) { rw = host.clientWidth; draw(); } });
  host._tlRO.observe(host);
  return { redraw: draw, set(nopts) { Object.assign(o, nopts); draw(); } };
}

/* A lens connecting a slice [a,b] of the band above (spanning from..to) to the full width of the band below. */
function lens(svgHost, from, to, a, b) {
  function draw() {
    const W = Math.max(280, svgHost.clientWidth), x = (t) => ((t - from) / (to - from)) * W;
    const xa = Math.max(0, x(a)), xb = Math.min(W, x(b));
    svgHost.innerHTML = `<svg class="lens" viewBox="0 0 ${W} 34" preserveAspectRatio="none" aria-hidden="true">
      <polygon points="${xa},0 ${xb},0 ${W},34 0,34"></polygon>
      <line x1="${xa}" y1="0" x2="0" y2="34"></line><line x1="${xb}" y1="0" x2="${W}" y2="34"></line></svg>`;
  }
  draw();
  svgHost._ro?.disconnect();
  svgHost._ro = new ResizeObserver(draw); svgHost._ro.observe(svgHost);
}

/* ---------- line art ---------- */
/* Wireframe sphere: meridians and parallels as ellipses, Celestia-like. */
function sphereSVG({ size = 520, tilt = -18, stroke = "#7c68f2", opacity = 0.7 } = {}) {
  const r = size / 2 - 2, c = size / 2;
  let s = `<circle cx="${c}" cy="${c}" r="${r}"/>`;
  for (let i = 1; i < 7; i++) s += `<ellipse cx="${c}" cy="${c}" rx="${(r * Math.cos((i * Math.PI) / 14)).toFixed(1)}" ry="${r}" transform="rotate(${tilt} ${c} ${c})"/>`;
  for (let i = -3; i <= 3; i++) {
    const y = (i / 4) * r, rr = Math.sqrt(r * r - y * y);
    s += `<ellipse cx="${c}" cy="${(c + y).toFixed(1)}" rx="${rr.toFixed(1)}" ry="${(rr * 0.22).toFixed(1)}" transform="rotate(${tilt} ${c} ${c})"/>`;
  }
  return `<svg viewBox="0 0 ${size} ${size}" fill="none" stroke="${stroke}" stroke-width="1" opacity="${opacity}" aria-hidden="true">${s}</svg>`;
}

