/* Hero art: the chain as a braid of light flowing in from the right (now) and ageing to the left.
   The braid fades where Celestia nodes prune; every share that flows along it is also caught and
   set into the lattice below, which never fades. */
"use strict";
(function () {
  const canvas = document.getElementById("hero-art");
  if (!canvas) return;
  const ctx = canvas.getContext("2d", { alpha: false });
  const host = canvas.parentElement;
  const reduce = matchMedia("(prefers-reduced-motion: reduce)");

  let W = 0, H = 0, dpr = 1, G = null;          // geometry
  let strands = [], parts = [], cols = [], offset = 0, t = 0;
  const DRIFT = 12;                              // px/s: how fast everything ages to the left
  const BG = "rgb(4,2,7)";

  function geometry() {
    const mobile = W < 720;
    const amp = Math.min(H * 0.12, mobile ? 32 : 60);
    const cy = amp + (mobile ? 46 : 64);
    const cell = mobile ? 13 : 17;
    const ly = cy + amp + (mobile ? 26 : 34);           // far edge of the lattice floor
    const rows = mobile ? 9 : 11;
    const g = {
      mobile, cy, amp, cell, ly, rows, bottom: H - 6,
      fadeA: W * (mobile ? 0.1 : 0.14), fadeB: W * (mobile ? 0.62 : 0.6),   // the network's memory edge
      catchA: W * 0.78, catchB: W * 0.995,
      nStrands: mobile ? 14 : 26, maxParts: mobile ? 80 : 170,
    };
    // spawn fast enough that every column is full before it leaves the catch zone
    g.rate = 1.35 * rows * DRIFT / cell;
    // perspective: row 0 is far (near the braid), the last row is near (bottom of the hero)
    g.scale = Array.from({ length: rows }, (_, r) => 1 / (1 + (rows - 1 - r) * 0.16));
    const sFar = g.scale[0];
    g.rowY = g.scale.map((sc) => ly + (g.bottom - ly) * ((sc - sFar) / (1 - sFar)));
    return g;
  }

  function resize() {
    const r = host.getBoundingClientRect();
    W = Math.max(320, Math.round(r.width)); H = Math.max(260, Math.round(r.height));
    dpr = Math.min(1.75, window.devicePixelRatio || 1);
    canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
    G = geometry();
    strands = Array.from({ length: G.nStrands }, (_, i) => ({
      ph: (i / G.nStrands) * Math.PI * 2 + Math.random() * 0.4,
      r: 0.35 + 0.65 * Math.random(),
      k: 0.0042 + Math.random() * 0.0016,
      w: 0.55 + Math.random() * 0.7,
      tone: Math.random(),
    }));
    // lattice columns: index 0 at the left; each has rows of cell brightness (0 = empty)
    const n = Math.ceil(W / G.cell) + 3;
    cols = Array.from({ length: n }, (_, c) => newCol(c * G.cell < W * 0.9 ? 1 : 0));
    offset = 0; parts = [];
    for (const p of document.querySelectorAll("[data-hero-at]")) place(p);
  }
  // prefilled history columns are lit; new columns arrive empty and get filled by catches
  function newCol(fill) {
    const a = new Float32Array(G.rows);
    if (fill) for (let r = 0; r < G.rows; r++) a[r] = 0.3 + Math.random() * 0.12;
    return a;
  }

  // a point on strand s at x: y and depth (front/back of the braid)
  function radius(x) { return G.amp * (0.62 + 0.38 * Math.sin((x / W) * Math.PI * 1.15 + 0.35)); }
  function sy(s, x) {
    const a = s.ph + x * s.k - t * 0.55;
    return [G.cy + radius(x) * s.r * Math.sin(a), Math.cos(a)];
  }
  const smooth = (a, b, x) => { const u = Math.min(1, Math.max(0, (x - a) / (b - a))); return u * u * (3 - 2 * u); };
  const memory = (x) => smooth(G.fadeA, G.fadeB, x);
  const colX = (c) => c * G.cell - offset;
  // screen position of cell (c, r) on the receding floor; x converges toward the centre with depth
  const px = (x, r) => W / 2 + (x - W / 2) * (0.62 + 0.38 * G.scale[r]);
  const rowY = (r) => G.rowY[r];

  function spawn(x0) {
    if (parts.length >= G.maxParts) return;
    const s = strands[(Math.random() * strands.length) | 0];
    parts.push({ s, x: x0 ?? W + 4, v: 70 + Math.random() * 90, catchX: G.catchA + Math.random() * (G.catchB - G.catchA),
      drop: null, size: Math.random() < 0.18 ? 3 : 2 });
  }
  // an empty cell in the columns under the catch point; if they are full, light one again
  function target(x) {
    const c0 = Math.round((x + offset) / G.cell);
    for (let d = 0; d <= 4; d++) for (const c of d ? [c0 - d, c0 + d] : [c0]) {
      const col = cols[c]; if (!col) continue;
      for (let r = 0; r < G.rows; r++) if (col[r] === 0) { col[r] = -1; return { c, r }; }
    }
    return { c: Math.max(0, Math.min(cols.length - 1, c0)), r: (Math.random() * G.rows) | 0 };
  }

  let acc = 0;
  function step(dt) {
    t += dt;
    offset += DRIFT * dt;
    while (offset >= G.cell) { offset -= G.cell; cols.shift(); cols.push(newCol(0)); for (const p of parts) if (p.drop) p.drop.c--; }
    acc += dt * G.rate;
    while (acc >= 1) { acc -= 1; spawn(); }
    for (let i = parts.length - 1; i >= 0; i--) {
      const p = parts[i];
      if (p.drop) {
        const d = p.drop; d.k += dt / d.dur;
        if (d.k >= 1) {
          const col = cols[d.c]; if (col) col[d.r] = 1.0;
          d.done = true;
        }
      }
      if (!p.copied && p.x <= p.catchX) {
        // the archive's copy leaves the braid; the network's copy keeps flowing and will fade
        p.copied = true;
        const tg = target(p.x);
        const [y] = sy(p.s, p.x);
        parts.push({ ghost: true, drop: { x0: p.x, y0: y, c: tg.c, r: tg.r, k: 0, dur: 0.9 + Math.random() * 0.6 }, size: p.size });
      }
      if (p.ghost) { if (p.drop.done) parts.splice(i, 1); continue; }
      p.x -= p.v * dt;
      if (p.x < G.fadeA - 20) parts.splice(i, 1);
    }
    // lit cells settle back to their persistent glow
    for (const col of cols) for (let r = 0; r < col.length; r++) if (col[r] > 0.42) col[r] = Math.max(0.36, col[r] - dt * 0.5);
  }

  function draw() {
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.globalCompositeOperation = "source-over";
    ctx.fillStyle = BG; ctx.fillRect(0, 0, W, H);

    // lattice floor: faint wireframe in perspective, always there
    const last = G.rows - 1, half = G.cell / 2;
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(253,252,255,0.06)";
    ctx.beginPath();
    for (let c = 0; c < cols.length; c++) { const x = colX(c) - half; ctx.moveTo(px(x, 0), rowY(0) - 6); ctx.lineTo(px(x, last), G.bottom + 20); }
    for (let r = 0; r < G.rows; r++) { const y = Math.round(rowY(r)) + 0.5; ctx.moveTo(0, y); ctx.lineTo(W, y); }
    ctx.stroke();
    // lit cells (the archive), sized by depth
    for (let c = 0; c < cols.length; c++) {
      const x = colX(c), col = cols[c];
      for (let r = 0; r < G.rows; r++) {
        const b = col[r]; if (b <= 0) continue;
        const sc = G.scale[r], sz = G.cell * 0.34 * (0.55 + 0.45 * sc) * (G.mobile ? 1.1 : 1);
        const a = b * (0.55 + 0.45 * sc);
        ctx.fillStyle = b > 0.5 ? `rgba(228,222,255,${a.toFixed(3)})` : `rgba(150,134,250,${(a * 1.35).toFixed(3)})`;
        ctx.fillRect(px(x, r) - sz / 2, rowY(r) - sz / 2 - half * sc * 0.6, sz, sz);
      }
    }

    // the braid (what the network holds), additive light, fading left
    ctx.globalCompositeOperation = "lighter";
    const grad = ctx.createLinearGradient(0, 0, W, 0);
    grad.addColorStop(0, "rgba(124,104,242,0)");
    grad.addColorStop(G.fadeA / W, "rgba(124,104,242,0)");
    grad.addColorStop((G.fadeA + (G.fadeB - G.fadeA) * 0.55) / W, "rgba(150,132,250,0.32)");
    grad.addColorStop(G.fadeB / W, "rgba(205,198,255,0.68)");
    grad.addColorStop(1, "rgba(253,252,255,1)");
    const stepX = G.mobile ? 10 : 12, x0 = Math.max(0, G.fadeA - 30);
    // a wide, faint pass first gives the braid its glow; then the fine strands
    ctx.strokeStyle = grad;
    ctx.globalAlpha = 0.05;
    ctx.lineWidth = G.mobile ? 6 : 9;
    for (let i = 0; i < strands.length; i += 3) {
      const s = strands[i];
      ctx.beginPath();
      for (let x = x0; x <= W + stepX; x += stepX) { const [y] = sy(s, x); x === x0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y); }
      ctx.stroke();
    }
    for (const s of strands) {
      ctx.strokeStyle = grad;
      ctx.lineWidth = s.w;
      ctx.globalAlpha = 0.35 + 0.5 * s.tone;
      ctx.beginPath();
      for (let x = x0; x <= W + stepX; x += stepX) { const [y] = sy(s, x); x === x0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y); }
      ctx.stroke();
    }
    ctx.globalAlpha = 1;
    // shares travelling along the braid, and the archive's copies dropping into the lattice
    for (const p of parts) {
      if (p.ghost) {
        const d = p.drop, e = d.k * d.k * (3 - 2 * d.k);
        const tx = px(colX(d.c), d.r), ty = rowY(d.r) - (G.cell / 2) * G.scale[d.r] * 0.6;
        const x = d.x0 + (tx - d.x0) * e, y = d.y0 + (ty - d.y0) * (d.k * d.k);
        ctx.fillStyle = `rgba(214,206,255,${(0.9 - 0.4 * d.k).toFixed(3)})`;
        ctx.fillRect(x - 1, y - 1, p.size, p.size);
        continue;
      }
      const [y, depth] = sy(p.s, p.x);
      const a = memory(p.x) * (0.55 + 0.45 * depth);
      if (a < 0.02) continue;
      ctx.fillStyle = `rgba(253,252,255,${a.toFixed(3)})`;
      ctx.fillRect(p.x - p.size / 2, y - p.size / 2, p.size, p.size);
    }
    ctx.globalCompositeOperation = "source-over";
    // fade the lattice into the page at the bottom
    const fb = ctx.createLinearGradient(0, H - 70, 0, H);
    fb.addColorStop(0, "rgba(4,2,7,0)"); fb.addColorStop(1, "rgba(4,2,7,1)");
    ctx.fillStyle = fb; ctx.fillRect(0, H - 70, W, 70);
  }

  // position HTML annotations from the same geometry
  function place(el) {
    const at = el.dataset.heroAt;
    if (at === "forget") {
      // centred over the fade, but never past the edges
      const x = G.fadeA + (G.fadeB - G.fadeA) * 0.38, w = el.offsetWidth || 260, m = G.mobile ? 16 : 40;
      const left = Math.max(m, Math.min(W - m - w, x - w / 2));
      el.style.left = `${left}px`; el.style.top = `${G.cy - G.amp - 46}px`;
      el.style.setProperty("--tick", `${x - left}px`);
    }
    if (at === "keep") { el.style.left = `${G.mobile ? 16 : 40}px`; el.style.top = `${G.ly - 30}px`; }
    if (at === "now") { el.style.right = `${G.mobile ? 16 : 40}px`; el.style.top = `${G.cy - G.amp - 46}px`; }
  }

  // warm up so the first frame already shows a history
  function warm(seconds) { for (let i = 0; i < seconds * 30; i++) step(1 / 30); }

  let running = false, raf = 0, last = 0, visible = true;
  function frame(now) {
    const dt = Math.min(0.05, (now - last) / 1000 || 0.016); last = now;
    step(dt); draw();
    raf = requestAnimationFrame(frame);
  }
  function start() { if (running || reduce.matches || !visible || document.hidden) return; running = true; last = performance.now(); raf = requestAnimationFrame(frame); }
  function stop() { running = false; cancelAnimationFrame(raf); }

  function init() {
    resize(); warm(reduce.matches ? 40 : 14); draw();
    host.classList.add("art-ready");
    start();
  }
  let rt;
  new ResizeObserver(() => { clearTimeout(rt); rt = setTimeout(() => { const w = W; const r = host.getBoundingClientRect(); if (Math.round(r.width) === w && Math.round(r.height) === H) return; stop(); init(); }, 120); }).observe(host);
  new IntersectionObserver((es) => { visible = es[0].isIntersecting; visible ? start() : stop(); }, { threshold: 0.02 }).observe(host);
  document.addEventListener("visibilitychange", () => (document.hidden ? stop() : start()));
  reduce.addEventListener?.("change", () => { stop(); init(); });
  init();
})();
