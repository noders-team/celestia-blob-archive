/* Block page: list every blob of a height from the archive's own index, open one, and check it.
   Data logic from direction C; presentation rewritten for direction D. Needs site.js and verify.js. */
"use strict";
const NET = net();
const QS = new URLSearchParams(location.search);
const out = $("#out");
const H = Number((QS.get("h") || "").replace(/[,\s_]/g, ""));
const NSQ = QS.get("ns") || "";
let wantC = QS.get("c") || "";

/* ---------- small helpers specific to this page ---------- */
function ago(t) {
  const s = Math.round((Date.now() - new Date(t)) / 1000);
  if (s < 90) return "just now";
  if (s < 5400) return `${Math.round(s / 60)} minutes ago`;
  if (s < 129600) return `${Math.round(s / 3600)} hours ago`;
  if (s < 86400 * 60) return `${Math.round(s / 86400)} days ago`;
  if (s < 86400 * 730) return `${Math.round(s / 86400 / 30.4)} months ago`;
  return `${Math.round(s / 86400 / 365)} years ago`;
}
const utc = (t) => fmtDateTime(Date.parse(t));
const short = (s, n = 10) => (s.length > 2 * n + 1 ? `${s.slice(0, n)}…${s.slice(-n)}` : s);
const pad = (n, w) => String(n).padStart(w, "0");
const toB64 = (b) => btoa(String.fromCharCode(...b));
const eqB = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);
/* Namespaces: 29 bytes, version byte + 28-byte id. The readable tail when it is text, and the 10-byte id in hex. */
function nsInfo(b) {
  let i = 1; while (i < b.length && b[i] === 0) i++;
  const tail = b.subarray(i);
  const txt = tail.length && tail.every((c) => c >= 0x20 && c < 0x7f) ? String.fromCharCode(...tail) : "";
  return { text: txt, hex: Array.from(b.subarray(19), (x) => x.toString(16).padStart(2, "0")).join("") };
}
function parseNamespace(s) {
  s = s.trim(); if (!s) return null;
  let b = null;
  if (/^(0x)?[0-9a-fA-F]+$/.test(s)) {
    const h = s.replace(/^0x/, ""); if (h.length % 2) return undefined;
    b = Uint8Array.from(h.match(/../g), (x) => parseInt(x, 16));
  } else {
    try { b = Uint8Array.from(atob(s), (c) => c.charCodeAt(0)); } catch (e) { return undefined; }
  }
  if (b.length > 29) return undefined;
  if (b.length < 29) { const o = new Uint8Array(29); o.set(b, 29 - b.length); b = o; }
  return b;
}
const shareCount = (len, v) => { const first = 478 - (v === 1 ? 20 : 0); return 1 + Math.max(0, Math.ceil((len - first) / 482)); };

class ApiError extends Error { constructor(e) { super(e.message); this.code = e.code; } }
async function rpcBatch(calls) {
  const body = calls.map(([method, params], i) => ({ jsonrpc: "2.0", id: i + 1, method, params }));
  const r = await fetch(`/api/${NET}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!r.ok) throw new ApiError({ code: r.status, message: `the read API answered HTTP ${r.status}` });
  const res = await r.json();
  const byId = new Map(res.map((x) => [x.id, x]));
  return calls.map((_, i) => {
    const x = byId.get(i + 1);
    if (!x) return new ApiError({ code: 0, message: "no answer for this call" });
    return x.error ? new ApiError(x.error) : x.result;
  });
}
async function rpc1(method, params) { const [r] = await rpcBatch([[method, params]]); if (r instanceof ApiError) throw r; return r; }

async function range(url, start, len) {
  const r = await fetch(url, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
  if (r.status === 404) return null;
  if (r.status !== 206 && r.status !== 200) throw new Error(`HTTP ${r.status}`);
  const buf = await r.arrayBuffer();
  if (buf.byteLength < len) return null;
  return new DataView(buf.byteLength === len ? buf : buf.slice(start, start + len));
}
/* Directory entry + row index: every blob of the height, without knowing namespaces. */
async function listHeight(h) {
  const b = Math.floor((h - 1) / 10000), slot = (h - 1) % 10000;
  const e = await range(`/data/${NET}/dir/${pad(b, 8)}.dir`, 16 + slot * 24, 24);
  if (!e) return null;
  const sf = Number(e.getBigUint64(0, true)), sl = Number(e.getBigUint64(8, true));
  const rs = e.getUint32(16, true), rc = e.getUint32(20, true);
  if (!rc) return null;
  const ix = await range(`/data/${NET}/segments/${pad(sf, 12)}-${pad(sl, 12)}.idx2`, 32 + rs * 88, rc * 88);
  if (!ix) throw new Error("the row index is missing");
  const rows = [];
  for (let i = 0; i < rc; i++) {
    const o = i * 88, u = new Uint8Array(ix.buffer, ix.byteOffset + o, 88);
    if (Number(ix.getBigUint64(o, true)) !== h) throw new Error("the row index does not match the height");
    if (u[8] !== 2) continue;
    rows.push({ ns: u.slice(9, 38), commitment: toB64(u.slice(38, 70)), recordLen: ix.getUint32(o + 78, true) });
  }
  return { rows, segment: [sf, sl] };
}

function notice(title, body, bad = false) { return `<div class="notice${bad ? " bad" : ""}"><strong>${title}</strong>${body}</div>`; }
function notArchived(h, m) {
  const name = NETS[NET].label;
  if (h > m.highest_height) return notice(`Height ${fmtN(h)} is not archived yet.`, `The newest archived ${esc(name)} height is <a href="${withNet(`block.html?h=${m.highest_height}`)}">${fmtN(m.highest_height)}</a>. New blocks reach the archive a few minutes after the chain makes them; try again shortly.`);
  return notice(`Height ${fmtN(h)} is not archived yet.`, `The ${esc(name)} backfill has every height from genesis to ${fmtN(m.contiguous_until)} and is still filling in the blocks above it. <a href="${withNet("coverage.html")}">See what is covered</a>.`);
}

/* ---------- the form ---------- */
$("#f-h").value = QS.get("h") || ""; $("#f-ns").value = NSQ; $("#f-net").value = NET;
$("#lookup").addEventListener("submit", (e) => {
  const h = $("#f-h"); h.value = h.value.replace(/[,\s_]/g, "");
  if (!/^\d+$/.test(h.value) || +h.value < 1) { e.preventDefault(); h.setCustomValidity("Enter a block height: a whole number from 1."); h.reportValidity(); }
  if (!$("#f-ns").value.trim()) $("#f-ns").disabled = true;
});
$("#f-h").addEventListener("input", (e) => e.target.setCustomValidity(""));
$("#examples").innerHTML = NET === "mainnet"
  ? `Try <a href="${withNet("block.html?h=1435254&ns=AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770=")}">1,435,254</a>, pruned by the network in 2024, or <a href="${withNet("block.html?h=15334400")}">15,334,400</a>, from this week.`
  : `Try <a href="${withNet("block.html?h=1834013")}">1,834,013</a>, two blobs of one namespace, or <a href="${withNet("block.html?h=2063067")}">2,063,067</a>, a block with a Fibre payment.`;

/* ---------- main ---------- */
let STATE = null;
async function main() {
  if (!QS.get("h")) {
    $("#find").classList.add("alone");
    out.innerHTML = "";
    $("#f-h").focus();
    return;
  }
  if (!Number.isInteger(H) || H < 1) { out.innerHTML = notice(`“${esc(QS.get("h"))}” is not a block height.`, "Heights are whole numbers starting at 1.", true); return; }
  const nsWant = NSQ ? parseNamespace(NSQ) : null;
  if (nsWant === undefined) { out.innerHTML = notice("That namespace could not be read.", "Give it as base64, as celestia-node prints it, or as hex: at most 29 bytes.", true); return; }
  document.title = `Block ${fmtN(H)}, ${NETS[NET].label}, Celestia Blob Archive`;
  let m, list, hdr;
  try {
    [m, list, hdr] = await Promise.all([manifest(NET), listHeight(H), rpc1("header.GetByHeight", [H]).catch((e) => e)]);
  } catch (e) {
    out.innerHTML = notice("The archive could not be read.", `${esc(e.message)}. Try again in a minute.`, true); return;
  }
  if (!list) { out.innerHTML = notArchived(H, m); return; }
  if (hdr instanceof Error) hdr = null;
  STATE = { m, rows: list.rows, segment: list.segment, hdr, nsWant, blobs: new Map() };
  renderHead();
  renderTable();
  fibreHere();
  await loadSizes();
  const pick = STATE.rows.find((r) => r.commitment === wantC) || (nsWant && STATE.rows.find((r) => eqB(r.ns, nsWant))) || STATE.rows[0];
  if (pick) select(pick.commitment, false);
}

function renderHead() {
  const { hdr, rows, m } = STATE, h = hdr && hdr.header;
  const signed = hdr ? hdr.commit.signatures.filter((s) => s.block_id_flag === 2).length : 0;
  const pruned = h && Date.now() - Date.parse(h.time) > NETWORK_WINDOW;
  out.innerHTML = `
  <header class="bhead">
    <div>
      <p class="bnet">${esc(NETS[NET].label)}${h ? `, ${esc(utc(h.time))}` : ""}</p>
      <h1>Block ${fmtN(H)}</h1>
      ${h ? `<p class="bstate">${pruned ? `Made ${ago(h.time)}. Celestia nodes have pruned its blob data; this archive still has it.` : `Made ${ago(h.time)}. Celestia nodes still hold it too, until about ${fmtDate(Date.parse(h.time) + NETWORK_WINDOW)}.`}</p>` : ""}
    </div>
    <nav class="bnav" aria-label="Neighbouring blocks">
      ${H > 1 ? `<a class="pill ghost small" href="${withNet(`block.html?h=${H - 1}`)}" rel="prev" aria-label="Previous block, ${fmtN(H - 1)}"><svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><path d="M9 2 4 7l5 5" fill="none" stroke="currentColor" stroke-width="1.4"/></svg>${fmtN(H - 1)}</a>` : ""}
      ${H < m.highest_height ? `<a class="pill ghost small" href="${withNet(`block.html?h=${H + 1}`)}" rel="next" aria-label="Next block, ${fmtN(H + 1)}">${fmtN(H + 1)}<svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><path d="m5 2 5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.4"/></svg></a>` : ""}
    </nav>
  </header>
  ${h ? `<dl class="facts panel">
    <div><dt>Block hash</dt><dd class="hashv" title="${esc(hdr.commit.block_id.hash)}">${esc(hdr.commit.block_id.hash)}</dd></div>
    <div><dt>Data root</dt><dd class="hashv" title="${esc(h.data_hash)}">${esc(h.data_hash)}</dd></div>
    <div><dt>Signed by</dt><dd>${signed} of ${hdr.commit.signatures.length} validators</dd></div>
    <div><dt>Archive segment</dt><dd class="hashv">${fmtN(STATE.segment[0])}–${fmtN(STATE.segment[1])}</dd></div>
  </dl>` : `<p class="muted">The block header could not be read right now; the blobs below are still served.</p>`}
  <section class="bpart" id="blobs" aria-labelledby="blobs-h">
    <div class="bpart-head"><h2 id="blobs-h">${rows.length ? `${rows.length === 1 ? "One blob" : `${fmtN(rows.length)} blobs`} in this block` : "No blobs in this block"}</h2>
    ${rows.length ? `<p class="muted">Listed from the archive’s own index. Choose one to fetch it and check it.</p>` : ""}</div>
    <div id="nsnote"></div>
    <div id="table"></div>
  </section>
  <section class="bpart" id="proofpart" hidden aria-labelledby="proof-h">
    <div class="bpart-head"><h2 id="proof-h">Is this blob genuine?</h2>
    <p class="muted">You don’t have to take the archive’s word for it. Each step follows from the one before and says who checked it.</p></div>
    <div class="proof">
      <figure class="sqfig panel" id="fig">
        <canvas id="sq" role="img" aria-label="Data square"></canvas>
        <ul class="sqlegend">
          <li><i class="k-blob"></i>This blob</li><li><i class="k-other"></i>Other blobs</li><li><i class="k-tx"></i>Transactions</li><li><i class="k-pad"></i>Padding</li><li><i class="k-par"></i>Parity</li>
        </ul>
        <figcaption id="figcap"></figcaption>
        <div id="zoombox" hidden><p class="zoomcap" id="zoomcap"></p><canvas id="zoom" role="img"></canvas></div>
      </figure>
      <div id="steps"><p class="loading">Fetching the blob and its proof</p></div>
    </div>
  </section>`;
  if (!rows.length) $("#table").innerHTML = `<p class="muted">Height ${fmtN(H)} is archived and has no blobs: nobody posted data in this block. Try a neighbouring one.</p>`;
  if (STATE.nsWant) {
    const hit = rows.filter((r) => eqB(r.ns, STATE.nsWant)).length;
    const l = nsInfo(STATE.nsWant);
    $("#nsnote").innerHTML = hit
      ? `<p class="nsnote">${hit === 1 ? "One blob" : `${hit} blobs`} in your namespace <span class="mono">${esc(l.text || l.hex)}</span>, marked below.</p>`
      : notice(`No blob in namespace ${esc(l.text || l.hex)} at this height.`, `The height is archived${rows.length ? "; the namespaces it does hold are listed below" : ""}. Check the height, or look the namespace up at a nearby one.`);
  }
}

/* Fibre payloads never enter the square, so they are not in the blob list; say so when this block has any. */
async function fibreHere() {
  const j = await getJSON(`/data/${NET}/fibre/index/${pad(Math.floor((H - 1) / 10000), 8)}.json`, { allow404: true }).catch(() => null);
  const es = j && j.entries ? j.entries.filter((e) => e.height === H) : [];
  if (!es.length) return;
  const kept = es.filter((e) => e.data === "archived");
  const p = document.createElement("p");
  p.className = "fibre-note";
  p.innerHTML = `This block also carries ${es.length === 1 ? "a Fibre payment" : `${es.length} Fibre payments`} (tx ${es.map((e) => e.tx_index).join(", ")}). ${es.length === 1 ? "Its payload lives" : "Their payloads live"} outside the data square; ${kept.length === es.length ? `the archive captured ${kept.length === 1 ? "it" : "all of them"}, ${fmtBytes(kept.reduce((a, e) => a + e.data_size, 0))}` : `${kept.length} of ${es.length} were captured`}. <a href="${withNet("fibre.html")}">See Fibre captures</a>.`;
  $("#blobs").append(p);
}

function renderTable() {
  const { rows, nsWant } = STATE;
  if (!rows.length) return;
  $("#table").innerHTML = `<div class="tbl blobtbl"><table>
    <thead><tr><th>Namespace</th><th>Commitment</th><th class="r">Size</th><th class="r opt">Shares</th><th class="r opt">Share version</th></tr></thead>
    <tbody>${rows.map((r) => {
      const l = nsInfo(r.ns), b = STATE.blobs.get(r.commitment);
      const len = b && b.data ? atob(b.data).length : 0;
      const size = b && b.data ? fmtBytes(len) : b && b.withdrawn ? "withdrawn" : STATE.partial ? "" : "…";
      return `<tr data-c="${esc(r.commitment)}" aria-selected="false">
        <td>${l.text ? `<span class="nsname">${esc(l.text)}</span>` : ""}<span class="hash${l.text ? " nshex" : ""}">${esc(l.hex)}</span>${nsWant && eqB(r.ns, nsWant) ? '<span class="tag">your namespace</span>' : ""}</td>
        <td><a class="hash" href="${withNet(`block.html?h=${H}&c=${encodeURIComponent(r.commitment)}`)}" title="${esc(r.commitment)}"><span class="c-long">${esc(short(r.commitment, 10))}</span><span class="c-short">${esc(short(r.commitment, 5))}</span></a></td>
        <td class="r num">${size}</td>
        <td class="r num opt">${b && b.data ? fmtN(shareCount(len, b.share_version)) : ""}</td>
        <td class="r num opt">${b && b.data ? b.share_version : ""}</td></tr>`;
    }).join("")}</tbody></table></div>`;
  $("#table tbody").addEventListener("click", (e) => {
    const tr = e.target.closest("tr"); if (!tr) return;
    e.preventDefault(); select(tr.dataset.c, true);
    if (matchMedia("(max-width: 900px)").matches) $("#proofpart").scrollIntoView({ block: "start" });
  });
  markSelected();
}
function markSelected() {
  document.querySelectorAll("#table tbody tr").forEach((tr) => tr.setAttribute("aria-selected", String(tr.dataset.c === wantC)));
}

/* One GetAll per namespace, so a withdrawn blob does not hide the others. */
async function loadSizes() {
  const total = STATE.rows.reduce((n, r) => n + r.recordLen, 0);
  if (total > 8e6) { STATE.partial = true; return; }
  const nss = [...new Map(STATE.rows.map((r) => [toB64(r.ns), r.ns])).keys()];
  for (let i = 0; i < nss.length; i += 16) {
    const chunk = nss.slice(i, i + 16);
    let res;
    try { res = await rpcBatch(chunk.map((ns) => ["blob.GetAll", [H, [ns]]])); } catch (e) { STATE.partial = true; continue; }
    res.forEach((r, j) => {
      if (r instanceof ApiError) {
        if (/withdrawn/.test(r.message)) STATE.rows.filter((x) => toB64(x.ns) === chunk[j]).forEach((x) => STATE.blobs.set(x.commitment, { withdrawn: true }));
        else STATE.partial = true;
        return;
      }
      (r || []).forEach((b) => STATE.blobs.set(b.commitment, b));
    });
  }
  renderTable();
}

async function select(c, push) {
  wantC = c; markSelected();
  if (push) { const q = new URLSearchParams(location.search); q.set("c", c); history.replaceState(null, "", `?${q}`); }
  $("#proofpart").hidden = false;
  const steps = $("#steps");
  steps.innerHTML = `<p class="loading">Fetching the blob and its proof</p>`;
  const row = STATE.rows.find((r) => r.commitment === c);
  const nsb = toB64(row.ns);
  let blob = STATE.blobs.get(c), proof;
  try {
    if (blob && blob.withdrawn) throw new ApiError({ message: "blob withdrawn from this archive" });
    const calls = [["blob.GetProof", [H, nsb, c]]];
    if (!blob) calls.push(["blob.Get", [H, nsb, c]]);
    const res = await rpcBatch(calls);
    res.forEach((r) => { if (r instanceof ApiError) throw r; });
    proof = res[0]; if (!blob) { blob = res[1]; STATE.blobs.set(c, blob); }
  } catch (e) {
    steps.innerHTML = /withdrawn/.test(e.message)
      ? notice("This blob was withdrawn from the archive.", `It is listed in the <a href="${withNet("removals.html")}">removals</a>. It is still part of the chain; a Celestia node that has not pruned this height can serve it.`)
      : /failed verification/.test(e.message)
      ? notice("The archive refused to serve this blob.", "Its stored record did not pass the archive’s own check against the data root, so it is not shown.", true)
      : notice("The blob could not be fetched.", `${esc(e.message)}. Try again in a minute.`, true);
    STATE.cur = null; drawSquare(null); return;
  }
  if (wantC !== c) return;
  let r;
  try { r = await checkBlob(blob, proof); } catch (e) {
    steps.innerHTML = notice("Your browser could not check this blob.", esc(e.message), true); return;
  }
  let hh = null;
  if (STATE.hdr) { try { hh = hex(await headerHash(STATE.hdr.header)).toUpperCase(); } catch (e) { hh = null; } }
  if (wantC !== c) return;
  renderProof(blob, r, hh);
}

/* ---------- data view ---------- */
function hexdump(data, n) {
  const lines = [];
  for (let o = 0; o < Math.min(n, data.length); o += 16) {
    const s = data.subarray(o, Math.min(o + 16, data.length));
    const hx = Array.from(s, (x) => x.toString(16).padStart(2, "0")).join(" ").padEnd(47);
    const tx = Array.from(s, (x) => (x >= 0x20 && x < 0x7f ? String.fromCharCode(x) : ".")).join("");
    lines.push(`<span class="off">${o.toString(16).padStart(5, "0")}</span>  ${hx}  <span class="asc">${esc(tx)}</span>`);
  }
  return lines.join("\n");
}
function textView(data, n) {
  const s = new TextDecoder("utf-8", { fatal: false }).decode(data.subarray(0, n));
  const printable = [...s].filter((ch) => ch >= " " || ch === "\n" || ch === "\t").length / Math.max(1, s.length);
  return { text: s.replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "·"), readable: printable > 0.85 };
}

const WHO = {
  browser: '<span class="who w-browser">Checked in your browser</span>',
  computed: '<span class="who w-browser">Computed in your browser</span>',
  archive: '<span class="who w-archive">Checked by the archive</span>',
  node: '<span class="who w-node">Check with your own node</span>',
  bad: (t) => `<span class="who w-bad">${t}</span>`,
};

function renderProof(blob, r, hh) {
  const { hdr } = STATE, h = hdr && hdr.header;
  const nl = nsInfo(r.ns), nsName = nl.text || nl.hex;
  const n = r.shares.length, k = r.k;
  const lastOds = r.odsStart + n - 1;
  const rowsTxt = r.rows.length === 1 ? `row ${r.firstRow}` : `rows ${r.firstRow} to ${r.rows.at(-1).row}`;
  const signed = hdr ? hdr.commit.signatures.filter((s) => s.block_id_flag === 2).length : 0;
  const hashOk = h && hh === hdr.commit.block_id.hash;
  const cmd = `cba verify --archive ${NETS[NET].files} \\\n  --height ${H} --namespace ${blob.namespace} \\\n  --commitment ${blob.commitment} \\\n  --rpc <your-consensus-node>:9090`;
  STATE.cur = { r, lastOds };
  const tv = textView(r.data, 4096);
  const allOk = r.commitOk && r.shapeOk && hashOk;
  $("#steps").innerHTML = `<ol class="steps">
    <li data-focus="cells">
      <p class="claim">The blob is these ${fmtN(r.data.length)} bytes.</p>
      <p class="why">As the archive’s read API returned them with <code>blob.Get</code>. The steps below are what make them trustworthy.</p>
      <div class="dataview">
        <div class="dv-bar"><div class="seg" role="group" aria-label="Show the data as">
          <button type="button" data-v="hex" aria-pressed="${tv.readable ? "false" : "true"}">Hex</button><button type="button" data-v="text" aria-pressed="${tv.readable ? "true" : "false"}">Text</button></div>
          <span class="dv-actions">${r.data.length > 128 ? `<button class="quiet" type="button" id="more">Show more</button>` : ""}<button class="quiet" type="button" id="dl">Download</button></span></div>
        <pre class="dump" id="dump" tabindex="0" aria-label="Blob data"></pre>
      </div>
    </li>
    <li data-focus="cells">
      <p class="claim">${n === 1 ? "They fit in one share" : `They fill ${fmtN(n)} shares`} of 512 bytes in namespace <span class="mono">${esc(nsName)}</span>, under share commitment <span class="mono">${esc(short(blob.commitment, 8))}</span>.</p>
      <p class="why">Your browser laid the bytes out as share version ${blob.share_version} share${n === 1 ? "" : "s"}${blob.share_version === 1 ? ", with the signer in the first share," : ""} and recomputed the commitment from them. ${r.commitOk ? "It matches the commitment the blob was posted under, so these are the bytes that were paid for." : "It does not match the commitment the blob was posted under."}</p>
      <div class="whos">${r.commitOk ? WHO.browser : WHO.bad("Commitment does not match")}</div>
    </li>
    <li data-focus="cells">
      <p class="claim">In the block’s ${k} × ${k} data square ${n === 1 ? `it is share ${fmtN(r.odsStart)}` : `they are shares ${fmtN(r.odsStart)} to ${fmtN(lastOds)}`}, in ${rowsTxt}.</p>
      <p class="why">The start is the blob’s index from <code>blob.Get</code>; the square’s width follows from the size of the Merkle trees in the proof, and the proof covers exactly the blob’s ${n === 1 ? "one share" : `${fmtN(n)} shares`}.</p>
      <div class="whos">${r.shapeOk ? WHO.browser : WHO.bad("Proof does not cover the blob’s shares")}</div>
    </li>
    <li data-focus="rows">
      <p class="claim">${r.rows.length === 1 ? "That row has this root:" : "Those rows have these roots:"}</p>
      <ul class="roots">${r.rows.map((x) => `<li><span>Row ${x.row}</span><span class="mono" title="${hex(x.root)}">${short(hex(x.root.subarray(58)), 12)}</span></li>`).join("")}</ul>
      <p class="why">Your browser hashed the shares into each row’s namespaced Merkle tree. The rest of the row, the other shares and the parity half that erasure coding adds, comes in as sibling hashes from <code>blob.GetProof</code>. Change one byte of the blob and these roots change.</p>
      <div class="whos">${WHO.computed}</div>
    </li>
    <li data-focus="square">
      <p class="claim">The data root <span class="mono">${h ? esc(short(h.data_hash, 8)) : ""}</span> commits to ${r.rows.length === 1 ? "this row root" : "these row roots"}, along with the other ${4 * k - r.rows.length} row and column roots of the square.</p>
      <p class="why">The archive checked this with the row proofs it stores before serving the blob. The read API does not hand out row proofs, so this is the one step your browser cannot repeat. <code>cba verify</code> repeats it, with steps 2 to 4, against a node you run:</p>
      <div class="codebox"><pre><code>${esc(cmd)}</code></pre><button class="copy" type="button" data-copy="${esc(cmd.replace(/\\\n\s+/g, ""))}">Copy</button></div>
      <div class="whos">${WHO.archive}${WHO.node}</div>
    </li>
    <li data-focus="none">
      ${h ? `<p class="claim">That data root is in the header of block ${fmtN(H)}, whose hash is <span class="mono">${esc(short(hdr.commit.block_id.hash, 8))}</span>.</p>
      <p class="why">${hashOk ? "Your browser rebuilt the block hash from the header’s fields, and it equals the hash in the block’s commit." : "The header’s fields do not hash to the block hash in the commit."} ${signed} of ${hdr.commit.signatures.length} validators signed that hash (commit taken from block ${fmtN(H + 1)}). Checking their signatures needs the validator set, which your node has and this page does not.</p>
      <div class="whos">${hashOk ? WHO.browser : WHO.bad("Header hash does not match")}<span class="who w-node">Signatures: check with your own node</span></div>`
      : `<p class="claim">The data root is in the block header.</p><p class="why">The header could not be read right now. Reload the page to check this step.</p>`}
    </li>
  </ol>
  <div class="verdict ${allOk ? "ok" : "bad"}">
    <p class="vt">${allOk ? "Five of six steps checked without trusting the archive." : "Some checks did not pass. Do not rely on this blob without checking it against your own node."}</p>
    <p class="muted">${allOk ? "Your browser tied the bytes to the commitment, placed them in the square and tied the data root to the block hash. Step 5 rests on the archive’s word here; <code>cba verify</code> closes it against your own node." : "Run <code>cba verify</code> as shown in step 5 to see which part fails."}</p>
  </div>`;
  // data view
  let mode = tv.readable ? "text" : "hex", full = false;
  const dump = $("#dump");
  const paint = () => {
    const lim = full ? 4096 : 128;
    if (mode === "hex") dump.innerHTML = hexdump(r.data, lim);
    else { const t = textView(r.data, lim); dump.textContent = t.text; }
    dump.classList.toggle("text", mode === "text");
    const more = $("#more");
    if (more) more.textContent = full ? "Show less" : r.data.length > 4096 ? "Show the first 4 KB" : "Show all";
  };
  paint();
  document.querySelectorAll(".dv-bar .seg button").forEach((b) => b.addEventListener("click", () => {
    mode = b.dataset.v; document.querySelectorAll(".dv-bar .seg button").forEach((x) => x.setAttribute("aria-pressed", String(x === b))); paint();
  }));
  $("#more") && $("#more").addEventListener("click", () => { full = !full; paint(); });
  $("#dl").addEventListener("click", () => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([r.data], { type: "application/octet-stream" }));
    a.download = `${NET}-${H}-${blob.commitment.replace(/[^A-Za-z0-9]/g, "").slice(0, 12)}.bin`;
    a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  });
  document.querySelectorAll("ol.steps > li").forEach((li) => {
    const on = () => { document.querySelectorAll("ol.steps > li").forEach((x) => x.classList.toggle("active", x === li)); drawSquare(li.dataset.focus); };
    li.addEventListener("mouseenter", on); li.addEventListener("focusin", on);
  });
  const cap = STATE.partial ? " Other blobs are not placed: the block is too large to load them all here." : "";
  $("#figcap").textContent = `The ${k} × ${k} original square, top left, extended to ${2 * k} × ${2 * k} with parity. This blob is ${fmtN(n)} share${n === 1 ? "" : "s"} in ${rowsTxt}; each row root covers the whole extended row.${cap}`;
  $("#sq").setAttribute("aria-label", `Data square of ${k} by ${k} shares; the blob occupies ${rowsTxt}.`);
  drawSquare("rows");
}

/* ---------- the data square (drawing logic from direction C, dark palette) ---------- */
const SQ = { blob: "#7c68f2", other: "#3d3670", tx: "#5b5a70", pad: "#17151f", par: "#0e0c15", parLine: "#1d1b28", ink: "#fdfcff", muted: "#b0b7c0", line: "#3a3949" };
function classify(k) {
  const e = 2 * k, cls = new Uint8Array(k * k);
  let firstBlob = Infinity;
  for (const b of STATE.blobs.values()) {
    if (!b.data || b.index == null || b.index < 0) continue;
    const row = Math.floor(b.index / e), start = row * k + (b.index % e);
    const cnt = shareCount(atob(b.data).length, b.share_version);
    firstBlob = Math.min(firstBlob, start);
    for (let i = start; i < Math.min(start + cnt, k * k); i++) cls[i] = b.commitment === wantC ? 3 : 2;
  }
  const r = STATE.cur.r;
  for (let i = r.odsStart; i < r.odsStart + r.shares.length && i < k * k; i++) cls[i] = 3;
  if (!STATE.partial) for (let i = 0; i < Math.min(firstBlob, k * k); i++) if (!cls[i]) cls[i] = 1;
  return cls;
}
const fillFor = (t) => (t === 3 ? SQ.blob : t === 2 ? SQ.other : t === 1 ? SQ.tx : t === 0 ? SQ.pad : SQ.par);
function drawSquare(focus) {
  const cv = $("#sq"); if (!cv) return;
  const cur = STATE.cur, ctx = cv.getContext("2d");
  const W = Math.round(cv.getBoundingClientRect().width) || 460, dpr = Math.min(2, window.devicePixelRatio || 1);
  const labelW = 66, top = 24;
  cv.width = W * dpr; cv.height = (W - labelW + top) * dpr; cv.style.aspectRatio = `${W} / ${W - labelW + top}`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, W);
  if (!cur || !focus) { $("#zoombox").hidden = true; return; }
  const { r } = cur, k = r.k, e = 2 * k, S = W - labelW, c = S / e, gap = c >= 5 ? 1 : 0;
  const cls = classify(k);
  const rowSet = new Set(r.rows.map((x) => x.row));
  ctx.font = `12px ${getComputedStyle(document.documentElement).getPropertyValue("--sans")}`; ctx.textBaseline = "middle";
  ctx.fillStyle = SQ.muted;
  ctx.fillText("Original data", 0, 9);
  ctx.fillText("Parity", S / 2 + 2, 9);
  for (let y = 0; y < e; y++) for (let x = 0; x < e; x++) {
    const f = x < k && y < k ? fillFor(cls[y * k + x]) : SQ.par;
    ctx.globalAlpha = focus === "rows" && !rowSet.has(y) ? 0.4 : 1;
    ctx.fillStyle = f; ctx.fillRect(x * c, top + y * c, c - gap, c - gap);
  }
  ctx.globalAlpha = 1;
  // parity quadrants get a fine diagonal so they read as derived, not data
  ctx.save(); ctx.beginPath(); ctx.rect(S / 2, top, S / 2, S); ctx.rect(0, top + S / 2, S / 2, S / 2); ctx.clip();
  ctx.strokeStyle = SQ.parLine; ctx.lineWidth = 1; ctx.beginPath();
  for (let d = -S; d < S; d += 7) { ctx.moveTo(d, top + S); ctx.lineTo(d + S, top); }
  ctx.stroke(); ctx.restore();
  ctx.strokeStyle = SQ.line; ctx.lineWidth = 1;
  ctx.strokeRect(0.5, top + 0.5, S - 1, S - 1);
  ctx.beginPath(); ctx.moveTo(S / 2, top); ctx.lineTo(S / 2, top + S); ctx.moveTo(0, top + S / 2); ctx.lineTo(S, top + S / 2); ctx.stroke();
  const rows = r.rows.map((x) => x.row), y0 = top + rows[0] * c, y1 = top + (rows.at(-1) + 1) * c;
  if (focus === "rows" || focus === "cells") {
    ctx.strokeStyle = SQ.ink; ctx.lineWidth = 1;
    ctx.strokeRect(0.5, Math.round(y0) - 0.5, S - 1, Math.max(3, y1 - y0));
  }
  ctx.strokeStyle = SQ.blob; ctx.lineWidth = 2;
  ctx.beginPath(); ctx.moveTo(S + 4, y0); ctx.lineTo(S + 8, y0); ctx.lineTo(S + 8, y1); ctx.lineTo(S + 4, y1); ctx.stroke();
  ctx.fillStyle = SQ.ink;
  ctx.fillText(rows.length === 1 ? `row ${rows[0]}` : `rows ${rows[0]}–${rows.at(-1)}`, S + 13, Math.min((y0 + y1) / 2, top + S - 8));
  if (focus === "square") { ctx.lineWidth = 2; ctx.strokeStyle = SQ.blob; ctx.strokeRect(1, top + 1, S - 2, S - 2); }
  drawZoom(cls, focus);
}
/* The blob's rows, enlarged: every cell is one share the row root is computed over. */
function drawZoom(cls) {
  const { r } = STATE.cur, k = r.k, e = 2 * k, box = $("#zoombox");
  const rows = r.rows.map((x) => x.row);
  if (e > 128 || rows.length > 8) { box.hidden = true; return; }
  box.hidden = false;
  $("#zoomcap").textContent = `${rows.length === 1 ? `Row ${rows[0]}` : `Rows ${rows[0]} to ${rows.at(-1)}`}, enlarged. Each cell is one 512-byte share; a row root covers all ${e} cells of its row, the parity half included.`;
  const cv = $("#zoom"), ctx = cv.getContext("2d"), d = Math.min(2, window.devicePixelRatio || 1);
  const W = Math.round($("#sq").getBoundingClientRect().width) || 460, lw = 52, c = (W - lw) / e, gap = c >= 5 ? 1 : 0;
  const rh = Math.max(c, 18), Hh = rows.length * rh + 4;
  cv.width = W * d; cv.height = Hh * d; cv.style.width = "100%"; cv.style.aspectRatio = `${W} / ${Hh}`;
  ctx.setTransform(d, 0, 0, d, 0, 0); ctx.clearRect(0, 0, W, Hh);
  ctx.font = `12px ${getComputedStyle(document.documentElement).getPropertyValue("--sans")}`; ctx.textBaseline = "middle";
  rows.forEach((y, i) => {
    for (let x = 0; x < e; x++) {
      ctx.fillStyle = x < k ? fillFor(cls[y * k + x]) : SQ.par;
      ctx.fillRect(lw + x * c, 2 + i * rh, c - gap, rh - 2);
    }
    ctx.fillStyle = SQ.muted; ctx.fillText(`row ${y}`, 0, 2 + i * rh + rh / 2);
  });
  ctx.strokeStyle = SQ.line; ctx.lineWidth = 1;
  ctx.strokeRect(lw + 0.5, 1.5, W - lw - 1, rows.length * rh);
  ctx.beginPath(); ctx.moveTo(lw + k * c, 0); ctx.lineTo(lw + k * c, Hh); ctx.stroke();
}
let rz; addEventListener("resize", () => { clearTimeout(rz); rz = setTimeout(() => STATE && STATE.cur && drawSquare("rows"), 150); });
document.fonts && document.fonts.ready.then(() => STATE && STATE.cur && drawSquare("rows"));
document.addEventListener("click", (e) => {
  const b = e.target.closest("button.copy[data-copy]"); if (!b) return;
  e.stopImmediatePropagation();
  navigator.clipboard?.writeText(b.dataset.copy).then(() => { b.textContent = "Copied"; setTimeout(() => (b.textContent = "Copy"), 1500); }, () => { b.textContent = "Select and copy"; });
}, true);
main();
