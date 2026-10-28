/* Browser-side checks of an archived blob, from direction C (unchanged): shares, NMT row roots,
   share commitment and CometBFT header hash. Dependency-free; uses WebCrypto. */
"use strict";
const SHARE = 512, NS = 29, THRESHOLD = 64;
const b64 = s => Uint8Array.from(atob(s), c => c.charCodeAt(0));
const hex = u => Array.from(u, x => x.toString(16).padStart(2, '0')).join('');
const cat = (...a) => { const o = new Uint8Array(a.reduce((n, x) => n + x.length, 0)); let i = 0; for (const x of a) { o.set(x, i); i += x.length; } return o; };
const eq = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);
const cmp = (a, b) => { for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return a[i] - b[i]; return 0; };
const sha = async u => new Uint8Array(await crypto.subtle.digest('SHA-256', u));
const PARITY = new Uint8Array(NS).fill(0xff);
const split = n => { let k = 1; while (k * 2 < n) k *= 2; return k; }; // largest power of two below n

function toShares(ns, data, version, signer) {
  const shares = [];
  const firstCap = SHARE - NS - 1 - 4 - (version === 1 ? 20 : 0);
  let off = 0, first = true;
  do {
    const s = new Uint8Array(SHARE);
    s.set(ns, 0);
    s[NS] = (version << 1) | (first ? 1 : 0);
    let p = NS + 1;
    if (first) {
      new DataView(s.buffer).setUint32(p, data.length); p += 4;
      if (version === 1) { s.set(signer, p); p += 20; }
    }
    const cap = first ? firstCap : SHARE - NS - 1;
    s.set(data.subarray(off, off + cap), p);
    off += cap; first = false;
    shares.push(s);
  } while (off < data.length);
  return shares;
}

const leafHash = async (ns, share) => cat(ns, ns, await sha(cat([0], ns, share)));
async function nodeHash(l, r) {
  const lmin = l.subarray(0, NS), lmax = l.subarray(NS, 2 * NS), rmin = r.subarray(0, NS), rmax = r.subarray(NS, 2 * NS);
  const min = cmp(lmin, rmin) <= 0 ? lmin : rmin;
  const max = eq(rmin, PARITY) ? lmax : (cmp(lmax, rmax) >= 0 ? lmax : rmax);
  return cat(min, max, await sha(cat([1], l, r)));
}
async function nmtRoot(leaves) {
  if (leaves.length === 1) return leaves[0];
  const k = split(leaves.length);
  return nodeHash(await nmtRoot(leaves.slice(0, k)), await nmtRoot(leaves.slice(k)));
}
// Replays nmt's range-proof verification; returns the row root and the row's leaf width.
async function rowRoot(proof, leaves) {
  const start = proof.start || 0, end = proof.end;
  const nodes = proof.nodes.map(b64); leaves = leaves.slice();
  const pop = () => nodes.length ? nodes.shift() : null;
  async function go(s, e) {
    if (e - s === 1) return (start <= s && s < end) ? leaves.shift() : pop();
    if (e <= start || s >= end) return pop();
    const k = split(e - s);
    const l = await go(s, s + k), r = await go(s + k, e);
    return r ? nodeHash(l, r) : l;
  }
  let width = Math.max(1, split(end) * 2);
  let root = await go(0, width);
  while (nodes.length) { root = await nodeHash(root, nodes.shift()); width *= 2; }
  return { root, width, start, end };
}
async function merkle(items) { // RFC 6962, as CometBFT's merkle.HashFromByteSlices
  if (!items.length) return sha(new Uint8Array());
  if (items.length === 1) return sha(cat([0], items[0]));
  const k = split(items.length);
  return sha(cat([1], await merkle(items.slice(0, k)), await merkle(items.slice(k))));
}
const pow2up = n => { let p = 1; while (p < n) p *= 2; return p; };
async function commitment(ns, shares) {
  const n = shares.length;
  const width = Math.min(pow2up(Math.ceil(n / THRESHOLD)), pow2up(Math.ceil(Math.sqrt(n))));
  const roots = []; let i = 0;
  while (i < n) {
    let size = width; const left = n - i;
    if (left < width) { size = 1; while (size * 2 <= left) size *= 2; }
    const leaves = await Promise.all(shares.slice(i, i + size).map(s => leafHash(ns, s)));
    roots.push(await nmtRoot(leaves)); i += size;
  }
  return merkle(roots);
}

// Full check of one blob as the API returned it plus its proof.
async function checkBlob(blob, proof) {
  const ns = b64(blob.namespace), data = b64(blob.data);
  const signer = blob.signer ? b64(blob.signer) : null;
  const shares = toShares(ns, data, blob.share_version, signer);
  const commit = await commitment(ns, shares);
  const commitOk = eq(commit, b64(blob.commitment));
  const covered = proof.reduce((n, p) => n + p.end - (p.start || 0), 0);
  const rows = []; let used = 0;
  for (const p of proof) {
    const cnt = p.end - (p.start || 0);
    const leaves = await Promise.all(shares.slice(used, used + cnt).map(s => leafHash(ns, s)));
    used += cnt;
    rows.push(await rowRoot(p, leaves));
  }
  const eds = rows[0].width, k = eds / 2;
  const firstRow = Math.floor(blob.index / eds), firstCol = blob.index % eds;
  rows.forEach((r, i) => { r.row = firstRow + i; });
  const shapeOk = covered === shares.length && firstCol === (proof[0].start || 0) && rows.every(r => r.width === eds);
  return { ns, data, shares, commit, commitOk, rows, k, firstRow, firstCol, shapeOk,
    odsStart: firstRow * k + firstCol, };
}

// CometBFT Header.Hash(): RFC 6962 tree over the proto encoding of each field.
const fromHex = h => Uint8Array.from((h || '').match(/../g) || [], x => parseInt(x, 16));
function varint(n) { n = BigInt(n); const o = []; do { let b = Number(n & 0x7fn); n >>= 7n; if (n) b |= 0x80; o.push(b); } while (n); return o; }
const fVar = (f, v) => (BigInt(v) === 0n ? [] : [f << 3, ...varint(v)]);
const fLen = (f, bytes) => (bytes.length ? [(f << 3) | 2, ...varint(bytes.length), ...bytes] : []);
const enc = s => new TextEncoder().encode(s);
function tsParts(t) {
  const m = t.match(/^(.*?)(?:\.(\d+))?Z$/);
  const secs = Math.floor(Date.parse(m[1] + 'Z') / 1000);
  const nanos = m[2] ? Number((m[2] + '000000000').slice(0, 9)) : 0;
  return [secs, nanos];
}
async function headerHash(h) {
  const [secs, nanos] = tsParts(h.time);
  const bid = h.last_block_id;
  const psh = [...fVar(1, bid.parts.total), ...fLen(2, fromHex(bid.parts.hash))];
  const fields = [
    [...fVar(1, h.version.block), ...fVar(2, h.version.app || 0)],
    fLen(1, enc(h.chain_id)),
    fVar(1, h.height),
    [...fVar(1, secs), ...fVar(2, nanos)],
    [...fLen(1, fromHex(bid.hash)), ...fLen(2, psh)],
    ...['last_commit_hash', 'data_hash', 'validators_hash', 'next_validators_hash', 'consensus_hash',
        'app_hash', 'last_results_hash', 'evidence_hash', 'proposer_address'].map(k => fLen(1, fromHex(h[k]))),
  ].map(a => new Uint8Array(a));
  return merkle(fields);
}
