# Archive format (version 2)

All objects live in one bucket. Heights in keys are zero-padded decimals.
Format 2 (`manifest.format_version: 2`) replaces the per-segment protobuf
`.idx` of format 1 with a fixed-size height directory (`dir/`) and a
fixed-size row index per segment (`.idx2`), so a reader finds one blob with
four range requests and never downloads a whole index.

| Key | Mutability | Content |
|---|---|---|
| `segments/<first:012d>-<last:012d>.seg` | written once; rewritten in place only by a takedown scrub | framed records for heights first..last |
| `segments/<first:012d>-<last:012d>.idx2` | immutable | row index of the segment |
| `dir/<b:08d>.dir` | open bucket rewritten, then frozen | height directory for heights `b*10000+1 .. (b+1)*10000` |
| `catalog/<n:08d>.json` | open chunk rewritten, full chunks frozen | list of segment refs |
| `manifest.json` | mutable | top-level pointer |
| `tombstones.json` | mutable, add-only | entries withdrawn from the archive |
| `ns/...` | see [Namespace index](#namespace-index) | optional namespace index |
| `op/<ns_hex>/<keccak_hex>` | written once; deleted by a takedown | op-alt-da fallback object: raw blob data (see [OP layer](#op-layer)) |
| `op/_namespaces.json` | mutable | namespaces the OP layer is written for |
| `fibre/...` | see [Fibre](#fibre) | Fibre payloads, records, index and manifest; outside format 2 |
| `migrate-v1/manifest.json`, `migrate-v1/catalog/<n:08d>.json` | transient | backup of the format 1 manifest and catalog, only on a migrated archive |

Format 1 archives also hold `segments/*.idx` (protobuf `SegmentIndex`). A
migrated archive keeps them until the operator deletes them; format 2 readers
and writers never read them.

All integers in `dir/` and `.idx2` are little-endian.

## Object headers

| Key class | `Cache-Control` | `Content-Type` |
|---|---|---|
| `.idx2`; a frozen `dir/` bucket; a full catalog chunk; a full namespace-index chunk; a frozen `fibre/index/` bucket | `public, max-age=31536000, immutable` | binary / JSON |
| `.seg` (first write and scrub rewrite alike); `fibre/data/*`; `fibre/records/*` | `public, max-age=86400` | `application/octet-stream` |
| `op/<ns_hex>/<keccak_hex>` (a takedown deletes it; downstream caches drop it within 5 minutes, the edge never caches `op/`) | `public, max-age=300` | `application/octet-stream` |
| an open `dir/` bucket; `manifest.json`; the open catalog chunk; the open namespace-index chunk; `ns/*/head.json`; `ns/_progress.json`; `tombstones.json`; `op/_namespaces.json`; `migrate-v1/*`; an open `fibre/index/` bucket; `fibre/manifest.json` | `no-cache` | binary / JSON |

JSON objects have `Content-Type: application/json`, `.seg`, `.idx2`,
`dir/`, `fibre/data/*` and `fibre/records/*` have
`application/octet-stream`.

A `.seg` may be overwritten in place only to replace a blob record with a
same-length `TombstoneRecord` (see [Tombstones](#tombstones)). Every other
offset and byte of the segment stays the same, so its `.idx2` and the
directory never change. `.seg` is not marked immutable so that a takedown
reaches downstream caches within a day.

A migrated archive may still hold `.seg` objects written by a format 1
ingester with `public, max-age=31536000, immutable`: migration does not
rewrite segments. For such a segment the one-day bound does not hold; a
downstream cache that fetched it before the takedown may keep the old bytes
for up to a year, even after the scrub rewrite gives it the header above. The
operator can give them the format 2 header up front by copying each object
onto itself with only the headers replaced (bytes unchanged).

The public domains cache `.idx2` and `dir/` at the edge (respecting the
headers above, so an open `no-cache` bucket is not cached) and never cache
`.seg`: a scrub is visible at the origin immediately.

The first write of a `.seg` and of an `.idx2` is a conditional PUT
(`If-None-Match: *`); an existing object with different bytes is an error
(for a `.seg`, unless the difference is only tombstones, see
[Commit order](#commit-order)). Directory buckets, catalog chunks and the
other JSON objects are written with a plain PUT, including the write that
freezes them. `fibre/data/*` and the first write of `fibre/records/*` are
conditional PUTs too (see [Fibre](#fibre)).

## Segment (`.seg`)

A concatenation of frames. Each frame is an unsigned varint (protobuf
varint) length `n` followed by `n` bytes of a protobuf `Record` as defined in
`proto/archive/v1/records.proto`. Within a segment, for each height ascending
there is one `HeightRecord` followed by that height's blob records in chain
order. Heights in a segment are consecutive.

A blob record is a `BlobRecord`, or a `TombstoneRecord` once the blob is
withdrawn. `HeightRecord.blob_count` counts both: a tombstone keeps its place
and its count. Readers must accept `Record.tombstone` (oneof field 3) and
ignore `Record.filler` (field 15).

## Height directory (`dir/`)

Bucket `b` holds one entry per height `h` with `b = (h-1) / 10000`, at slot
`(h-1) % 10000`. Buckets line up with the backfill ranges (`[1,10000]`,
`[10001,20000]`, …). Every bucket object is exactly `16 + 10000 * 24 =
240016` bytes:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `CBADIR2\0` |
| 8 | 8 | bucket number `b` (u64) |
| 16 + slot·24 | 24 | entry of height `b*10000 + slot + 1` |

An entry:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `seg_first` (u64): first height of the segment holding `h` |
| 8 | 8 | `seg_last` (u64): its last height |
| 16 | 4 | `row_start` (u32): index of `h`'s first row in that segment's `.idx2` |
| 20 | 4 | `row_count` (u32): number of rows of `h` (1 height row + 1 per blob) |

The all-zero entry means the height is not archived, and so does a missing
bucket object. Otherwise `row_count ≥ 1` and
`seg_first ≤ h ≤ seg_last`; the entry points at
`segments/<seg_first>-<seg_last>.seg` and `.idx2`.

**The directory, not the catalog, is the reader's map.** A reader that wants
a height reads its entry; it does not load the catalog.

## Row index (`.idx2`)

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `CBAIDX2\0` |
| 8 | 8 | `first` (u64): first height of the segment |
| 16 | 8 | `last` (u64): last height |
| 24 | 8 | `count` (u64): number of rows |
| 32 + i·88 | 88 | row `i` |

The object is exactly `32 + count * 88` bytes. A row:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `height` (u64) |
| 8 | 1 | `kind`: 1 = height record, 2 = blob record (or its tombstone) |
| 9 | 29 | namespace; zero for a height row |
| 38 | 32 | share commitment; zero for a height row |
| 70 | 8 | `offset` (u64): first byte of the `Record` in the `.seg`, after the frame's length varint |
| 78 | 4 | `length` (u32): size of the `Record` bytes |
| 82 | 6 | reserved, zero |

Rows are sorted by `(height, kind, namespace, offset)`. So the rows of one
height are contiguous: the height row first, then its blob rows sorted by
namespace and, within a namespace, by offset (which is chain order). A
tombstone keeps the row of the blob it replaced (kind 2, same namespace,
commitment, offset and length).

### Lookup

To read the blob `(height, namespace, commitment)`:

1. Range-read the 24-byte directory entry at `16 + ((h-1) % 10000) * 24` of
   `dir/<(h-1)/10000>.dir`. Missing object or zero entry: not archived.
2. Range-read `row_count * 88` bytes at `32 + row_start * 88` of the `.idx2`.
   A row group of at most 64 KiB (744 rows) is fetched whole. A larger one is
   binary-searched on namespace with single-row reads (the height row is read
   first); then, from the first row of the namespace, up to 64 KiB of rows
   are read at a time and scanned for the commitment until the namespace ends.
3. Range-read the height record (`offset`, `length` of the height row).
4. Range-read the blob record.

The rows are not trusted: each record read must match its row (kind, height,
namespace, commitment), and `blob_count + 1` must equal `row_count`, so a
short row group cannot hide a blob. A height archived without the requested
blob is "blob not found", distinct from "height not archived". Reading a
whole height takes three requests: steps 1–2, then one range over all of its
records.

A worked example: height 1834013 holds two blobs of one namespace in the
segment covering 1834001..1834060; the reader wants the second one. HTTP
ranges are inclusive.

```lookup
height 1834013
# 1. directory entry: bucket (1834013-1)/10000 = 183, slot 4012, 16 + 4012*24 = 96304
GET dir/00000183.dir bytes=96304-96327
entry seg_first=1834001 seg_last=1834060 row_start=37 row_count=3
# 2. rows 37..39: 32 + 37*88 = 3288, 3*88 = 264 bytes
GET segments/000001834001-000001834060.idx2 bytes=3288-3551
row kind=1 offset=812345 length=1630
row kind=2 offset=813977 length=6120   # 812345+1630 + 2-byte length varint
row kind=2 offset=820099 length=3071   wanted
# 3. height record, 4. blob record
GET segments/000001834001-000001834060.seg bytes=812345-813974
GET segments/000001834001-000001834060.seg bytes=820099-823169
```

`internal/archiveread` `TestFormatDocExample` replays this block against the
reader.

## Embedded messages

Some record fields hold marshaled protobuf messages defined outside this
repository:

| Field | Message (full proto name) | Defined in |
|---|---|---|
| `HeightRecord.header` | `tendermint.types.Header` | CometBFT (`proto/tendermint/types/types.proto`; celestia-core fork) |
| `HeightRecord.commit` | `tendermint.types.Commit` | same; the `LastCommit` of block height+1, so its `block_id.hash` is the hash of `header` |
| `BlobRecord.share_proof` | `celestia.core.v1.proof.ShareProof` | celestia-app (`proto/celestia/core/v1/proof/proof.proto`) |

`share_start` and `share_end` give the blob's half-open share range
`[share_start, share_end)` in the original data square, as a row-major share
index (share `i` is row `i / square_size`, column `i % square_size`).
`share_proof` proves exactly the shares in that range against
`HeightRecord.data_hash`: its row proofs prove the row roots against the data
root, and its NMT proofs prove the shares against those row roots. An NMT leaf
is `namespace ‖ share`: the blob's 29-byte namespace followed by the whole
512-byte share (which itself starts with the namespace). `ShareProof.data`
holds the 512-byte shares; the verifier prepends the namespace.

## Catalog

```json
{"segments":[{"first":1,"last":100,"key":"segments/000000000001-000000000100.seg","size":12345}]}
```

The catalog lists every committed segment; the ingester, the migration, the
namespace index and `cba inspect` use it, lookups do not. A ref names the
`.seg`; its `.idx2` is `segments/<first>-<last>.idx2`. Chunks migrated from
format 1 also carry the v1 `index_key`; it is optional and ignored in format
2, and dropped when the open chunk is next rewritten.

A chunk holds up to 1000 refs, sorted by `first`. A chunk is simply the next
1000 committed refs; backfill commits out of order, so chunks do not cover
disjoint height ranges, and a catalog consumer (the ingester, `cba inspect`)
loads all chunks to see the committed ranges. Lookups use `dir/` instead. The last chunk (`manifest.last_catalog_chunk`) is rewritten while it
is open; the write that fills it to 1000 refs is its last.

Readers discover the chunks by probing `catalog/00000000.json`,
`catalog/00000001.json`, ... upward until the first 404, not by trusting
`last_catalog_chunk`: the catalog is written before the manifest, so after an
interrupted commit the pointer can lag behind the chunks that exist.

## Manifest

```json
{"format_version":2,"chain_id":"mocha-5","highest_height":300,"contiguous_until":300,"last_catalog_chunk":0,"updated_at":"2026-10-26T12:00:00Z"}
```

`contiguous_until` is the largest H such that every height 1..H is covered by
a committed segment (genesis is height 1). `highest_height` is the largest
committed height. `updated_at` is RFC3339 UTC. Readers refuse any
`format_version` other than 2.

## Commit order

For each segment: `.seg`, then `.idx2`, then the directory buckets it
touches, then the catalog chunk, then `manifest.json`.

- `.seg` and `.idx2` are written with `If-None-Match: *`, so the directory
  never points at an object that does not exist.
- Each touched directory bucket is written `no-cache` before the catalog, so
  a catalogued height always has its entry.
- After the catalog write that makes the catalog cover all 10000 heights of a
  bucket, the bucket is rewritten with the immutable header (frozen). It is
  never written again. A failed freeze only leaves the bucket `no-cache`; it
  is retried by the next commit.
- The manifest is written last.

A crash between these writes can leave a `.seg`/`.idx2` that no catalog chunk
references (an orphan), and, after a crash between the directory and the
catalog write, open-bucket entries pointing at it. Such an entry still leads
to a complete, correct segment; the ingester clears entries of uncatalogued
segments when it next writes the bucket, and a later commit of the same
range reuses the keys only if the bytes are identical (up to tombstones, see
below). A bucket is frozen only once every height in it is catalogued, so a
frozen bucket never points at an orphan.

On shutdown the ingester commits the open tail segment in one attempt of at
most 90 seconds; if that fails, the next start archives those heights again.

## Namespace index

An optional index built from committed segments' `.idx2`, so the blobs of one
namespace can be listed without scanning segments. It only covers heights
`1..contiguous_until`; segments above a gap are indexed once the gap is filled.
`ns_hex` is the lowercase hex of the full 29-byte namespace (58 characters).

| Key | Mutability | Content |
|---|---|---|
| `ns/<ns_hex>/<seq:08d>.json` | open chunk rewritten, full chunks frozen | `{"entries":[...]}`, up to 10000 entries |
| `ns/<ns_hex>/head.json` | mutable | `{"last_seq":0,"blobs":3,"first_height":2,"last_height":9}` |
| `ns/_progress.json` | mutable | `{"last_height":9}` |

An entry is `{"height":2,"commitment":"<hex>","segment_key":"segments/...seg","offset":123,"length":456}`;
`offset`/`length` address the blob record inside the segment like an `.idx2`
row. Entries are in height order (and archive order within a height).
`seq` starts at 0. The last chunk of a namespace is rewritten as blobs are
appended until it holds 10000 entries; then it is never written again and the
next entry starts chunk `seq+1`.

Each update writes the chunks, then each `head.json`, and `_progress.json`
last. `head.json` is authoritative: after a crash a chunk may hold entries
beyond `head.blobs`, which readers ignore and the next update overwrites.
`_progress.json.last_height` is the end of the last fully indexed segment.

## Tombstones

`tombstones.json` (optional, mutable) is the public list of blobs withdrawn
from the archive on a legal request. A missing file means no tombstones.

```json
{"tombstones":[{"height":123,"namespace":"<hex 29B>","commitment":"<hex 32B>","date":"YYYY-MM-DD","reason":"..."}]}
```

`namespace` (29 bytes) and `commitment` (32 bytes) are hex; writers use
lowercase like the namespace index, readers compare the decoded bytes (hex is
case-insensitive) and reject a malformed entry instead of ignoring it. Only
the operator commands `cba-ingester tombstone add` and `tombstone add-fibre`
(see [Fibre entries](#fibre-entries)) write the file. It is add-only: an
entry is never removed.

The ingester reloads `tombstones.json` every minute. For each listed blob it
overwrites the blob's record in place, in every `segments/*.seg` whose range
covers the height (catalogued segments and orphans alike), with a
`TombstoneRecord` of exactly the same length. A segment committed later is
written already scrubbed. Offsets do not change, so the `.idx2` row and the
directory entry stay valid: the row still names the blob's namespace and
commitment and now leads to the tombstone. The namespace index stays factual
too.

### Tombstone record

```proto
message Record {
  oneof kind { HeightRecord height = 1; BlobRecord blob = 2; TombstoneRecord tombstone = 3; }
  uint64 filler = 15; // set only on tombstone records
}
message TombstoneRecord {
  uint64 height = 1; bytes namespace = 2; bytes commitment = 3;
  uint32 share_start = 4; uint32 share_end = 5;
  bytes padding = 15; // zero bytes
}
```

`height`, `namespace`, `commitment`, `share_start` and `share_end` are those
of the withdrawn blob; the data, signer and proof are gone. `padding` and
`Record.filler` carry no information and exist only to hit the replaced
length: the frame's length varint is not rewritten, so the tombstone `Record`
must be exactly as long as the `BlobRecord` it replaces.

The bytes are canonical, so every writer produces the same tombstone for the
same blob and length. The field order is fixed, and it is not plain number
order (protobuf-go writes oneof fields last): in `Record`, `filler` (field
15, tag `0x78`) comes first when present, then `tombstone` (field 3, tag
`0x1a`); inside `TombstoneRecord`, fields 1..5 in number order (zero values
omitted), then `padding` (field 15, tag `0x7a`) when present. Every length is
a minimal varint and there are no other fields. The length is matched with
the smallest `filler` and then the smallest `padding`. `filler` is absent (0) or the smallest value with a varint of
`k` bytes (`1`, `128`, `16384`, …, so it adds 2..11 bytes); `padding` is
absent or non-empty. The smallest tombstone (`min`, neither field) and every
length from `min + 2` up can be produced; `min + 1` cannot, since every
added field costs at least two bytes. A blob record is always far longer than
`min + 1`.

For example, with height 1834013, namespace `07`×29, commitment `09`×32 and
shares [70, 200), `min` is 76 bytes and the tombstones of 78 and 79 bytes are
(`internal/segment` `TestTombstoneBytes` pins them):

```text
F  = 08 9df86f  12 1d 07…07  1a 20 09…09  20 46  28 c801     (74 bytes, fields 1..5)
78 = 78 01  1a 4a F                 filler 1, no padding
79 = 1a 4d F  7a 01 00              no filler, 1 byte of padding
```

A record that cannot be scrubbed is reported to the operator
(`cba_tombstone_errors_total`), never retried.

Readers treat a tombstone as a withdrawn blob: `cba verify` reports "blob
withdrawn, see tombstones.json"; `cba export` skips listed blobs (reloading
`tombstones.json` once if it meets a tombstone the loaded file does not
list), and a read API must do the same; the ingester's audit accepts a
tombstone in place of the chain's blob only if `tombstones.json` lists it and
the tombstone keeps the blob's height, namespace, commitment and share range.

A takedown also deletes the blob's [OP layer](#op-layer) objects, whatever
its namespace and whether or not the namespace is still configured.

### Fibre entries

Fibre blobs are withdrawn by blob ID, in a separate top-level array
`"fibre"`:

```json
{"tombstones":[...],"fibre":[{"blob_id":"<hex 33B>","height":1497140,"namespace":"<hex 29B>","date":"YYYY-MM-DD","reason":"..."}]}
```

`blob_id` (33 bytes, version byte ‖ commitment) is hex under the same rules
as above; a malformed entry fails the whole load. `height` and `namespace`
are optional and informational (one tx carrying the ID): the ID is withdrawn
wherever it appears. Only `cba-ingester tombstone add-fibre` adds Fibre
entries, and it deletes `fibre/data/<blob_id_hex>` right after saving the
entry.

Readers written before the array existed decode the file into
`{"tombstones":[...]}` and ignore the unknown key, so they keep working.
Writers must preserve `fibre` and every other unknown top-level key; an old
`tombstone add` would drop them, so only upgraded binaries may write the file,
and every one is upgraded before the first Fibre entry. Writers emit
`tombstones` (an empty array when there are none), then `fibre`, then the
unknown keys in sorted order. Top-level keys are case-sensitive: a key equal
to `tombstones` or `fibre` up to case fails the load, because Go readers
written before the array match it case-insensitively. See
[Fibre takedowns](#fibre-takedowns).

## OP layer

For namespaces listed in its configuration (`-op-namespaces` /
`CBA_OP_NAMESPACES`), the ingester also writes each blob where op-alt-da's
S3 fallback provider looks for it, so an OP rollup can read its history
straight from the bucket. The objects duplicate data already in the
segments; they carry no proof. Verify against the segment (`cba verify`) if
it matters.

Key: `op/<ns_hex>/<hex(keccak256(0x01 0x0c ‖ id))>`, lowercase hex without
`0x`, where `ns_hex` is the 29-byte namespace and `id` the op-alt-da
Celestia blob ID:

- compact ID (40 B): height u64 little-endian ‖ commitment (32 B);
- full ID (48 B): compact ID ‖ `share_offset` u32 LE ‖ `share_size` u32 LE,
  with `share_offset = (share_start / w) * 2w + share_start % w` (the EDS
  index of the first share, `w` = square size) and
  `share_size = share_end − share_start` (shares).

Only the compact-ID key is written. The full-ID key is computed so that
takedowns delete it too; it does not exist unless full writing is added
later.

Value: the blob's raw data (`BlobRecord.data`), with `Cache-Control: public,
max-age=300` and `Content-Type: application/octet-stream`, written with a
conditional PUT. Only share-version-0 blobs get an object (op-alt-da checks
the blob against a v0 commitment); others, and blobs already withdrawn, are
skipped and counted. A height is committed only after its OP objects are
written.

`op/_namespaces.json` lists the configured namespaces; the ingester
overwrites it at every start (an empty list clears it):

```json
{"namespaces":["<hex 29B>", "..."],"updated_at":"2026-10-27T12:00:00Z"}
```

A namespace added later gets its history written once by
`cba-ingester op-backfill` (`internal/opbackfill`), which reads only the
archive (namespace index and `.idx2`).

Takedowns delete the OP keys (compact and full) of every withdrawn blob,
derived straight from `tombstones.json` and from the tombstoned records in
the segments, in any namespace; a missing key counts as deleted. A segment
commit also deletes the OP keys of the listed blobs it holds, so an OP object
written just before a takedown cannot outlive it. Deletes need the R2 token
to allow `DeleteObject`.

Metrics (ingester): `cba_op_objects_written_total` (one per key written),
`cba_op_blobs_skipped_total{reason="share_version|withdrawn"}`,
`cba_op_errors_total` (failed writes of objects and of
`op/_namespaces.json`, and failed OP deletes in segment commits; all
retried), `cba_op_namespaces` (configured namespaces).

## Fibre

Fibre payloads are not in the data square: a `MsgPayForFibre` (PFF) tx on
chain carries a payment promise (with the blob's commitment and padded size)
and the validators' signatures, and the payload itself is held by the
validators' Fibre servers for a limited time (`shard_retention`, 4 h by
default). `cba-ingester fibre-capture` follows the chain head, downloads the
payload of every PFF and every `MsgPaymentPromiseTimeout` while it is still
retrievable, checks it against the paid commitment and writes it under
`fibre/`. It writes nothing else, and the format 2 objects never refer to
`fibre/`.

`fibre/` is outside the format version: `manifest.format_version` stays 2,
format 2 readers ignore `fibre/`, and the Fibre key space has its own
version, `fibre/manifest.json` `fibre_format` (1). Readers refuse any other
`fibre_format`.

The share-v2 system blob a PFF puts in the data square is not added to
segments; it belongs to the proof stage, inside `FibreRecord`.

### Keys

Hex is lowercase without `0x`. `blob_id_hex` is the 33-byte blob ID
(version byte ‖ 32-byte commitment). Index bucket `b` holds heights
`b*10000+1 .. (b+1)*10000`, `b = (h-1)/10000`, like `dir/`.

| Key | Written | `Cache-Control` | `Content-Type` |
|---|---|---|---|
| `fibre/data/<blob_id_hex(66)>` | once, conditional PUT; deleted only by a takedown | `public, max-age=86400` | `application/octet-stream` |
| `fibre/records/<height:012d>-<tx_index:05d>-<msg_index:03d>.pb` | once by capture (conditional PUT); rewritten once by the proof stage | `public, max-age=86400` | `application/octet-stream` |
| `fibre/index/<b:08d>.json` | open bucket rewritten; frozen bucket never rewritten | open `no-cache`, frozen `public, max-age=31536000, immutable` | `application/json` |
| `fibre/manifest.json` | every flush | `no-cache` | `application/json` |

`fibre/data/<id>` is the raw payload (`Blob.Data()`), nothing else. One blob
ID is stored once however many messages carry it. An existing object with
other bytes is an error, never overwritten. A record is the marshaled
`FibreRecord` below, not framed. `tx_index` is the tx's index in
`block.data.txs`, `msg_index` the message's index in the tx body (0 for a
PFF, which has exactly one message).

### Write order

For each captured message: the payload is first spooled to local disk, then
`fibre/data/<id>` (archived only), then the record, then the index bucket
holding its height, then `fibre/manifest.json`. So an index entry always has
its record, and an `archived` record always has its data (unless a takedown
removed it). A crash can leave data without a record, or a record without an
index entry; the restarted capture rewrites them: the data PUT accepts
identical bytes, the record PUT accepts a record of the same message
(height, `tx_index`, `msg_index`, kind and `tx` bytes) and fails on any
other. Index buckets and the manifest are flushed every 30 s, or at once when
100 messages are final.

### Data status

`FibreRecord.data_status`, and `data` in the index (by name):

| Value | Name | Meaning |
|---|---|---|
| 1 | `archived` | `fibre/data/<id>` was written (or already held verified bytes) |
| 2 | `expired` | not retrievable before the deadline |
| 3 | `unrecoverable` | downloaded bytes failed verification, blob version unsupported, or the promise was malformed; reason in `error` |
| 4 | `withdrawn` | listed in `tombstones.json` before capture; not downloaded |
| 5 | `skipped_cap` | daily cap reached; not downloaded |

A record is written only once its status is final, and every status in it
is final: capture never changes it. `archived` is final when the data is
stored, `unrecoverable` at the first failed verification (no retry),
`withdrawn` and `skipped_cap` when the message is taken up (a `skipped_cap`
message is not retried after UTC midnight), `expired` when the deadline
(`block_time + shard_retention − min(10 min, shard_retention / 4)`) passes
without a successful download. Messages still being retried are not public;
`fibre/manifest.json` `pending` counts them.

A `MsgPaymentPromiseTimeout` is captured like a PFF; its client may never
have uploaded, so `expired` is common for it. A record states what was
captured. It is not proof that the tx executed successfully: the tx result
code comes with the proof stage.

### Record

```proto
// Fibre records of the Celestia blob archive (fibre/records/*.pb, see
// docs/format.md, Fibre). Outside format_version 2.
syntax = "proto3";

package archive.v1;

import "google/protobuf/timestamp.proto";

// One Fibre message (MsgPayForFibre or MsgPaymentPromiseTimeout) found on chain.
message FibreRecord {
  uint64 height = 1;                        // block holding the tx
  uint32 tx_index = 2;                      // index in block.data.txs
  uint32 kind = 3;                          // 1 = MsgPayForFibre, 2 = MsgPaymentPromiseTimeout
  bytes  tx = 4;                            // raw tx bytes: promise (+ validator_signatures for kind 1)
  // Proof stage (not written by capture; field numbers fixed):
  uint32 tx_share_start = 5;                // compact-share range of the tx in the ODS
  uint32 tx_share_end = 6;
  bytes  tx_share_proof = 7;                // celestia.core.v1.proof.ShareProof vs data_hash(height)
  bytes  system_blob_proof = 8;             // ShareProof of the share-v2 system blob (kind 1)
  uint32 system_blob_start = 9;
  uint32 system_blob_end = 10;
  bytes  validator_set = 11;                // tendermint.types.ValidatorSet at promise.height+1
  // Capture:
  uint32 data_status = 12;                  // 1 archived, 2 expired, 3 unrecoverable, 4 withdrawn, 5 skipped_cap
  uint64 data_size = 13;                    // payload length when archived
  bytes  data_sha256 = 14;                  // convenience; the commitment is the proof
  google.protobuf.Timestamp captured_at = 15;
  uint32 msg_index = 16;                    // message index in the tx body (0 for kind 1)
  google.protobuf.Timestamp block_time = 17;
  bytes  blob_id = 18;                      // 33 B, derived from the promise; readers recheck
  string error = 19;                        // reason for unrecoverable, empty otherwise
  reserved 20 to 29;                        // proof stage (e.g. tx result code)
}
```

Records are marshaled deterministically. Readers reject a record whose
`kind` is not 1 or 2, whose `blob_id` is not 33 bytes, whose `data_status`
is not 1..5, or whose `data_size` is non-zero for any status but `archived`
(and zero for `archived`). `blob_id` is a convenience copy: readers derive
the ID from the promise in `tx` and compare.

Capture leaves fields 5–11 empty. The proof stage rewrites each record once,
filling fields 5–11 (and, if needed, fields from the reserved range 20–29);
every other field stays byte-identical, and `fibre/data/` is not touched.

### Index

`fibre/index/<b:08d>.json` lists the final messages of the bucket's heights,
sorted by `(height, tx_index, msg_index)`, one entry per message:

```json
{"entries":[{"height":1497140,"tx_index":3,"msg_index":0,"kind":"pff","namespace":"<hex 29B>","blob_id":"<hex 33B>","blob_size":17039360,"data":"archived","data_size":16777216}]}
```

`kind` is `pff` or `timeout`, `namespace` the promise's namespace,
`blob_size` the promise's padded upload size, `data` the status name;
`data_size` (the payload length) is present only for `archived`. Entries are
factual at write time: a later takedown does not rewrite them or the record;
readers consult `tombstones.json`, as with `.idx2` rows.

The open bucket is rewritten (`no-cache`) as messages become final. Bucket
`b` is frozen (rewritten once with the immutable header, then never written
again) only when all three hold:

- `scanned_until ≥ (b+1)*10000`;
- no message of the bucket is pending or not yet indexed;
- `b*10000+1 ≥ first_height` (the bucket lies wholly inside what capture
  scanned).

So the bucket holding `first_height` stays open for a later history stage. A
missing bucket object means no Fibre message is indexed in that range.

### Manifest

```json
{"fibre_format":1,"chain_id":"mocha-5","first_height":1490001,"scanned_until":1497200,"pending":2,"counts":{"archived":10,"expired":1},"bytes_archived":167772160,"daily_cap_bytes":50000000000,"updated_at":"2026-10-27T12:00:00Z"}
```

- `first_height`: the first height capture scanned; no record exists below
  it.
- `scanned_until`: every Fibre message at or below this height has a record
  or is pending.
- `pending`: messages taken up but not yet final.
- `counts`: records written, by status name (absent names are 0).
- `bytes_archived`: total length of the payloads written to `fibre/data/`.
- `daily_cap_bytes`: the capture's cap on bytes written to `fibre/data/` per
  UTC day; a message over it gets `skipped_cap`.
- `updated_at`: RFC3339 UTC.

### Fibre takedowns

A blob ID listed in the `fibre` array of `tombstones.json` (see
[Fibre entries](#fibre-entries)) is never downloaded or written by capture:
a message carrying it gets `withdrawn`. The capture reloads
`tombstones.json` every minute and deletes `fibre/data/<id>` of every listed
ID, including one whose PUT was in flight and a spooled payload replayed after
a restart; the operator command deletes it at once. Records and index
entries stay as written. The edge does not cache `fibre/`; a downstream
cache may keep `fibre/data/<id>` up to its one-day `max-age`.

## Trust

The archive is not trusted by its readers. `cba verify` proves one blob
against a Celestia node: the archived data root must equal the node's header,
and the stored share proof must place exactly the archived blob's shares under
that root. `cba export` only checks each exported blob against the archive's
own records (namespace index entry, record fields and the commitment
recomputed from the data); it does not prove on-chain inclusion. Use
`cba verify` for that.

`cba fibre verify` checks one Fibre record from the archive alone:

- the record's `blob_id` equals the ID derived from the promise in `tx`
  (version byte ‖ commitment);
- the promise is signed by its escrow owner (`PaymentPromise.Validate`);
- for `archived`: the stored payload re-encodes to exactly that blob ID
  (commitment) and its padded upload size equals the promise's `blob_size`,
  and its sha256 equals `data_sha256`;
- a listed ID is reported as withdrawn; any other status is reported as is.

It does not yet prove that the tx is in the chain or that a validator quorum
signed the promise. The proof stage adds both: `tx_share_proof` places the tx
under `data_hash` of its height (checked against a node's header like
`cba verify`), and `validator_signatures` in `tx` are checked against
`validator_set` (at `promise.height + 1`) for quorum.
