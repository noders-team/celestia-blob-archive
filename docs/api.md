# Read API

`api` (`cmd/api`) answers a subset of celestia-node's JSON-RPC from the
archive, so that rollup clients (op-alt-da, ev-node) can read historical
blobs after the network's pruning window. One process serves one network:

| Network | Public endpoint | Archive it reads |
|---|---|---|
| mainnet (`celestia`) | `https://celestia-blobs-rpc.noders.services` | `https://celestia-blobs.noders.services` |
| mocha-5 (`mocha-5`) | `https://celestia-t-blobs-rpc.noders.services` | `https://celestia-t-blobs.noders.services` |

The API holds no credentials: it reads the public archive over HTTP like any
other reader. It refuses to start if the archive's `manifest.chain_id` is not
`-chain-id` or its `format_version` is not 2.

## Protocol

- JSON-RPC 2.0 over HTTP `POST /`, `Content-Type: application/json`.
  Parameters are positional, as celestia-node's go-jsonrpc sends them.
- Batches (a JSON array of calls) are answered in order. A call without
  `id` is a notification and gets no reply.
- `GET /health` is the only other path. Any other path is 404, any other
  HTTP method 405.
- The `Authorization` header is ignored: there are no tokens.
- There is no WebSocket and no subscription (`blob.Subscribe`,
  `header.Subscribe`): a GET with `Upgrade: websocket` gets
  `400 websocket is not supported; use HTTP POST`.

## Methods

The examples are the request bodies and responses of celestia-node v0.34.3
captured in `testdata/celestia-node/v0.34.3`. For `blob.*` the archive
returns the same JSON for the same call (the `cmd/api` end-to-end test and
`internal/api` `TestWireMatchesCelestiaNodeFixtures` check this). For
`header.*` it returns a trimmed header whose commit may carry other
signatures; see
[`header.GetByHeight`](#headergetbyheightheight-and-headernetworkhead).
Long base64 strings are shortened with `…`.

Encodings follow celestia-node: namespaces (29 B), commitments (32 B), data
and proof nodes are base64; heights are JSON numbers; `index` is the EDS
index of the blob's first share.

### `blob.Get(height, namespace, commitment)`

```json
{"jsonrpc":"2.0","id":1,"method":"blob.Get","params":[1500004,"AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770=","6lQZZ156JUlMWdtSHNmB7Ri/L+as/BjICVX2kk86VIA="]}
```

```json
{"id":1,"jsonrpc":"2.0","result":{"namespace":"AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770=","data":"CL7DGBDAwxga4gYKrAIKAggLEgi8+u8NNudCixi+…","share_version":0,"commitment":"6lQZZ156JUlMWdtSHNmB7Ri/L+as/BjICVX2kk86VIA=","index":162}}
```

A share-version-1 blob also has `signer`
(`resp-blob.Get-15300000-sv1.json`):

```json
{"namespace":"AAAAAAAAAAAAAAAAAAAAAAAAAHJlbGF5LWRhdGE=","data":"CNBbAAAAAAADAAAA/AMAAAD3AwAA8wMAAAL5A++D…","share_version":1,"commitment":"X6I2iJEH9q1XTNl385USEK9MqVc+gQTNVxiOMbcPXaM=","signer":"w4fh4VAFDqJSLixtfYaA65ODNZk=","index":798}
```

An archived height without the blob:

```json
{"error":{"code":1,"message":"blob: not found"},"id":1,"jsonrpc":"2.0"}
```

### `blob.GetAll(height, [namespace, …])`

```json
{"jsonrpc":"2.0","id":1,"method":"blob.GetAll","params":[1500004,["AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770="]]}
```

The result is the list of blobs (as in `blob.Get`) of every requested
namespace, in request order. If no namespace has a blob at the height, the
result is `null`, not `[]` and not an error:

```json
{"id":1,"jsonrpc":"2.0","result":null}
```

At most 16 namespaces per call; a duplicate namespace is a bad-params error.
If any blob of the requested namespaces at that height is withdrawn, the
whole call fails with the withdrawn error: the API never returns a partial
list that a client could take for the complete one.

### `blob.GetProof(height, namespace, commitment)`

Returns the blob's NMT proofs, one per row the blob spans, in celestia-node's
encoding (`start` is omitted when 0):

```json
{"jsonrpc":"2.0","id":1,"method":"blob.GetProof","params":[1500004,"AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770=","6lQZZ156JUlMWdtSHNmB7Ri/L+as/BjICVX2kk86VIA="]}
```

```json
{"id":1,"jsonrpc":"2.0","result":[{"start":2,"end":16,"nodes":["AAAA…pylyVlSu","////…O0Ab3"],"is_max_namespace_ignored":true},{"end":16,"nodes":["////…JHR9M"],"is_max_namespace_ignored":true},…]}
```

### `blob.Included(height, namespace, proof, commitment)`

`proof` is a `blob.GetProof` result. The answer is `true` if it equals the
proof of the stored blob, otherwise `false`; a blob the archived height does
not hold is `false` too.

```json
{"jsonrpc":"2.0","id":1,"method":"blob.Included","params":[1500004,"AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770=",[{"start":2,"end":16,"nodes":["…"],"is_max_namespace_ignored":true},…],"6lQZZ156JUlMWdtSHNmB7Ri/L+as/BjICVX2kk86VIA="]}
```

```json
{"id":1,"jsonrpc":"2.0","result":true}
```

A height that is not archived and a withdrawn blob are errors, not `false`.

### `header.GetByHeight(height)` and `header.NetworkHead()`

The archive keeps each height's block header and its commit, not the
validator set or the data availability header. The result is therefore a
trimmed `ExtendedHeader`: `header` and `commit` as celestia-node encodes
them, `validator_set` and `dah` always `null`:

```json
{"jsonrpc":"2.0","id":1,"method":"header.GetByHeight","params":[1500004]}
```

```json
{"id":1,"jsonrpc":"2.0","result":{"header":{"version":{"block":"11","app":"1"},"chain_id":"celestia","height":"1500004","time":"2024-05-22T06:43:57.944783606Z","last_block_id":{…},"data_hash":"EB5004B0AC0A417772C804B88B315DAE0CCAF28BD108E9E4547AF69A93C85AD8",…},"commit":{"height":"1500004","round":0,"block_id":{"hash":"D831E898…","parts":{…}},"signatures":[…]},"validator_set":null,"dah":null}}
```

- The commit is the canonical one: the `LastCommit` of block `h+1`. It
  commits the same block as celestia-node's, but celestia-node serves the
  commit its bridge saw first, so the `signatures` may differ (more, fewer or
  other validators). Do not compare commits byte for byte with a node's.
- `header.NetworkHead()` is the header of the archive's `highest_height`:
  the archive's head, which trails the chain's head by the ingester's lag.
  It is not the chain's head.
- `header.*` calls do not reserve in-flight bytes (a header is small).

Every other method gets:

```json
{"code":-32601,"message":"unsupported method <name>: the archive API serves blob.Get, blob.GetAll, blob.GetProof, blob.Included, header.NetworkHead, header.GetByHeight"}
```

## Errors

Application errors use code `1`, as celestia-node does.

| Case | Code | Message |
|---|---|---|
| Archived height without the blob | 1 | exactly `blob: not found` |
| Height above the archive's `highest_height` M | 1 | `archive: height H not archived yet (header: given height is from the future: highest archived height M)` |
| Height in a gap of the archive | 1 | `archive: height H not archived (gap in the archive; contiguous up to C, highest archived H2)` |
| Height 0 | 1 | `header: height is equal to 0` |
| Withdrawn blob | 1 | `blob withdrawn from this archive (see tombstones.json); fetch it from another source` |
| Stored record failed verification | 1 | `archive: stored record for height H failed verification` |
| Upstream (archive) failure, timeout, internal error | 1 | `archive: upstream unavailable` (details only in the API's logs) |
| Bad params | -32602 | `invalid params: …` |
| Unsupported method | -32601 | see above |
| Parse error | -32700 | `parse error` |
| Invalid request, batch too large | -32600 | `invalid request: …` |

C is `manifest.contiguous_until` (no gaps up to it), H2 the
`highest_height`.

**Why `not found` is reserved.** op-alt-da treats any error containing
`not found` as final, and ev-node skips the height: the blob would be lost to
the rollup for good. The API says `blob: not found` (celestia-node's exact
text) only when it holds the height and knows the blob is not there. Every
other message, including those for heights it does not hold yet or at all,
is worded to avoid the substring, so the client retries or asks another
source. The future-height message contains celestia-node's
`given height is from the future`, which ev-node treats as "retry later".
HTTP-level rejections (404 for an unknown path) avoid it too.

## Withdrawn blobs

A blob withdrawn on a legal request is listed in the archive's public
`tombstones.json` and its record is scrubbed from the segment (see
`docs/format.md`, Tombstones). The API answers every call for it with the
withdrawn error above, never with `blob: not found`: the blob exists on
chain, and the client should fetch it from another source (a celestia-node
that still holds it) rather than conclude it was never posted.

The API reloads `tombstones.json` every minute. It also checks every record
it reads: a scrubbed record that the loaded list does not contain yet (added
since the last reload) is answered as withdrawn at once and triggers an
immediate reload (at most one per 10 s). A blob listed but not yet scrubbed
is refused from the list alone. So a takedown reaches the API within a
minute of the list being written, and as soon as the segment is scrubbed.

## Verification

The API does not trust the bytes it reads either. Before serving a blob
(`Get`, `GetAll`, `GetProof`, `Included`) it checks the stored record against
its stored height record: the proof's shares must decode exactly to the
blob, the commitment recomputed from the data must match, and the stored
share proof must cover the blob's share range under the height's stored data
root. A record that fails is never served; the call gets
`failed verification`, the failure is logged with height, namespace and
commitment and counted in `cba_api_verify_failures_total`. Headers are
checked for consistency (header height, commit of that header's hash).

This proves the archive is self-consistent, not that the data root is the
chain's. A client that wants that checks the blob against its own node:

```sh
cba verify --archive https://celestia-blobs.noders.services \
  --height 1500004 --namespace AAAAAAAAAAAAAAAAAAAAAAAAALz67w0250KL770= \
  --commitment 6lQZZ156JUlMWdtSHNmB7Ri/L+as/BjICVX2kk86VIA= \
  --rpc <consensus-node-grpc:9090>
```

`cba verify` reads the archive directly (not through this API), compares the
archived data root with the node's header and checks the share proof under
it (`docs/format.md`, Trust).

## Limits

| Limit | Default | Flag | When exceeded |
|---|---|---|---|
| Request body | 1 MiB | `-max-body` | HTTP 413 |
| Calls per batch | 16 | `-max-batch` | `-32600` |
| `GetAll` namespaces | 16, no duplicates | `-max-namespaces` | `-32602` |
| Per-call timeout | 30 s | `-request-timeout` | `archive: upstream unavailable` |
| In-flight blob bytes across requests | 1 GiB | `-max-inflight-bytes` | the call waits (up to its timeout) |
| HTTP `ReadHeaderTimeout` / `WriteTimeout` / `IdleTimeout` | 10 s / 60 s / 120 s | — | connection closed |

A blob call reserves 3× the raw segment bytes it reads (decoded records,
base64 JSON, encoded reply) before reading them and keeps the reservation
until the HTTP reply is written; in a batch every call keeps its bytes
until the whole batch reply is written, so a batch whose calls together
need more than the budget fails its later calls as `archive: upstream
unavailable` at their timeout. A call larger than the whole budget runs
alone. `manifest.json` and `tombstones.json` larger than 16 MiB fail the
reload, and the previous state is kept.

In production nginx in front also limits each client IP to 20 requests/s
(burst 40) and 20 connections. The rate limit counts HTTP requests, not
calls: a batch is one request and carries up to 16 calls.

## `GET /health`

```json
{"chain_id":"celestia","highest_height":15314000,"contiguous_until":15314000,"manifest_age_seconds":3,"tombstones":0,"tombstones_age_seconds":41}
```

- 200 if `manifest.json` was loaded less than 60 s ago, otherwise 503 with
  the same body (the API keeps serving from the state it has).
- `highest_height`, `contiguous_until`: from the loaded manifest.
- `manifest_age_seconds`, `tombstones_age_seconds`: time since the last
  successful load of `manifest.json` and `tombstones.json`. A stale
  tombstone list does not make the API unhealthy (it is alerted on
  separately); before the first load both are `-1`.
- `tombstones`: blobs in the loaded tombstone list.

## Running

```sh
api -archive https://celestia-blobs.noders.services -chain-id celestia \
  -listen 127.0.0.1:8471 -metrics-addr 127.0.0.1:9469
```

| Flag | Env | Default |
|---|---|---|
| `-archive` | `CBA_API_ARCHIVE` | required |
| `-chain-id` | `CBA_CHAIN_ID` | required |
| `-listen` | | `127.0.0.1:8471` |
| `-metrics-addr` (empty disables) | | `127.0.0.1:9469` |
| `-cache-bytes` | | `512MiB` |
| `-open-ttl` | | `6s` |
| `-manifest-poll` | | `6s` |
| `-tombstone-poll` | | `1m` |
| `-max-body` | | `1MiB` |
| `-max-batch` | | `16` |
| `-max-namespaces` | | `16` |
| `-request-timeout` | | `30s` |
| `-max-inflight-bytes` | | `1GiB` |

Sizes take a plain byte count or a `KiB`/`MiB`/`GiB` suffix.

- The first load of `manifest.json` and `tombstones.json` must succeed (and
  match `-chain-id`, format 2) before the API listens; otherwise it exits 1.
- SIGTERM or SIGINT stops accepting connections and drains in-flight
  requests for up to 35 s; the exit status is then 0.
- The archive is read over HTTP with up to 64 idle connections, without a
  client-wide timeout: each read is bounded by its call's timeout.
- Reads go through an in-memory cache (`-cache-bytes`) that keeps only bytes
  that cannot change under it: frozen `dir/` buckets and `.idx2` ranges until
  evicted, an open `dir/` bucket (or its absence) for `-open-ttl`. `.seg`,
  `manifest.json` and `tombstones.json` are never cached, so a scrub is seen
  at once.

## Metrics

| Metric | Meaning |
|---|---|
| `cba_api_requests_total{method,outcome}` | calls by method and outcome (`ok`, `not_found`, `not_archived`, `withdrawn`, `bad_request`, `upstream`, `verify_failed`); `method="other"` for calls of no served method, `method="http"` for requests refused before JSON-RPC (body too large, wrong path or HTTP method, WebSocket) |
| `cba_api_request_seconds{method}` | call duration (5 ms … 30 s) |
| `cba_api_upstream_requests_total{outcome}` | reads of the archive below the cache (`ok`, `not_found`, `error`, `canceled`) |
| `cba_api_cache_bytes` | bytes held by the cache |
| `cba_api_cache_hits_total{class}`, `cba_api_cache_misses_total{class}` | cache hits and misses by class (`dir_frozen`, `dir_open`, `idx2`). Every waiter of a shared fetch counts as a miss, so misses can exceed the upstream reads they caused |
| `cba_api_archive_highest_height` | loaded `highest_height` |
| `cba_api_manifest_age_seconds`, `cba_api_tombstones_age_seconds` | time since the last successful load |
| `cba_api_tombstones` | blobs in the loaded tombstone list |
| `cba_api_state_errors_total` | failed reloads (the previous state is kept) |
| `cba_api_verify_failures_total` | stored records refused by verification |
| `cba_api_panics_total` | calls that panicked (answered as upstream unavailable) |

## Not covered yet: ev-node compatibility

The wire format is checked against celestia-node fixtures, not against a
running ev-node. Before ev-node relies on this API, a live check against an
ev-node is needed for: `blob.GetAll` (including `null` for none),
`blob.GetProof` and `blob.Included`, the `time` of `header.GetByHeight`, and
its handling of `header.NetworkHead` returning the archive's head rather than
the chain's (design spec §2). This is out of scope here.
