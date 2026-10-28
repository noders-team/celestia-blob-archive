# Celestia Blob Archive

A free public archive of every blob posted to Celestia Mainnet Beta and Mocha, run by [NODERS](https://noders.services).

Celestia nodes keep blob data only for a limited window. This archive keeps all of it in public storage. Every blob is stored with its share proof against the block's data root, so you can check anything you download against the chain yourself.

Live status: **https://celestia-blob-archive.noders.services**

## What is in it

- **All blobs, both networks.** Every `PayForBlobs` blob since genesis lives in immutable segments, each with its share proof and the block header and commit. The history backfill of Mainnet Beta is still running; the status page shows how far it has got.
- **A read API that speaks celestia-node's JSON-RPC.** `blob.Get`, `blob.GetAll`, `blob.GetProof`, `blob.Included` and the basic `header.*` methods work for any archived height. The API checks every blob against its proof before returning it. See [docs/api.md](docs/api.md).
- **op-alt-da fallback for OP Stack rollups.** For Manta Pacific, Derive, Orderly, Towns and Nillion, blobs are also stored under the keys op-alt-da's S3 fallback looks up. A rollup node can re-derive its history from the archive after Celestia's pruning window has passed.
- **Fibre capture.** Fibre data stays on validators for only four hours. The archive downloads every Fibre blob within that window and checks it against the paid commitment. It keeps each blob together with the payment promise and the validator signatures from the chain.

## Endpoints

| | Mainnet Beta | Mocha |
|---|---|---|
| Read API (JSON-RPC) | `https://celestia-blobs-rpc.noders.services` | `https://celestia-t-blobs-rpc.noders.services` |
| Archive files | `https://celestia-blobs.noders.services` | `https://celestia-t-blobs.noders.services` |

Everything is free and needs no key. The read API is rate limited per client.

## Use it

Read blobs the way you would from a celestia-node:

```sh
curl -s https://celestia-blobs-rpc.noders.services -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"blob.GetAll","params":[HEIGHT,["NAMESPACE_B64"]]}'
```

Verify a blob against a node you trust. `cba verify` fetches the blob from the archive, checks its share proof, and compares the data root with a block header from your node:

```sh
make build
bin/cba verify --archive https://celestia-blobs.noders.services \
  --height HEIGHT --namespace NS_B64 --commitment COMMITMENT_B64 --rpc YOUR_NODE:9090
```

Export all blobs of a namespace to files: `bin/cba export --archive URL --namespace NS_B64 --from H1 --to H2 --out DIR`.

Point an OP Stack rollup's op-alt-da server at the archive:

```
--fallback.enabled --fallback.provider s3 --fallback.mode read_fallback
--fallback.s3.endpoint https://celestia-blobs.noders.services --fallback.s3.region auto
--fallback.s3.bucket op --fallback.s3.prefix <namespace hex> --fallback.s3.credential-type anonymous
```

Check a captured Fibre blob: `bin/cba fibre verify --archive URL --height H --tx-index I`.

## Trust model

- You do not need to trust the archive. A blob is only as good as its proof, and the proof is checked against a data root you take from a Celestia node of your choice. `cba verify` does exactly that, and so does the read API before it returns a blob.
- The archive can withhold data, but it cannot alter data without detection.

What `cba fibre verify` checks today:

- the data matches the commitment that was paid for;
- the payment promise carries a valid signature.

Inclusion proofs and validator quorum for Fibre records are the next stage.

## Removal requests

Data posted to Celestia is public, but we remove an archived blob on a legitimate request: write to **abuse@noders.team**. Every removal is listed in the public `tombstones.json` of the network's archive. The blob's place in the archive is overwritten, so the rest of the archive stays verifiable.

## For operators and contributors

- Storage format (version 2): [docs/format.md](docs/format.md)
- Read API reference: [docs/api.md](docs/api.md)
- Example systemd units and environment file: [deploy/](deploy/)

Build with `make build` (outputs `bin/ingester`, `bin/cba`, `bin/api`), test with `make test`.
