# celestia-node v0.34.3 response fixtures

Captured 2026-10-27 from the NODERS mainnet archival bridge (celestia-node v0.34.3,
commit 7909f857dbbe35bb9fca75dc01fb6181997ad7f3) through the public read-only endpoint
https://celestia-archive-da.noders.services (JSON-RPC over HTTP POST). Public chain data,
nothing redacted. `req-*.json` is the exact request body, `resp-*.json` the raw response.

| Fixture | Height | What |
|---|---|---|
| blob.Get-1500004 | 1500004 | one blob, share version 0, shares [82,152) in a 16×16 square (5 rows); same block as testdata/blocks/mainnet-1500004.json |
| blob.GetProof-1500004 | 1500004 | per-row NMT proofs of that blob |
| blob.GetAll-1500004-one | 1500004 | GetAll for its namespace |
| blob.GetAll-1500004-absent | 1500004 | GetAll for a namespace with no blob at that height → `result: null` |
| blob.Get-1500004-wrongcm | 1500004 | Get with another blob's commitment → error `{"code":1,"message":"blob: not found"}` |
| blob.Included-1500004 | 1500004 | Included with the proof from GetProof → `true` |
| header.GetByHeight-1500004 | 1500004 | full ExtendedHeader (the archive API returns only `header` and `commit`) |
| blob.GetAll-15300000-two | 15300000 | GetAll for two namespaces (relay-data, share version 1 with signer; and 0x48ebd3411d6431afa0c5); same block as testdata/blocks/mainnet-15300000.json |
| blob.Get-15300000-sv1 | 15300000 | Get of the share-version-1 blob (has `signer`) |
| blob.Get-future | 99999999 | error text for a height above the network head |
