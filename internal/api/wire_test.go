package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/nmt"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmttypes "github.com/cometbft/cometbft/types"
	gproto "google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

const nodeFixtures = "../../testdata/celestia-node/v0.34.3"

// nodeCall is a captured JSON-RPC exchange: the request params and the
// response result or error, both as sent on the wire.
type nodeCall struct {
	Params []json.RawMessage
	Result json.RawMessage
	Error  json.RawMessage
}

func loadCall(t *testing.T, name string) nodeCall {
	t.Helper()
	var req struct{ Params []json.RawMessage }
	var resp struct{ Result, Error json.RawMessage }
	for f, v := range map[string]any{"req-" + name + ".json": &req, "resp-" + name + ".json": &resp} {
		raw, err := os.ReadFile(filepath.Join(nodeFixtures, f))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, v); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	return nodeCall{Params: req.Params, Result: resp.Result, Error: resp.Error}
}

func compact(t *testing.T, raw []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func param[T any](t *testing.T, c nodeCall, i int) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(c.Params[i], &v); err != nil {
		t.Fatalf("param %d: %v", i, err)
	}
	return v
}

func ns29(t *testing.T, b []byte) [29]byte {
	t.Helper()
	if len(b) != 29 {
		t.Fatalf("namespace of %d bytes", len(b))
	}
	return [29]byte(b)
}

// archived builds an archive holding the transactions of the block fixture
// (relabelled to height 2; blob and proof JSON carry no height) and reads the
// blobs of nss at that height back from it, checking each record.
func archived(t *testing.T, fixture string, nss ...[]byte) (*archivev1.HeightRecord, [][]*archivev1.BlobRecord) {
	t.Helper()
	blocks := archivetest.Blocks(t, 3, map[int64]string{2: fixture})
	st := store.NewFS(t.TempDir())
	archivetest.Build(t, st, blocks, 1, 2, 2, nil)
	keys := make([][29]byte, len(nss))
	for i, ns := range nss {
		keys[i] = ns29(t, ns)
	}
	hr, recs, err := archiveread.ReadNamespaces(context.Background(), st, 2, keys, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := make([][]*archivev1.BlobRecord, len(recs))
	for i, rs := range recs {
		for _, r := range rs {
			if err := verify.CheckRecord(r.GetBlob(), hr); err != nil {
				t.Fatal(err)
			}
			out[i] = append(out[i], r.GetBlob())
		}
	}
	return hr, out
}

func blobsJSON(t *testing.T, hr *archivev1.HeightRecord, recs ...[]*archivev1.BlobRecord) []byte {
	t.Helper()
	var all []Blob
	for _, rs := range recs {
		for _, r := range rs {
			b, err := BlobFrom(r, hr.SquareSize)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, b)
		}
	}
	raw, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func findBlob(t *testing.T, recs []*archivev1.BlobRecord, commitment []byte) *archivev1.BlobRecord {
	t.Helper()
	for _, r := range recs {
		if bytes.Equal(r.Commitment, commitment) {
			return r
		}
	}
	t.Fatalf("no archived blob with commitment %x", commitment)
	return nil
}

func eq(t *testing.T, what string, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	i := 0
	for i < len(got) && i < len(want) && got[i] == want[i] {
		i++
	}
	from := max(0, i-80)
	t.Errorf("%s differs from the node at byte %d:\n got …%.200s\nwant …%.200s", what, i, got[from:], want[from:])
}

// TestWireMatchesCelestiaNodeFixtures pins the archive's JSON encoding to
// what celestia-node v0.34.3 sent for the same blocks.
func TestWireMatchesCelestiaNodeFixtures(t *testing.T) {
	t.Run("blob.Get", func(t *testing.T) {
		for _, c := range []struct{ call, block string }{
			{"blob.Get-1500004", "mainnet-1500004"},
			{"blob.Get-15300000-sv1", "mainnet-15300000"},
		} {
			call := loadCall(t, c.call)
			ns, cm := param[[]byte](t, call, 1), param[[]byte](t, call, 2)
			hr, recs := archived(t, c.block, ns)
			b, err := BlobFrom(findBlob(t, recs[0], cm), hr.SquareSize)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, c.call, got, compact(t, call.Result))
		}
	})

	t.Run("blob.GetAll", func(t *testing.T) {
		for _, c := range []struct{ call, block string }{
			{"blob.GetAll-1500004-one", "mainnet-1500004"},
			{"blob.GetAll-15300000-two", "mainnet-15300000"},
			{"blob.GetAll-1500004-absent", "mainnet-1500004"},
		} {
			call := loadCall(t, c.call)
			hr, recs := archived(t, c.block, param[[][]byte](t, call, 1)...)
			eq(t, c.call, blobsJSON(t, hr, recs...), compact(t, call.Result))
		}
		// "None" is null, not []: GetAll must encode a nil slice.
		if got := compact(t, loadCall(t, "blob.GetAll-1500004-absent").Result); string(got) != "null" {
			t.Fatalf("absent GetAll result %s, want null", got)
		}
	})

	t.Run("blob.Get not found", func(t *testing.T) {
		call := loadCall(t, "blob.Get-1500004-wrongcm")
		var e JSONRPCError
		if err := json.Unmarshal(call.Error, &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != 1 || e.Message != "blob: not found" {
			t.Fatalf("node error %+v", e)
		}
		got, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, "error", got, compact(t, call.Error))
	})

	t.Run("blob.GetProof", func(t *testing.T) {
		call := loadCall(t, "blob.GetProof-1500004")
		ns, cm := param[[]byte](t, call, 1), param[[]byte](t, call, 2)
		_, recs := archived(t, "mainnet-1500004", ns)
		var sp proof.ShareProof
		if err := sp.Unmarshal(findBlob(t, recs[0], cm).ShareProof); err != nil {
			t.Fatal(err)
		}
		if len(sp.ShareProofs) < 2 {
			t.Fatalf("blob spans %d rows, want ≥ 2", len(sp.ShareProofs))
		}
		ours, err := ProofFrom(&sp)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(ours)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, "proof", got, compact(t, call.Result))

		// The node's proof verifies each row against our stored row roots.
		var node []*nmt.Proof
		if err := json.Unmarshal(call.Result, &node); err != nil {
			t.Fatal(err)
		}
		if len(node) != len(sp.RowProof.RowRoots) {
			t.Fatalf("%d row proofs, %d row roots", len(node), len(sp.RowProof.RowRoots))
		}
		shares := sp.Data
		for i, p := range node {
			n := p.End() - p.Start()
			if n > len(shares) {
				t.Fatalf("row %d: proof covers %d shares, %d left", i, n, len(shares))
			}
			if !p.VerifyInclusion(sha256.New(), ns, shares[:n], sp.RowProof.RowRoots[i]) {
				t.Errorf("row %d: node proof does not verify", i)
			}
			shares = shares[n:]
		}
		if len(shares) != 0 {
			t.Errorf("%d shares not covered by the proofs", len(shares))
		}

		if !ProofsEqual(ours, node) {
			t.Error("ProofsEqual(ours, node) = false")
		}
		// blob.Included sends the GetProof result back.
		if !ProofsEqual(ours, param[[]*nmt.Proof](t, loadCall(t, "blob.Included-1500004"), 2)) {
			t.Error("ProofsEqual(ours, Included param) = false")
		}
		if string(compact(t, loadCall(t, "blob.Included-1500004").Result)) != "true" {
			t.Error("Included fixture is not true")
		}
	})

	t.Run("header.GetByHeight", func(t *testing.T) {
		call := loadCall(t, "header.GetByHeight-1500004")
		cur, next := blockFixture(t, "mainnet-1500004")
		hr, _, err := process.Process(cur, next)
		if err != nil {
			t.Fatal(err)
		}
		var want map[string]json.RawMessage
		if err := json.Unmarshal(call.Result, &want); err != nil {
			t.Fatal(err)
		}
		got := headerParts(t, hr)
		if len(got) != 4 {
			t.Errorf("HeaderFrom keys %d, want 4", len(got))
		}
		eq(t, "header", compact(t, got["header"]), compact(t, want["header"]))
		for _, k := range []string{"validator_set", "dah"} {
			if string(got[k]) != "null" {
				t.Errorf("%s = %s, want null", k, got[k])
			}
		}

		// The node serves the commit its bridge saw when the block was new;
		// the archive stores the canonical one from block h+1's LastCommit.
		// Both commit the same block with different signature sets, so
		// only their common fields can match ours.
		type commitHead struct {
			Height, Round, BlockID json.RawMessage
		}
		head := func(raw json.RawMessage) (commitHead, int) {
			var c struct {
				Height     json.RawMessage   `json:"height"`
				Round      json.RawMessage   `json:"round"`
				BlockID    json.RawMessage   `json:"block_id"`
				Signatures []json.RawMessage `json:"signatures"`
			}
			if err := json.Unmarshal(raw, &c); err != nil {
				t.Fatal(err)
			}
			return commitHead{c.Height, c.Round, compact(t, c.BlockID)}, len(c.Signatures)
		}
		gh, gn := head(got["commit"])
		wh, wn := head(want["commit"])
		if string(gh.Height) != string(wh.Height) || string(gh.Round) != string(wh.Round) || gn != wn {
			t.Errorf("commit height/round/signature count: got %s/%s/%d, want %s/%s/%d",
				gh.Height, gh.Round, gn, wh.Height, wh.Round, wn)
		}
		eq(t, "commit block_id", gh.BlockID, wh.BlockID)

		// The encoding is pinned with the node's own commit stored in a
		// height record: HeaderFrom must give it back byte for byte.
		var nc cmttypes.Commit
		if err := cmtjson.Unmarshal(want["commit"], &nc); err != nil {
			t.Fatal(err)
		}
		ncRaw, err := nc.ToProto().Marshal()
		if err != nil {
			t.Fatal(err)
		}
		withNode := gproto.Clone(hr).(*archivev1.HeightRecord)
		withNode.Commit = ncRaw
		eq(t, "commit", compact(t, headerParts(t, withNode)["commit"]), compact(t, want["commit"]))
	})
}

func headerParts(t *testing.T, hr *archivev1.HeightRecord) map[string]json.RawMessage {
	t.Helper()
	raw, err := HeaderFrom(hr)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func blockFixture(t *testing.T, name string) (cur, next *source.Block) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../testdata/blocks", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct{ Cur, Next *source.Block }
	if err := json.Unmarshal(raw, &f); err != nil || f.Cur == nil || f.Next == nil {
		t.Fatalf("%s: %v", name, err)
	}
	return f.Cur, f.Next
}

func TestProofsEqual(t *testing.T) {
	base := func() []*nmt.Proof {
		p := nmt.NewInclusionProof(2, 16, [][]byte{{1}, {2}}, true)
		q := nmt.NewInclusionProof(0, 8, [][]byte{{3}}, true)
		return []*nmt.Proof{&p, &q}
	}
	if !ProofsEqual(base(), base()) {
		t.Fatal("equal proofs differ")
	}
	absence := nmt.NewAbsenceProof(2, 16, [][]byte{{1}, {2}}, []byte{9}, true)
	for name, b := range map[string][]*nmt.Proof{
		"start":     {ptr(nmt.NewInclusionProof(1, 16, [][]byte{{1}, {2}}, true)), base()[1]},
		"end":       {ptr(nmt.NewInclusionProof(2, 15, [][]byte{{1}, {2}}, true)), base()[1]},
		"node":      {ptr(nmt.NewInclusionProof(2, 16, [][]byte{{1}, {7}}, true)), base()[1]},
		"nodes len": {ptr(nmt.NewInclusionProof(2, 16, [][]byte{{1}}, true)), base()[1]},
		"ignore":    {ptr(nmt.NewInclusionProof(2, 16, [][]byte{{1}, {2}}, false)), base()[1]},
		"leaf hash": {&absence, base()[1]},
		"count":     base()[:1],
		"nil":       {nil, base()[1]},
	} {
		if ProofsEqual(base(), b) || ProofsEqual(b, base()) {
			t.Errorf("%s: different proofs compare equal", name)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestBlobFromRejectsBadRecord(t *testing.T) {
	for name, r := range map[string]*archivev1.BlobRecord{
		"nil":           nil,
		"share version": {ShareVersion: 256},
		"outside":       {ShareStart: 16 * 16},
	} {
		if _, err := BlobFrom(r, 16); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestProofFromRejectsAbsenceProof(t *testing.T) {
	sp := &proof.ShareProof{ShareProofs: []*proof.NMTProof{{Start: 0, End: 1, LeafHash: []byte{1}}}}
	if _, err := ProofFrom(sp); err == nil {
		t.Fatal("ProofFrom accepted an absence proof")
	}
}
