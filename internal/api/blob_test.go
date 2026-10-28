package api

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/celestiaorg/nmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

var segTwoKey = archive.SegmentKey(5, 6)

// flipData flips the first data byte of the blob with commitment cm and
// keeps its commitment, so only CheckRecord can notice.
func flipData(cm []byte) archivetest.Mutate {
	return func(hr *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		for _, b := range blobs {
			if bytes.Equal(b.Commitment, cm) {
				b.Data[0] ^= 0xff
			}
		}
		return hr, blobs
	}
}

func withdraw(t *testing.T, st store.Store, segKey string, cm []byte) {
	t.Helper()
	if n := archivetest.Withdraw(t, st, segKey, func(b *archivev1.BlobRecord) bool { return bytes.Equal(b.Commitment, cm) }); n != 1 {
		t.Fatalf("withdrew %d records", n)
	}
}

func okResult(t *testing.T, r rpcResponse) json.RawMessage {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("error %+v", r.Error)
	}
	return r.Result
}

func TestBlobGetOK(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	res := okResult(t, ts.call(t, "blob.Get", fixtureParams(t, "blob.Get-1500004", hOne)...))
	var b Blob
	if err := json.Unmarshal(res, &b); err != nil {
		t.Fatal(err)
	}
	eq(t, "blob.Get", compact(t, res), compact(t, loadCall(t, "blob.Get-1500004").Result))
}

func TestBlobGetAllOK(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	for _, c := range []struct {
		call string
		h    int64
	}{
		{"blob.GetAll-1500004-one", hOne},
		{"blob.GetAll-15300000-two", hTwo},
		{"blob.GetAll-1500004-absent", hOne},
	} {
		res := okResult(t, ts.call(t, "blob.GetAll", fixtureParams(t, c.call, c.h)...))
		var bs []Blob
		if err := json.Unmarshal(res, &bs); err != nil {
			t.Fatal(err)
		}
		eq(t, c.call, compact(t, res), compact(t, loadCall(t, c.call).Result))
	}

	// Request order: the namespaces of "two" reversed give its blobs
	// grouped in the reversed order.
	call := loadCall(t, "blob.GetAll-15300000-two")
	nss := param[[][]byte](t, call, 1)
	var want []Blob
	if err := json.Unmarshal(call.Result, &want); err != nil {
		t.Fatal(err)
	}
	var wantRev []Blob
	for _, ns := range [][]byte{nss[1], nss[0]} {
		for _, b := range want {
			if bytes.Equal(b.Namespace, ns) {
				wantRev = append(wantRev, b)
			}
		}
	}
	wantJSON, _ := json.Marshal(wantRev)
	res := okResult(t, ts.call(t, "blob.GetAll", hTwo, [][]byte{nss[1], nss[0]}))
	eq(t, "reversed", compact(t, res), wantJSON)
}

func TestBlobGetProofOK(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	res := okResult(t, ts.call(t, "blob.GetProof", fixtureParams(t, "blob.GetProof-1500004", hOne)...))
	var p []*nmt.Proof
	if err := json.Unmarshal(res, &p); err != nil {
		t.Fatal(err)
	}
	eq(t, "blob.GetProof", compact(t, res), compact(t, loadCall(t, "blob.GetProof-1500004").Result))
}

func TestBlobIncludedTrue(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	res := okResult(t, ts.call(t, "blob.Included", fixtureParams(t, "blob.Included-1500004", hOne)...))
	if string(res) != "true" {
		t.Fatalf("Included = %s", res)
	}
}

func TestBlobGetWrongCommitmentNotFound(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	resp, body := post(t, ts.url, request(t, "blob.Get", fixtureParams(t, "blob.Get-1500004-wrongcm", hOne)...))
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	var r struct{ Error json.RawMessage }
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatal(err)
	}
	eq(t, "error", r.Error, compact(t, loadCall(t, "blob.Get-1500004-wrongcm").Error))
	if string(r.Error) != `{"code":1,"message":"blob: not found"}` {
		t.Fatalf("error %s", r.Error)
	}
}

func TestIncludedFalseForMissingBlob(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	p := fixtureParams(t, "blob.Included-1500004", hOne)
	p[3] = loadCall(t, "blob.Get-1500004-wrongcm").Params[2]
	if res := okResult(t, ts.call(t, "blob.Included", p...)); string(res) != "false" {
		t.Fatalf("Included = %s", res)
	}
	// An archived height without the blob's namespace too.
	p = fixtureParams(t, "blob.Included-1500004", hOne)
	p[1] = testNamespace(1)
	if res := okResult(t, ts.call(t, "blob.Included", p...)); string(res) != "false" {
		t.Fatalf("Included = %s", res)
	}
	// A height that is not archived is an error, not false.
	p = fixtureParams(t, "blob.Included-1500004", hGap)
	if e := mustErr(t, ts.call(t, "blob.Included", p...)); !strings.Contains(e.Message, "not archived") {
		t.Fatalf("%+v", e)
	}
}

func TestIncludedFalseForOtherProof(t *testing.T) {
	ts := serve(t, buildArchive(t, nil), Config{})
	call := loadCall(t, "blob.Included-1500004")
	proofs := param[[]*nmt.Proof](t, call, 2)
	for name, other := range map[string][]*nmt.Proof{
		"fewer rows": proofs[:len(proofs)-1],
		"other start": append([]*nmt.Proof{ptr(nmt.NewInclusionProof(proofs[0].Start()+1, proofs[0].End(),
			proofs[0].Nodes(), true))}, proofs[1:]...),
		"other nodes": append([]*nmt.Proof{ptr(nmt.NewInclusionProof(proofs[0].Start(), proofs[0].End(),
			proofs[0].Nodes()[:1], true))}, proofs[1:]...),
	} {
		p := fixtureParams(t, "blob.Included-1500004", hOne)
		p[2] = other
		if res := okResult(t, ts.call(t, "blob.Included", p...)); string(res) != "false" {
			t.Errorf("%s: Included = %s", name, res)
		}
	}
}

func TestGetAllWithdrawnFailsWholeCall(t *testing.T) {
	call := loadCall(t, "blob.GetAll-15300000-two")
	var blobs []Blob
	if err := json.Unmarshal(call.Result, &blobs); err != nil {
		t.Fatal(err)
	}
	last := blobs[len(blobs)-1]
	params := fixtureParams(t, "blob.GetAll-15300000-two", hTwo)
	const withdrawn = "blob withdrawn from this archive (see tombstones.json); fetch it from another source"

	for name, setup := range map[string]func(*testing.T, store.Store){
		"listed":   func(t *testing.T, st store.Store) { addTombstone(t, st, hTwo, last.Namespace, last.Commitment) },
		"scrubbed": func(t *testing.T, st store.Store) { withdraw(t, st, segTwoKey, last.Commitment) },
		"listed, scrubbed": func(t *testing.T, st store.Store) {
			addTombstone(t, st, hTwo, last.Namespace, last.Commitment)
			withdraw(t, st, segTwoKey, last.Commitment)
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := buildArchive(t, nil)
			setup(t, st)
			ts := serve(t, st, Config{})
			e := mustErr(t, ts.call(t, "blob.GetAll", params...))
			if e.Code != 1 || e.Message != withdrawn {
				t.Fatalf("%+v", e)
			}
			// The other namespace alone is unaffected.
			var other [][]byte
			for _, ns := range param[[][]byte](t, call, 1) {
				if !bytes.Equal(ns, last.Namespace) {
					other = append(other, ns)
				}
			}
			if len(other) != 1 {
				t.Fatalf("namespaces %x", other)
			}
			okResult(t, ts.call(t, "blob.GetAll", hTwo, other))
			// Get and GetProof report the same; Included is an error.
			for _, m := range []string{"blob.Get", "blob.GetProof"} {
				if e := mustErr(t, ts.call(t, m, hTwo, last.Namespace, last.Commitment)); e.Message != withdrawn {
					t.Errorf("%s: %+v", m, e)
				}
			}
			if name == "scrubbed" && ts.state.reloadCount() == 0 {
				t.Error("a tombstone record the set does not list did not reload the set")
			}
		})
	}
}

func TestRecordFailingVerificationIsNeverServed(t *testing.T) {
	call := loadCall(t, "blob.Get-1500004")
	cm := param[[]byte](t, call, 2)
	ts := serve(t, buildArchive(t, flipData(cm)), Config{})
	for _, m := range []struct{ method, fixture string }{
		{"blob.Get", "blob.Get-1500004"},
		{"blob.GetAll", "blob.GetAll-1500004-one"},
		{"blob.GetProof", "blob.GetProof-1500004"},
		{"blob.Included", "blob.Included-1500004"},
	} {
		e := mustErr(t, ts.call(t, m.method, fixtureParams(t, m.fixture, hOne)...))
		if e.Code != 1 || e.Message != "archive: stored record for height 2 failed verification" {
			t.Errorf("%s: %+v", m.method, e)
		}
	}
	if n := testutil.ToFloat64(ts.m.VerifyFailures); n != 4 {
		t.Fatalf("cba_api_verify_failures_total = %v, want 4", n)
	}
}

// TestForgedSquareSizeIsNeverServed: a height record whose square_size is
// not the proofs' width would make BlobFrom serve a wrong index; it fails
// verification instead.
func TestForgedSquareSizeIsNeverServed(t *testing.T) {
	mut := func(hr *archivev1.HeightRecord, blobs []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord) {
		hr = proto.Clone(hr).(*archivev1.HeightRecord)
		hr.SquareSize *= 2
		return hr, blobs
	}
	ts := serve(t, buildArchive(t, mut), Config{})
	for _, m := range []struct{ method, fixture string }{
		{"blob.Get", "blob.Get-1500004"},
		{"blob.GetAll", "blob.GetAll-1500004-one"},
		{"blob.GetProof", "blob.GetProof-1500004"},
		{"blob.Included", "blob.Included-1500004"},
	} {
		e := mustErr(t, ts.call(t, m.method, fixtureParams(t, m.fixture, hOne)...))
		if e.Code != 1 || e.Message != "archive: stored record for height 2 failed verification" {
			t.Errorf("%s: %+v", m.method, e)
		}
	}
	if n := testutil.ToFloat64(ts.m.VerifyFailures); n != 4 {
		t.Fatalf("cba_api_verify_failures_total = %v, want 4", n)
	}
}

// A result that cannot be encoded is answered like any other internal
// failure, with the documented upstream message.
func TestEncodeFailureIsUpstream(t *testing.T) {
	_, rerr := encode(make(chan int))
	if rerr != errUpstream {
		t.Fatalf("encode failure = %v, want %v", rerr, errUpstream)
	}
}
