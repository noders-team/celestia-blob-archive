// Package opcompat checks that op-alt-da's own S3 fallback provider reads the
// OP objects the archive writes. It is a separate module that imports only
// op-alt-da and go-square, never the archive, so the two dependency graphs
// never meet. The fixture is testdata/opcompat, written by
// internal/oplayer's TestOpcompatFixture.
package opcompat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/celestiaorg/go-square/merkle"
	"github.com/celestiaorg/go-square/v4/inclusion"
	"github.com/celestiaorg/go-square/v4/share"
	"github.com/celestiaorg/op-alt-da/fallback"
	"github.com/celestiaorg/op-alt-da/fallback/s3"
)

// subtreeRootThreshold is celestia-app's appconsts.SubtreeRootThreshold,
// which celestia-node's blob.NewBlob uses for the commitment.
const subtreeRootThreshold = 64

var fixtureDir = filepath.Join("..", "..", "testdata", "opcompat")

// fixtureHeights are the mainnet heights of the fixture blobs.
var fixtureHeights = map[uint64]bool{10_000_000: true, 15_300_000: true}

type vector struct {
	Namespace  string `json:"namespace"`
	Commitment string `json:"commitment"`
	DataSHA256 string `json:"data_sha256"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "vectors.json"))
	if err != nil {
		t.Fatalf("fixture missing (go test ./internal/oplayer/ -run OpcompatFixture -update-opcompat): %v", err)
	}
	var vs []vector
	if err := json.Unmarshal(raw, &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs) == 0 {
		t.Fatal("vectors.json lists no blobs")
	}
	return vs
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// bucketServer serves dir path-style as /<bucket>/<key> (bucket "op" maps to
// dir/op) and records every request as "METHOD path".
type bucketServer struct {
	dir string
	mu  sync.Mutex
	log []string
}

func (s *bucketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.log = append(s.log, r.Method+" "+r.URL.Path)
	s.mu.Unlock()
	b, err := os.ReadFile(filepath.Join(s.dir, filepath.FromSlash(strings.TrimPrefix(r.URL.Path, "/"))))
	if r.Method != http.MethodGet || err != nil {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(b)
}

func (s *bucketServer) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func newProvider(t *testing.T, endpoint, nsHex string) *s3.S3Provider {
	t.Helper()
	p, err := s3.NewS3Provider(context.Background(), s3.Config{
		Bucket: "op", Prefix: nsHex, Endpoint: endpoint, Region: "auto",
		CredentialType: "anonymous", Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOpAltDAProviderReadsArchiveObjects(t *testing.T) {
	ctx := context.Background()
	srv := &bucketServer{dir: fixtureDir}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	vs := loadVectors(t)
	for i, v := range vs {
		comm := mustHex(t, v.Commitment)
		if len(comm) != 2+40 || comm[0] != 0x01 || comm[1] != 0x0c {
			t.Fatalf("vector %d: commitment %s is not 0x01 0x0c ‖ compact ID", i, v.Commitment)
		}
		// The height is u64 little-endian, checked against the fixture's
		// real heights independently of oplayer.
		if h := binary.LittleEndian.Uint64(comm[2:10]); !fixtureHeights[h] {
			t.Errorf("vector %d: ID height %d (LE) is not a fixture height", i, h)
		}
		data, err := newProvider(t, ts.URL, v.Namespace).Get(ctx, comm)
		if err != nil {
			t.Fatalf("vector %d: Get: %v", i, err)
		}
		if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != v.DataSHA256 {
			t.Errorf("vector %d: data sha256 %x, want %s", i, sum, v.DataSHA256)
		}

		// CelestiaServer.getFallback accepts the data only if the share
		// version 0 commitment of the data equals the ID's commitment. This
		// mirrors the unexported CelestiaServer.verifyFallbackCommitment
		// (celestia_server.go) with the same primitives, because importing
		// op-alt-da's root package would pull in celestia-node.
		ns, err := share.NewNamespaceFromBytes(mustHex(t, v.Namespace))
		if err != nil {
			t.Fatal(err)
		}
		blob, err := share.NewV0Blob(ns, data)
		if err != nil {
			t.Fatal(err)
		}
		got, err := inclusion.CreateCommitment(blob, merkle.HashFromByteSlices, subtreeRootThreshold)
		if err != nil {
			t.Fatal(err)
		}
		if want := comm[2+8:]; !bytes.Equal(got, want) {
			t.Errorf("vector %d: data commits to %x, ID has %x", i, got, want)
		}
	}

	// An ID of the archive that was never written is op-alt-da's not found.
	unknown := mustHex(t, vs[0].Commitment)
	unknown[len(unknown)-1] ^= 0xff
	if data, err := newProvider(t, ts.URL, vs[0].Namespace).Get(ctx, unknown); !errors.Is(err, fallback.ErrNotFound) {
		t.Errorf("unknown commitment: %d bytes, %v; want fallback.ErrNotFound", len(data), err)
	}

	reqs := srv.requests()
	if len(reqs) != len(vs)+1 {
		t.Errorf("%d requests, want %d: %v", len(reqs), len(vs)+1, reqs)
	}
	want := regexp.MustCompile(`^GET /op/[0-9a-f]{58}/[0-9a-f]{64}$`)
	for i, r := range reqs {
		if !want.MatchString(r) {
			t.Errorf("request %q does not match %s", r, want)
		}
		if i < len(vs) && !strings.HasPrefix(r, "GET /op/"+vs[i].Namespace+"/") {
			t.Errorf("request %q is not under vector %d's namespace", r, i)
		}
	}
}

// Vector (a) of internal/oplayer/keys_test.go: the compact commitment from
// op-alt-da's fallback/s3/s3_test.go and the key our oplayer.Key derives
// for it, pinned there as opAltDAKeyHash.
const (
	vectorCommitment = "010ca2f3ac00000000007b156d9cdc8698d364787f4ae8a8e18346e55756533f6a4562a199b13d60548e"
	vectorNamespace  = "00000000000000000000000000000000000000ca1de12ac5a629c3c42f"
	vectorKey        = "op/" + vectorNamespace + "/47df61f28c24d5cd376ad7de3c2fcd35098c45add9e283b1a162c449585f85b4"
)

func TestKeyVectorMatchesProvider(t *testing.T) {
	srv := &bucketServer{dir: t.TempDir()}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	_, err := newProvider(t, ts.URL, vectorNamespace).Get(context.Background(), mustHex(t, vectorCommitment))
	if !errors.Is(err, fallback.ErrNotFound) {
		t.Fatalf("Get on an empty bucket: %v, want fallback.ErrNotFound", err)
	}
	reqs := srv.requests()
	if len(reqs) != 1 || reqs[0] != "GET /"+vectorKey {
		t.Fatalf("provider requested %v, want [GET /%s]", reqs, vectorKey)
	}
}
