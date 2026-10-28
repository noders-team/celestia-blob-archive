package oplayer_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// recordingStore records the options of every PutIfAbsent by key.
type recordingStore struct {
	store.Store
	mu   sync.Mutex
	opts map[string]store.PutOptions
}

func newRecordingStore(t *testing.T) *recordingStore {
	return &recordingStore{Store: store.NewFS(t.TempDir()), opts: map[string]store.PutOptions{}}
}

func (s *recordingStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	if len(opts) > 0 {
		s.opts[key] = opts[0]
	}
	s.mu.Unlock()
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

// fixtureBlobs returns the blob records of mocha-1834013: two share-version-1
// blobs of namespace v1NS and one share-version-0 blob of v0NS.
func fixtureBlobs(t *testing.T) []*archivev1.BlobRecord {
	t.Helper()
	blocks := archivetest.Blocks(t, 2, map[int64]string{1: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[1], blocks[2])
	if err != nil {
		t.Fatal(err)
	}
	return blobs
}

const (
	v1NS = "00000000000000000000000000000000000000636c6235656637626138"
	v0NS = "00000000000000000000000000000000000000893f79d6e2c81a4d08c4"
)

func ns(t *testing.T, s string) [29]byte {
	return [29]byte(mustHex(t, s))
}

func inNS(blobs []*archivev1.BlobRecord, s string) []*archivev1.BlobRecord {
	var out []*archivev1.BlobRecord
	for _, b := range blobs {
		if hex.EncodeToString(b.Namespace) == s {
			out = append(out, b)
		}
	}
	return out
}

// tombSet loads a tombstone set listing withdrawn.
func tombSet(t *testing.T, withdrawn ...*archivev1.BlobRecord) *tombstone.Set {
	t.Helper()
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	for _, b := range withdrawn {
		err := tombstone.Add(ctx, st, tombstone.Entry{
			Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
			Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-27", Reason: "test",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	s, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func keysOf(t *testing.T, b *archivev1.BlobRecord) [2]string {
	t.Helper()
	ob, err := oplayer.FromRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := ob.Keys()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func listOP(t *testing.T, st store.Store) []string {
	t.Helper()
	keys, err := st.List(context.Background(), "op/")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestWriteHeightWritesCompactKey(t *testing.T) {
	ctx := context.Background()
	blobs := fixtureBlobs(t)
	// The fixture's two blobs of one namespace are share version 1; as v0
	// they stand for a height with two writable blobs of one namespace.
	two := inNS(blobs, v1NS)
	if len(two) != 2 {
		t.Fatalf("fixture has %d blobs of %s, want 2", len(two), v1NS)
	}
	for i, b := range two {
		two[i] = proto.Clone(b).(*archivev1.BlobRecord)
		two[i].ShareVersion = 0
	}
	in := append(append([]*archivev1.BlobRecord{}, two...), inNS(blobs, v0NS)...)

	st := newRecordingStore(t)
	w := oplayer.NewWriter(st, [][29]byte{ns(t, v1NS)})
	w.SetTombstones(tombSet(t))
	written, skipped, err := w.WriteHeight(ctx, in)
	if err != nil || written != 2 || skipped != (oplayer.Skipped{}) {
		t.Fatalf("WriteHeight = %d, %+v, %v; want 2, none, nil", written, skipped, err)
	}
	if got := listOP(t, st); len(got) != 2 {
		t.Fatalf("op objects %v, want 2", got)
	}
	for _, b := range two {
		keys := keysOf(t, b)
		got, err := st.Get(ctx, keys[0])
		if err != nil || !bytes.Equal(got, b.Data) {
			t.Errorf("compact key %s: %d bytes, %v; want the blob data", keys[0], len(got), err)
		}
		if o := st.opts[keys[0]]; o.CacheControl != store.CacheOP || o.ContentType != store.TypeBinary {
			t.Errorf("compact key %s: headers %+v", keys[0], o)
		}
		if ok, err := st.Exists(ctx, keys[1]); err != nil || ok {
			t.Errorf("full key %s exists: %v, %v", keys[1], ok, err)
		}
	}
	for _, k := range listOP(t, st) {
		if strings.HasPrefix(k, "op/"+v0NS+"/") {
			t.Errorf("object %s in an unconfigured namespace", k)
		}
	}
}

func TestWriteHeightSkipsListedAndV1(t *testing.T) {
	ctx := context.Background()
	blobs := fixtureBlobs(t)
	v0 := inNS(blobs, v0NS)[0]
	nss := [][29]byte{ns(t, v1NS), ns(t, v0NS)}

	st := store.NewFS(t.TempDir())
	w := oplayer.NewWriter(st, nss)
	w.SetTombstones(tombSet(t, v0))
	written, skipped, err := w.WriteHeight(ctx, blobs)
	if want := (oplayer.Skipped{ShareVersion: 2, Withdrawn: 1}); err != nil || written != 0 || skipped != want {
		t.Fatalf("listed: WriteHeight = %d, %+v, %v; want 0, %+v, nil", written, skipped, err, want)
	}
	if got := listOP(t, st); len(got) != 0 {
		t.Fatalf("op objects %v, want none", got)
	}

	w.SetTombstones(tombSet(t))
	written, skipped, err = w.WriteHeight(ctx, blobs)
	if want := (oplayer.Skipped{ShareVersion: 2}); err != nil || written != 1 || skipped != want {
		t.Fatalf("unlisted: WriteHeight = %d, %+v, %v; want 1, %+v, nil", written, skipped, err, want)
	}
	if got := listOP(t, st); len(got) != 1 || got[0] != keysOf(t, v0)[0] {
		t.Fatalf("op objects %v, want only %s", got, keysOf(t, v0)[0])
	}
}

func TestWriteHeightIdempotent(t *testing.T) {
	ctx := context.Background()
	blobs := fixtureBlobs(t)
	v0 := inNS(blobs, v0NS)[0]
	st := store.NewFS(t.TempDir())
	w := oplayer.NewWriter(st, [][29]byte{ns(t, v0NS)})
	w.SetTombstones(tombSet(t))
	for i := range 2 {
		if n, _, err := w.WriteHeight(ctx, blobs); err != nil || n != 1 {
			t.Fatalf("call %d: written %d, %v", i+1, n, err)
		}
	}
	got, err := st.Get(ctx, keysOf(t, v0)[0])
	if err != nil || !bytes.Equal(got, v0.Data) {
		t.Fatalf("after two writes: %d bytes, %v", len(got), err)
	}
}

func TestWriteHeightBeforeTombstonesIsError(t *testing.T) {
	st := store.NewFS(t.TempDir())
	w := oplayer.NewWriter(st, [][29]byte{ns(t, v0NS)})
	if _, _, err := w.WriteHeight(context.Background(), fixtureBlobs(t)); !errors.Is(err, oplayer.ErrNoTombstoneSet) {
		t.Fatalf("err %v, want ErrNoTombstoneSet", err)
	}
	if got := listOP(t, st); len(got) != 0 {
		t.Fatalf("op objects %v, want none", got)
	}
}

func TestWriteHeightBadRecord(t *testing.T) {
	blobs := fixtureBlobs(t)
	bad := proto.Clone(inNS(blobs, v0NS)[0]).(*archivev1.BlobRecord)
	bad.ShareProof = []byte{0xff}
	st := store.NewFS(t.TempDir())
	w := oplayer.NewWriter(st, [][29]byte{ns(t, v0NS)})
	w.SetTombstones(tombSet(t))
	if _, _, err := w.WriteHeight(context.Background(), []*archivev1.BlobRecord{bad}); !errors.Is(err, oplayer.ErrBadRecord) {
		t.Fatalf("err %v, want ErrBadRecord", err)
	}
}

// blockingStore blocks every PutIfAbsent until release is closed, after
// signalling entered.
type blockingStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func TestSetTombstonesWaitsForInFlightWrite(t *testing.T) {
	blobs := fixtureBlobs(t)
	st := &blockingStore{Store: store.NewFS(t.TempDir()), entered: make(chan struct{}), release: make(chan struct{})}
	w := oplayer.NewWriter(st, [][29]byte{ns(t, v0NS)})
	w.SetTombstones(tombSet(t))

	wrote := make(chan error, 1)
	go func() {
		_, _, err := w.WriteHeight(context.Background(), blobs)
		wrote <- err
	}()
	<-st.entered

	next := tombSet(t, inNS(blobs, v0NS)[0])
	set := make(chan struct{})
	go func() {
		w.SetTombstones(next)
		close(set)
	}()
	select {
	case <-set:
		t.Fatal("SetTombstones returned while a write was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(st.release)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	select {
	case <-set:
	case <-time.After(5 * time.Second):
		t.Fatal("SetTombstones did not return after the write finished")
	}
}

func TestConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	if got, err := oplayer.LoadConfig(ctx, st); err != nil || len(got) != 0 {
		t.Fatalf("missing file: %x, %v", got, err)
	}
	nss := [][29]byte{ns(t, v1NS), ns(t, v0NS)}
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	if err := oplayer.WriteConfig(ctx, st, nss, now); err != nil {
		t.Fatal(err)
	}
	got, err := oplayer.LoadConfig(ctx, st)
	if err != nil || len(got) != 2 || got[0] != nss[0] || got[1] != nss[1] {
		t.Fatalf("loaded %x, %v; want %x", got, err, nss)
	}
	if err := oplayer.WriteConfig(ctx, st, nil, now); err != nil {
		t.Fatal(err)
	}
	if got, err := oplayer.LoadConfig(ctx, st); err != nil || len(got) != 0 {
		t.Fatalf("after clearing: %x, %v", got, err)
	}
	raw, err := st.Get(ctx, oplayer.ConfigKey)
	if err != nil || !strings.Contains(string(raw), `"namespaces":[]`) {
		t.Fatalf("cleared config %s, %v; want an empty list", raw, err)
	}
}
