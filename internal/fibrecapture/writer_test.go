package fibrecapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// hookStore is a store that records write order and can block a PutIfAbsent.
type hookStore struct {
	store.Store
	mu     sync.Mutex
	writes []string // "put <key>" / "putifabsent <key>" / "delete <key>", in order
	opts   map[string]store.PutOptions
	// beforePutIfAbsent, when set, runs before the write of key.
	beforePutIfAbsent func(key string)
}

func newHookStore(t *testing.T) *hookStore {
	return &hookStore{Store: store.NewFS(t.TempDir()), opts: map[string]store.PutOptions{}}
}

func (s *hookStore) log(op, key string, opts []store.PutOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, op+" "+key)
	if len(opts) > 0 {
		s.opts[key] = opts[0]
	}
}

func (s *hookStore) ops() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.writes...)
}

func (s *hookStore) PutIfAbsent(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.log("putifabsent", key, opts)
	if s.beforePutIfAbsent != nil {
		s.beforePutIfAbsent(key)
	}
	return s.Store.PutIfAbsent(ctx, key, b, opts...)
}

func (s *hookStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.log("put", key, opts)
	return s.Store.Put(ctx, key, b, opts...)
}

func (s *hookStore) Delete(ctx context.Context, key string) error {
	s.log("delete", key, nil)
	return s.Store.Delete(ctx, key)
}

// tombstones loads a tombstone set listing the given Fibre blob IDs.
func tombstones(t *testing.T, ids ...[33]byte) *tombstone.Set {
	t.Helper()
	f := tombstone.File{Tombstones: []tombstone.Entry{}}
	for _, id := range ids {
		f.Fibre = append(f.Fibre, tombstone.FibreEntry{BlobID: hex.EncodeToString(id[:]), Date: "2026-10-27", Reason: "test"})
	}
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewFS(t.TempDir())
	if err := st.Put(context.Background(), tombstone.Key, raw); err != nil {
		t.Fatal(err)
	}
	s, err := tombstone.Load(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

var testNow = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

func TestTakedownRacesInFlightPut(t *testing.T) {
	ctx := context.Background()
	data, id, _ := testBlob(t, 1000)
	key := fibrearchive.DataKey(id)
	st := newHookStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	st.beforePutIfAbsent = func(k string) {
		if k == key {
			close(entered)
			<-release
		}
	}
	w := NewWriter(st, fixedNow(testNow))
	if _, err := w.Withdrawn(id); !errors.Is(err, ErrNoTombstoneSet) {
		t.Fatalf("Withdrawn before SetTombstones: %v, want ErrNoTombstoneSet", err)
	}
	if err := w.SetTombstones(ctx, tombstones(t)); err != nil {
		t.Fatal(err)
	}

	type putResult struct {
		withdrawn bool
		err       error
	}
	putDone := make(chan putResult, 1)
	go func() {
		wd, err := w.PutData(ctx, id, data)
		putDone <- putResult{wd, err}
	}()
	<-entered

	setDone := make(chan error, 1)
	go func() { setDone <- w.SetTombstones(ctx, tombstones(t, id)) }()
	select {
	case err := <-setDone:
		t.Fatalf("SetTombstones returned (%v) while a PutData was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	if r := <-putDone; r.err != nil || r.withdrawn {
		t.Fatalf("in-flight PutData = %v, %v; want false, nil", r.withdrawn, r.err)
	}
	if err := <-setDone; err != nil {
		t.Fatalf("SetTombstones: %v", err)
	}
	if ok, err := st.Exists(ctx, key); err != nil || ok {
		t.Fatalf("after takedown %s exists=%v err=%v", key, ok, err)
	}
	if wd, err := w.Withdrawn(id); err != nil || !wd {
		t.Fatalf("Withdrawn after takedown = %v, %v", wd, err)
	}

	before := len(st.ops())
	wd, err := w.PutData(ctx, id, data)
	if err != nil || !wd {
		t.Fatalf("PutData of a withdrawn ID = %v, %v; want true, nil", wd, err)
	}
	if got := st.ops()[before:]; len(got) != 0 {
		t.Errorf("PutData of a withdrawn ID wrote %v", got)
	}
	if ok, _ := st.Exists(ctx, key); ok {
		t.Error("withdrawn data written")
	}
}

func TestPutDataWritesAndRefusesOtherBytes(t *testing.T) {
	ctx := context.Background()
	data, id, _ := testBlob(t, 1000)
	st := newHookStore(t)
	w := NewWriter(st, fixedNow(testNow))
	if _, err := w.PutData(ctx, id, data); !errors.Is(err, ErrNoTombstoneSet) {
		t.Fatalf("PutData before SetTombstones: %v, want ErrNoTombstoneSet", err)
	}
	if err := w.SetTombstones(ctx, tombstones(t)); err != nil {
		t.Fatal(err)
	}
	key := fibrearchive.DataKey(id)
	if wd, err := w.PutData(ctx, id, data); err != nil || wd {
		t.Fatalf("PutData = %v, %v", wd, err)
	}
	if got := st.opts[key]; got != (store.PutOptions{CacheControl: store.CacheSegment, ContentType: store.TypeBinary}) {
		t.Errorf("data options %+v", got)
	}
	if _, err := w.PutData(ctx, id, data); err != nil {
		t.Errorf("identical rewrite: %v", err)
	}

	// Case (c) at the writer level: the key holds other bytes.
	_, id2, _ := testBlob(t, 1000)
	if err := st.Store.Put(ctx, fibrearchive.DataKey(id2), []byte("other bytes")); err != nil {
		t.Fatal(err)
	}
	data2, _, _ := testBlob(t, 1000)
	_, err := w.PutData(ctx, id2, data2)
	if !errors.Is(err, store.ErrExists) {
		t.Fatalf("PutData over other bytes: %v, want ErrExists", err)
	}
}

func TestSetTombstonesJoinsDeleteErrors(t *testing.T) {
	ctx := context.Background()
	_, id, _ := testBlob(t, 100)
	w := NewWriter(failingDelete{store.NewFS(t.TempDir())}, fixedNow(testNow))
	err := w.SetTombstones(ctx, tombstones(t, id))
	if err == nil || !errors.Is(err, errDelete) {
		t.Fatalf("SetTombstones with a failing delete: %v", err)
	}
	if wd, _ := w.Withdrawn(id); !wd {
		t.Error("set not installed after a failed delete")
	}
}

var errDelete = errors.New("delete failed")

type failingDelete struct{ store.Store }

func (failingDelete) Delete(context.Context, string) error { return errDelete }

func testEntry(height int64, tx uint32, status fibrearchive.Status) *Entry {
	e := &Entry{
		Item: Item{
			Height: height, TxIndex: tx, Kind: fibrearchive.KindPayForFibre,
			Tx: []byte(fmt.Sprintf("tx-%d-%d", height, tx)), BlockTime: testNow.Add(-time.Minute), BlobSize: 4096,
		},
		Phase:  PhaseFinal,
		Status: status,
	}
	e.BlobID[0] = 0
	e.BlobID[1] = byte(height)
	e.BlobID[2] = byte(tx)
	e.Namespace[28] = 7
	if status == fibrearchive.StatusArchived {
		e.DataSize = 1000
		e.DataSHA256 = bytes.Repeat([]byte{1}, 32)
	}
	return e
}

func TestPutRecordIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newHookStore(t)
	e := testEntry(5, 1, fibrearchive.StatusArchived)
	if err := NewWriter(st, fixedNow(testNow)).PutRecord(ctx, e); err != nil {
		t.Fatal(err)
	}
	key := e.Key()
	first, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	r, err := fibrearchive.DecodeRecord(first)
	if err != nil {
		t.Fatal(err)
	}
	if r.Height != 5 || r.TxIndex != 1 || !r.CapturedAt.AsTime().Equal(testNow) || r.DataSize != 1000 ||
		!bytes.Equal(r.BlobId, e.BlobID[:]) || !r.BlockTime.AsTime().Equal(e.BlockTime) || r.Error != "" {
		t.Errorf("record %+v", r)
	}

	later := NewWriter(st, fixedNow(testNow.Add(time.Hour)))
	if err := later.PutRecord(ctx, e); err != nil {
		t.Fatalf("redo with another captured_at: %v", err)
	}
	if got, _ := st.Get(ctx, key); !bytes.Equal(got, first) {
		t.Error("redo replaced the first record")
	}

	other := *e
	other.Tx = []byte("another tx")
	if err := later.PutRecord(ctx, &other); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("different tx under the same key: %v, want ErrRecordConflict", err)
	}
}

func TestPutRecordUnrecoverableCarriesError(t *testing.T) {
	ctx := context.Background()
	st := newHookStore(t)
	w := NewWriter(st, fixedNow(testNow))
	e := testEntry(6, 0, fibrearchive.StatusUnrecoverable)
	e.LastErr = "payload does not match"
	if err := w.PutRecord(ctx, e); err != nil {
		t.Fatal(err)
	}
	raw, _ := st.Get(ctx, e.Key())
	r, err := fibrearchive.DecodeRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Error != e.LastErr || r.DataStatus != uint32(fibrearchive.StatusUnrecoverable) {
		t.Errorf("record %+v", r)
	}

	// An expired record never carries the last download error.
	x := testEntry(6, 1, fibrearchive.StatusExpired)
	x.LastErr = "not found"
	if err := w.PutRecord(ctx, x); err != nil {
		t.Fatal(err)
	}
	raw, _ = st.Get(ctx, x.Key())
	if r, _ := fibrearchive.DecodeRecord(raw); r.Error != "" {
		t.Errorf("expired record error %q", r.Error)
	}

	// An invalid record is refused before it is written.
	bad := testEntry(7, 0, fibrearchive.StatusExpired)
	bad.DataSize = 10
	if err := w.PutRecord(ctx, bad); err == nil {
		t.Error("PutRecord accepted data_size on an expired record")
	}
	if ok, _ := st.Exists(ctx, bad.Key()); ok {
		t.Error("invalid record written")
	}
}

func TestExistingDataReverified(t *testing.T) {
	ctx := context.Background()
	data, id, size := testBlob(t, 1000)
	st := newHookStore(t)
	w := NewWriter(st, fixedNow(testNow))

	if _, _, ok, err := w.ExistingData(ctx, id, size); ok || err != nil {
		t.Fatalf("missing object: ok=%v err=%v", ok, err)
	}
	key := fibrearchive.DataKey(id)
	if err := st.Store.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	n, sum, ok, err := w.ExistingData(ctx, id, size)
	want := sha256.Sum256(data)
	if err != nil || !ok || n != uint64(len(data)) || !bytes.Equal(sum, want[:]) {
		t.Fatalf("ExistingData = %d, %x, %v, %v", n, sum, ok, err)
	}

	tampered := bytes.Clone(data)
	tampered[0] ^= 1
	if err := st.Store.Put(ctx, key, tampered); err != nil {
		t.Fatal(err)
	}
	_, _, ok, err = w.ExistingData(ctx, id, size)
	if err == nil || ok || !errors.Is(err, ErrMismatch) || !bytes.Contains([]byte(err.Error()), []byte(key)) {
		t.Fatalf("tampered object: ok=%v err=%v", ok, err)
	}
}

func TestFlushIndexAndManifest(t *testing.T) {
	ctx := context.Background()
	st := newHookStore(t)
	w := NewWriter(st, fixedNow(testNow))
	w.DailyCapBytes = 50_000_000_000
	a := testEntry(10, 0, fibrearchive.StatusArchived)
	b := testEntry(10, 1, fibrearchive.StatusExpired)
	c := testEntry(10_005, 0, fibrearchive.StatusWithdrawn)
	p := testEntry(10_006, 0, 0)
	p.Phase = PhasePending
	s := &State{
		ChainID: "mocha-5", FirstHeight: 1, ScannedUntil: 20_000,
		Counts: map[string]uint64{"archived": 1, "expired": 1, "withdrawn": 1}, BytesArchived: 1000,
		Entries: map[string]*Entry{a.Key(): a, b.Key(): b, c.Key(): c, p.Key(): p},
	}
	indexed, err := w.Flush(ctx, s, func(bucket int64) bool { return bucket == 0 })
	if err != nil {
		t.Fatal(err)
	}
	wantIndexed := map[string]bool{a.Key(): true, b.Key(): true, c.Key(): true}
	if len(indexed) != len(wantIndexed) {
		t.Fatalf("indexed %v", indexed)
	}
	for _, k := range indexed {
		if !wantIndexed[k] {
			t.Errorf("indexed %s", k)
		}
	}

	ops := st.ops()
	want := []string{"put " + fibrearchive.IndexKey(0), "put " + fibrearchive.IndexKey(1), "put " + fibrearchive.ManifestKey}
	if fmt.Sprint(ops) != fmt.Sprint(want) {
		t.Errorf("store order %v, want %v", ops, want)
	}
	if got := st.opts[fibrearchive.IndexKey(0)].CacheControl; got != store.CacheImmutable {
		t.Errorf("bucket 0 cache-control %q, want frozen", got)
	}
	if got := st.opts[fibrearchive.IndexKey(1)].CacheControl; got != store.CacheNoCache {
		t.Errorf("bucket 1 cache-control %q, want open", got)
	}

	raw, err := st.Get(ctx, fibrearchive.IndexKey(0))
	if err != nil {
		t.Fatal(err)
	}
	var b0 fibrearchive.IndexBucket
	if err := json.Unmarshal(raw, &b0); err != nil {
		t.Fatal(err)
	}
	wantEntry := fibrearchive.IndexEntry{
		Height: 10, TxIndex: 0, Kind: "pff", Namespace: hex.EncodeToString(a.Namespace[:]),
		BlobID: hex.EncodeToString(a.BlobID[:]), BlobSize: 4096, Data: "archived", DataSize: 1000,
	}
	if len(b0.Entries) != 2 || b0.Entries[0] != wantEntry || b0.Entries[1].Data != "expired" {
		t.Errorf("bucket 0 %+v", b0.Entries)
	}

	m, err := fibrearchive.LoadManifest(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if m.FibreFormat != 1 || m.ChainID != "mocha-5" || m.FirstHeight != 1 || m.ScannedUntil != 20_000 ||
		m.Pending != 1 || m.Counts["withdrawn"] != 1 || m.BytesArchived != 1000 ||
		m.DailyCapBytes != 50_000_000_000 || !m.UpdatedAt.Equal(testNow) {
		t.Errorf("manifest %+v", m)
	}
}

func TestFlushManifestEmptyCounts(t *testing.T) {
	ctx := context.Background()
	st := newHookStore(t)
	if _, err := NewWriter(st, fixedNow(testNow)).Flush(ctx, &State{ChainID: "mocha-5"}, func(int64) bool { return false }); err != nil {
		t.Fatal(err)
	}
	raw, err := st.Get(ctx, fibrearchive.ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"counts":{}`)) {
		t.Errorf("manifest %s: want counts {}", raw)
	}
}
