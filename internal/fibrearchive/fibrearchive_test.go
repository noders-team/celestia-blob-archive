package fibrearchive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

func TestKeys(t *testing.T) {
	var id [33]byte
	for i := 1; i < len(id); i++ {
		id[i] = 0xab
	}
	if got, want := DataKey(id), "fibre/data/00"+strings.Repeat("ab", 32); got != want {
		t.Errorf("DataKey = %q, want %q", got, want)
	}
	if got := len(strings.TrimPrefix(DataKey(id), DataPrefix)); got != 66 {
		t.Errorf("DataKey hex is %d chars, want 66", got)
	}
	if got, want := RecordKey(1497140, 3, 0), "fibre/records/000001497140-00003-000.pb"; got != want {
		t.Errorf("RecordKey = %q, want %q", got, want)
	}
	if got, want := IndexKey(149), "fibre/index/00000149.json"; got != want {
		t.Errorf("IndexKey = %q, want %q", got, want)
	}
	for h, want := range map[int64]int64{1: 0, 10000: 0, 10001: 1, 1497140: 149} {
		if got := Bucket(h); got != want {
			t.Errorf("Bucket(%d) = %d, want %d", h, got, want)
		}
	}
}

func TestStatusAndKindNames(t *testing.T) {
	want := []string{"archived", "expired", "unrecoverable", "withdrawn", "skipped_cap"}
	for i, w := range want {
		if got := Status(i + 1).String(); got != w {
			t.Errorf("Status(%d) = %q, want %q", i+1, got, w)
		}
	}
	if StatusSkippedCap != 5 {
		t.Errorf("StatusSkippedCap = %d, want 5", StatusSkippedCap)
	}
	if KindPayForFibre.String() != "pff" || KindTimeout.String() != "timeout" {
		t.Errorf("kind names %q %q, want pff timeout", KindPayForFibre, KindTimeout)
	}
}

func validRecord() *archivev1.FibreRecord {
	id := make([]byte, 33)
	id[1] = 0xab
	return &archivev1.FibreRecord{
		Height:     1497140,
		TxIndex:    3,
		Kind:       uint32(KindPayForFibre),
		Tx:         []byte("raw tx"),
		DataStatus: uint32(StatusArchived),
		DataSize:   16777216,
		DataSha256: bytes.Repeat([]byte{1}, 32),
		CapturedAt: timestamppb.New(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)),
		BlockTime:  timestamppb.New(time.Date(2026, 10, 27, 11, 59, 0, 0, time.UTC)),
		BlobId:     id,
	}
}

func TestRecordRoundTripAndValidation(t *testing.T) {
	b, err := EncodeRecord(validRecord())
	if err != nil {
		t.Fatal(err)
	}
	r, err := DecodeRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := EncodeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, b2) {
		t.Error("record does not round-trip byte for byte")
	}

	expired := validRecord()
	expired.DataStatus = uint32(StatusExpired)
	expired.DataSize = 0
	if _, err := roundTrip(expired); err != nil {
		t.Errorf("expired record with data_size 0: %v", err)
	}

	for name, mut := range map[string]func(*archivev1.FibreRecord){
		"bad kind":              func(r *archivev1.FibreRecord) { r.Kind = 3 },
		"kind 0":                func(r *archivev1.FibreRecord) { r.Kind = 0 },
		"32-byte blob_id":       func(r *archivev1.FibreRecord) { r.BlobId = r.BlobId[:32] },
		"status 0":              func(r *archivev1.FibreRecord) { r.DataStatus = 0 },
		"status 6":              func(r *archivev1.FibreRecord) { r.DataStatus = 6 },
		"archived data_size 0":  func(r *archivev1.FibreRecord) { r.DataSize = 0 },
		"expired data_size > 0": func(r *archivev1.FibreRecord) { r.DataStatus = uint32(StatusExpired) },
	} {
		r := validRecord()
		mut(r)
		if _, err := roundTrip(r); err == nil {
			t.Errorf("%s: DecodeRecord accepted it", name)
		}
	}
	if _, err := DecodeRecord([]byte{0xff}); err == nil {
		t.Error("DecodeRecord accepted garbage")
	}
}

// roundTrip marshals r without validation and decodes it.
func roundTrip(r *archivev1.FibreRecord) (*archivev1.FibreRecord, error) {
	b, err := EncodeRecord(r)
	if err != nil {
		return nil, err
	}
	return DecodeRecord(b)
}

func TestSameMessage(t *testing.T) {
	a := validRecord()
	b := validRecord()
	b.DataStatus = uint32(StatusExpired)
	b.DataSize = 0
	b.CapturedAt = timestamppb.Now()
	if !SameMessage(a, b) {
		t.Error("records of the same message differ")
	}
	for name, mut := range map[string]func(*archivev1.FibreRecord){
		"height":    func(r *archivev1.FibreRecord) { r.Height++ },
		"tx_index":  func(r *archivev1.FibreRecord) { r.TxIndex++ },
		"msg_index": func(r *archivev1.FibreRecord) { r.MsgIndex++ },
		"kind":      func(r *archivev1.FibreRecord) { r.Kind = uint32(KindTimeout) },
		"tx":        func(r *archivev1.FibreRecord) { r.Tx = []byte("other") },
	} {
		c := validRecord()
		mut(c)
		if SameMessage(a, c) {
			t.Errorf("different %s is the same message", name)
		}
	}
}

func entry(h int64, tx, msg uint32) IndexEntry {
	return IndexEntry{Height: h, TxIndex: tx, MsgIndex: msg, Kind: "pff", Namespace: "00", BlobID: "00ab", BlobSize: 10, Data: "expired"}
}

func keyOf(e IndexEntry) [3]int64 { return [3]int64{e.Height, int64(e.TxIndex), int64(e.MsgIndex)} }

func TestMergeIndexDedupesAndSorts(t *testing.T) {
	b, err := MergeIndex(nil, []IndexEntry{entry(5, 0, 0), entry(3, 2, 0), entry(3, 1, 1), entry(3, 1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	var got [][3]int64
	for _, e := range b.Entries {
		got = append(got, keyOf(e))
	}
	want := [][3]int64{{3, 1, 0}, {3, 1, 1}, {3, 2, 0}, {5, 0, 0}}
	if !slices.Equal(got, want) {
		t.Fatalf("entries %v, want %v", got, want)
	}

	again, err := MergeIndex(b, []IndexEntry{entry(3, 2, 0), entry(4, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Entries) != 5 || keyOf(again.Entries[3]) != [3]int64{4, 0, 0} {
		t.Errorf("identical re-add changed the bucket: %v", again.Entries)
	}

	conflict := entry(3, 2, 0)
	conflict.Data = "archived"
	conflict.DataSize = 7
	if _, err := MergeIndex(b, []IndexEntry{conflict}); err == nil {
		t.Error("conflicting entry accepted")
	}
	if _, err := MergeIndex(nil, []IndexEntry{entry(1, 0, 0), conflict, entry(3, 2, 0)}); err == nil {
		t.Error("conflict within one add accepted")
	}
}

func TestIndexEntryJSONShape(t *testing.T) {
	e := IndexEntry{Height: 1497140, TxIndex: 3, Kind: "pff", Namespace: "ns", BlobID: "id", BlobSize: 17039360, Data: "archived", DataSize: 16777216}
	b, err := json.Marshal(IndexBucket{Entries: []IndexEntry{e}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"entries":[{"height":1497140,"tx_index":3,"msg_index":0,"kind":"pff","namespace":"ns","blob_id":"id","blob_size":17039360,"data":"archived","data_size":16777216}]}`
	if string(b) != want {
		t.Errorf("bucket JSON\n got %s\nwant %s", b, want)
	}
	e.Data, e.DataSize = "expired", 0
	b, _ = json.Marshal(e)
	if strings.Contains(string(b), "data_size") {
		t.Errorf("non-archived entry has data_size: %s", b)
	}
	empty, err := MergeIndex(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(empty)
	if string(b) != `{"entries":[]}` {
		t.Errorf("empty bucket JSON %s", b)
	}
}

// recordingStore records the options of every Put by key.
type recordingStore struct {
	store.Store
	mu   sync.Mutex
	opts map[string]store.PutOptions
}

func newRecordingStore(t *testing.T) *recordingStore {
	return &recordingStore{Store: store.NewFS(t.TempDir()), opts: map[string]store.PutOptions{}}
}

func (s *recordingStore) Put(ctx context.Context, key string, b []byte, opts ...store.PutOptions) error {
	s.mu.Lock()
	if len(opts) > 0 {
		s.opts[key] = opts[0]
	}
	s.mu.Unlock()
	return s.Store.Put(ctx, key, b, opts...)
}

func TestWriteIndexHeaders(t *testing.T) {
	ctx := context.Background()
	st := newRecordingStore(t)
	if err := WriteIndex(ctx, st, 0, []IndexEntry{entry(2, 0, 0)}, false); err != nil {
		t.Fatal(err)
	}
	key := IndexKey(0)
	if got := st.opts[key]; got != (store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}) {
		t.Errorf("open bucket options %+v", got)
	}
	if err := WriteIndex(ctx, st, 0, []IndexEntry{entry(1, 0, 0)}, true); err != nil {
		t.Fatal(err)
	}
	if got := st.opts[key]; got != (store.PutOptions{CacheControl: store.CacheImmutable, ContentType: store.TypeJSON}) {
		t.Errorf("frozen bucket options %+v", got)
	}
	raw, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var b IndexBucket
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if len(b.Entries) != 2 || b.Entries[0].Height != 1 || b.Entries[1].Height != 2 {
		t.Errorf("bucket after two writes: %+v", b.Entries)
	}

	conflict := entry(2, 0, 0)
	conflict.Data = "withdrawn"
	if err := WriteIndex(ctx, st, 0, []IndexEntry{conflict}, false); err == nil {
		t.Error("WriteIndex accepted a conflicting entry")
	}
}

func TestManifestJSONShape(t *testing.T) {
	m := Manifest{
		FibreFormat: 1, ChainID: "mocha-5", FirstHeight: 1490001, ScannedUntil: 1497200, Pending: 2,
		Counts: map[string]uint64{"archived": 10, "expired": 1}, BytesArchived: 167772160,
		DailyCapBytes: 50000000000, UpdatedAt: time.Date(2026, 10, 27, 15, 0, 0, 0, time.FixedZone("x", 3*3600)),
	}
	ctx := context.Background()
	st := newRecordingStore(t)
	if got, err := LoadManifest(ctx, st); got != nil || err != nil {
		t.Fatalf("LoadManifest of an empty store = %v, %v; want nil, nil", got, err)
	}
	if err := WriteManifest(ctx, st, m); err != nil {
		t.Fatal(err)
	}
	if got := st.opts[ManifestKey]; got != (store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}) {
		t.Errorf("manifest options %+v", got)
	}
	raw, err := st.Get(ctx, ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"fibre_format":1,"chain_id":"mocha-5","first_height":1490001,"scanned_until":1497200,"pending":2,"counts":{"archived":10,"expired":1},"bytes_archived":167772160,"daily_cap_bytes":50000000000,"updated_at":"2026-10-27T12:00:00Z"}`
	if string(raw) != want {
		t.Errorf("manifest JSON\n got %s\nwant %s", raw, want)
	}
	got, err := LoadManifest(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if got.ChainID != "mocha-5" || got.Counts["archived"] != 10 || !got.UpdatedAt.Equal(m.UpdatedAt) {
		t.Errorf("loaded manifest %+v", got)
	}

	if err := st.Put(ctx, ManifestKey, []byte("{")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(ctx, st); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Errorf("LoadManifest of bad JSON: %v", err)
	}
}

func TestManifestNilCountsIsEmptyObject(t *testing.T) {
	ctx := context.Background()
	st := newRecordingStore(t)
	if err := WriteManifest(ctx, st, Manifest{FibreFormat: 1, ChainID: "mocha-5"}); err != nil {
		t.Fatal(err)
	}
	raw, err := st.Get(ctx, ManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"counts":{}`) {
		t.Errorf("manifest %s: want counts {}", raw)
	}
}
