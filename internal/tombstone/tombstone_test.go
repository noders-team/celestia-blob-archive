package tombstone_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

const blobAt = 3

// fixture archives heights 1..5 (mocha-1834013 at height 3) in segments of 2
// heights under dir and returns the blocks, the blob records of height 3 and
// the key of the segment holding them.
func fixture(t *testing.T) (dir string, blocks map[int64]*source.Block, blobs []*archivev1.BlobRecord, segKey string) {
	t.Helper()
	blocks = archivetest.Blocks(t, 6, map[int64]string{blobAt: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[blobAt], blocks[blobAt+1])
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) < 2 {
		t.Fatalf("fixture has %d blobs, want at least 2", len(blobs))
	}
	dir = t.TempDir()
	archivetest.Build(t, store.NewFS(dir), blocks, 1, 5, 2, nil)
	return dir, blocks, blobs, archive.SegmentKey(3, 4)
}

func entryFor(b *archivev1.BlobRecord) tombstone.Entry {
	return tombstone.Entry{
		Height: int64(b.Height), Namespace: hex.EncodeToString(b.Namespace),
		Commitment: hex.EncodeToString(b.Commitment), Date: "2026-10-26", Reason: "test",
	}
}

func writeFile(t *testing.T, dir string, es ...tombstone.Entry) {
	t.Helper()
	b, err := json.Marshal(tombstone.File{Tombstones: es})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tombstone.Key), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func setOf(t *testing.T, es ...tombstone.Entry) *tombstone.Set {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, es...)
	s, err := tombstone.Load(context.Background(), store.NewFS(dir))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func readSeg(t *testing.T, dir, key string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, key))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rowOf returns the row of blob b in seg.
func rowOf(t *testing.T, seg []byte, b *archivev1.BlobRecord) segment.Row {
	t.Helper()
	rows, err := segment.ScanRows(seg)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Kind == segment.KindBlob && r.Height == b.Height &&
			bytes.Equal(r.Namespace[:], b.Namespace) && bytes.Equal(r.Commitment[:], b.Commitment) {
			return r
		}
	}
	t.Fatalf("no row for blob at height %d", b.Height)
	return segment.Row{}
}

func TestScrubKeepsEverythingElse(t *testing.T) {
	dir, _, blobs, key := fixture(t)
	seg := readSeg(t, dir, key)
	b := blobs[0]
	r := rowOf(t, seg, b)

	out, changed, err := tombstone.Scrub(seg, setOf(t, entryFor(b)))
	if err != nil || changed != 1 {
		t.Fatalf("Scrub: changed=%d err=%v", changed, err)
	}
	if len(out) != len(seg) {
		t.Fatalf("len(out) = %d, want %d", len(out), len(seg))
	}
	if &out[0] == &seg[0] {
		t.Fatal("out aliases seg")
	}
	end := r.Offset + uint64(r.Length)
	if !bytes.Equal(out[:r.Offset], seg[:r.Offset]) || !bytes.Equal(out[end:], seg[end:]) {
		t.Fatal("bytes outside the tombstoned record changed")
	}
	if bytes.Contains(out, b.Data) {
		t.Fatal("scrubbed segment still holds the blob data")
	}
	for i, o := range blobs[1:] {
		if !bytes.Contains(out, o.Data) {
			t.Fatalf("scrubbed segment lost blob %d", i+1)
		}
	}
	rec, err := segment.ReadAt(out, r.Offset, r.Length)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.Kind.(*archivev1.Record_Tombstone); !ok {
		t.Fatalf("record is %T, want a tombstone", rec.Kind)
	}
}

func TestScrubIdempotent(t *testing.T) {
	dir, _, blobs, key := fixture(t)
	seg := readSeg(t, dir, key)
	s := setOf(t, entryFor(blobs[1]))
	once, n, err := tombstone.Scrub(seg, s)
	if err != nil || n != 1 {
		t.Fatalf("first Scrub: %d %v", n, err)
	}
	twice, n, err := tombstone.Scrub(once, s)
	if err != nil || n != 0 {
		t.Fatalf("second Scrub: %d %v", n, err)
	}
	if !bytes.Equal(once, twice) {
		t.Fatal("second Scrub changed the segment")
	}
}

func TestScrubEmptySetIsCopy(t *testing.T) {
	dir, _, _, key := fixture(t)
	seg := readSeg(t, dir, key)
	out, n, err := tombstone.Scrub(seg, setOf(t))
	if err != nil || n != 0 || !bytes.Equal(out, seg) {
		t.Fatalf("Scrub with no tombstones: n=%d err=%v equal=%v", n, err, bytes.Equal(out, seg))
	}
}

// overlongBlob frames a Record holding b whose height varint carries one
// redundant byte, so the record is one byte longer than its canonical
// encoding: the one length (minimum+1) no canonical tombstone reaches.
func overlongBlob(t *testing.T, b *archivev1.BlobRecord) []byte {
	t.Helper()
	rest := proto.Clone(b).(*archivev1.BlobRecord)
	rest.Height = 0
	tail, err := proto.Marshal(rest)
	if err != nil {
		t.Fatal(err)
	}
	if b.Height >= 0x80 {
		t.Fatal("overlongBlob needs a one-byte height")
	}
	inner := protowire.AppendTag(nil, 1, protowire.VarintType)
	inner = append(inner, byte(b.Height)|0x80, 0x00)
	inner = append(inner, tail...)
	rec := protowire.AppendTag(nil, 2, protowire.BytesType)
	rec = protowire.AppendBytes(rec, inner)
	if want := proto.Size(&archivev1.Record{Kind: &archivev1.Record_Blob{Blob: b}}) + 1; len(rec) != want {
		t.Fatalf("overlong record is %d bytes, want %d", len(rec), want)
	}
	return protowire.AppendBytes(nil, rec)
}

func TestScrubContinuesPastUnscrubbableRecord(t *testing.T) {
	blob := func(c byte) *archivev1.BlobRecord {
		return &archivev1.BlobRecord{
			Height: 7, Namespace: bytes.Repeat([]byte{0xaa}, 29), Commitment: bytes.Repeat([]byte{c}, 32),
			Data: []byte("withdrawn data " + string(rune('A'+c))), ShareStart: 1, ShareEnd: 2,
		}
	}
	good, bad := blob(1), blob(2)
	// Minimal bad record: only the fields a tombstone also carries, so its
	// overlong encoding is exactly one byte above the smallest tombstone.
	bad.Data = nil
	var w segment.Writer
	if _, _, err := w.Append(&archivev1.Record{Kind: &archivev1.Record_Blob{Blob: good}}); err != nil {
		t.Fatal(err)
	}
	seg := append(slices.Clone(w.Bytes()), overlongBlob(t, bad)...)

	out, changed, err := tombstone.Scrub(seg, setOf(t, entryFor(good), entryFor(bad)))
	if changed != 1 || len(out) != len(seg) {
		t.Fatalf("Scrub: changed=%d len=%d (want 1, %d), err=%v", changed, len(out), len(seg), err)
	}
	if !errors.Is(err, segment.ErrTombstoneUnreachable) || !strings.Contains(err.Error(), hex.EncodeToString(bad.Commitment)) {
		t.Fatalf("err = %v, want ErrTombstoneUnreachable naming the bad record", err)
	}
	if bytes.Contains(out, good.Data) {
		t.Fatal("scrubbable record was not replaced")
	}
	if !bytes.Equal(out[len(w.Bytes()):], seg[len(w.Bytes()):]) {
		t.Fatal("unscrubbable record changed")
	}
}

type headers map[int64]*source.Block

func (h headers) Block(_ context.Context, height int64) (*source.Block, error) {
	b, ok := h[height]
	if !ok {
		return nil, errors.New("no block")
	}
	return b, nil
}

func TestScrubOtherProofStillVerifies(t *testing.T) {
	dir, blocks, blobs, key := fixture(t)
	seg := readSeg(t, dir, key)
	out, n, err := tombstone.Scrub(seg, setOf(t, entryFor(blobs[0])))
	if err != nil || n != 1 {
		t.Fatalf("Scrub: %d %v", n, err)
	}
	if err := os.WriteFile(filepath.Join(dir, key), out, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	st := client.New(srv.URL)
	p := func(b *archivev1.BlobRecord) verify.Params {
		return verify.Params{Height: int64(b.Height), Namespace: b.Namespace, Commitment: b.Commitment}
	}
	if err := verify.Verify(context.Background(), st, headers(blocks), p(blobs[1])); err != nil {
		t.Fatalf("other blob: %v", err)
	}
	if err := verify.Verify(context.Background(), st, headers(blocks), p(blobs[0])); !errors.Is(err, verify.ErrWithdrawn) {
		t.Fatalf("scrubbed blob: %v, want ErrWithdrawn", err)
	}
}

func TestLoadMissingIsEmpty(t *testing.T) {
	s, err := tombstone.Load(context.Background(), store.NewFS(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Heights()) != 0 || s.Version() != ([32]byte{}) || s.Has(1, make([]byte, 29), make([]byte, 32)) {
		t.Fatalf("missing file: heights=%v version=%x", s.Heights(), s.Version())
	}
}

func TestEntriesDecodedAndSorted(t *testing.T) {
	ns := strings.Repeat("0a", 29)
	dir := t.TempDir()
	writeFile(t, dir,
		tombstone.Entry{Height: 7, Namespace: ns, Commitment: strings.Repeat("02", 32)},
		tombstone.Entry{Height: 3, Namespace: strings.ToUpper(ns), Commitment: strings.Repeat("01", 32)})
	s, err := tombstone.Load(context.Background(), store.NewFS(dir))
	if err != nil {
		t.Fatal(err)
	}
	got := s.Entries()
	if len(got) != 2 || got[0].Height != 3 || got[1].Height != 7 {
		t.Fatalf("Entries = %+v, want heights 3, 7", got)
	}
	if hex.EncodeToString(got[0].Namespace[:]) != ns || got[0].Commitment != [32]byte(bytes.Repeat([]byte{1}, 32)) {
		t.Fatalf("entry 0 = %x %x", got[0].Namespace, got[0].Commitment)
	}
	for _, e := range got {
		if !s.Has(e.Height, e.Namespace[:], e.Commitment[:]) {
			t.Fatalf("Has(%+v) = false", e)
		}
	}
}

func TestLoadRejectsMalformed(t *testing.T) {
	ns := strings.Repeat("00", 29)
	c := strings.Repeat("ab", 32)
	for name, e := range map[string]tombstone.Entry{
		"bad ns hex":         {Height: 1, Namespace: "zz" + ns[2:], Commitment: c},
		"short ns":           {Height: 1, Namespace: ns[2:], Commitment: c},
		"bad commitment hex": {Height: 1, Namespace: ns, Commitment: "xyz"},
		"short commitment":   {Height: 1, Namespace: ns, Commitment: c[2:]},
		"zero height":        {Height: 0, Namespace: ns, Commitment: c},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, e)
			if _, err := tombstone.Load(context.Background(), store.NewFS(dir)); err == nil || !strings.Contains(err.Error(), "tombstone") {
				t.Fatalf("Load = %v, want a tombstone error", err)
			}
		})
	}
	t.Run("not json", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, tombstone.Key), []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := tombstone.Load(context.Background(), store.NewFS(dir)); err == nil {
			t.Fatal("Load accepted malformed JSON")
		}
	})
}

func TestHexCaseInsensitive(t *testing.T) {
	ns := bytes.Repeat([]byte{0xab}, 29)
	c := bytes.Repeat([]byte{0xcd}, 32)
	s := setOf(t, tombstone.Entry{
		Height:     7,
		Namespace:  strings.ToUpper(hex.EncodeToString(ns)),
		Commitment: strings.ToUpper(hex.EncodeToString(c)),
	})
	if !s.Has(7, ns, c) {
		t.Fatal("upper-case hex entry not matched")
	}
	if s.Has(8, ns, c) || s.Has(7, ns, bytes.Repeat([]byte{0xce}, 32)) {
		t.Fatal("Has matched a different blob")
	}
	if h := s.Heights(); len(h) != 1 || h[0] != 7 {
		t.Fatalf("Heights = %v", h)
	}
	if s.Version() == ([32]byte{}) {
		t.Fatal("non-empty set has zero version")
	}
}

func TestAddRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	ns := strings.Repeat("Ab", 29)
	c := strings.Repeat("cD", 32)
	e := tombstone.Entry{Height: 5, Namespace: ns, Commitment: c, Date: "2026-10-26", Reason: "dmca"}
	if err := tombstone.Add(ctx, st, e); err != nil {
		t.Fatal(err)
	}
	// The same blob with different hex case and reason is still a duplicate.
	dup := e
	dup.Namespace, dup.Commitment, dup.Reason = strings.ToLower(ns), strings.ToUpper(c), "again"
	if err := tombstone.Add(ctx, st, dup); !errors.Is(err, tombstone.ErrDuplicate) {
		t.Fatalf("Add duplicate = %v, want ErrDuplicate", err)
	}
	other := e
	other.Height = 6
	if err := tombstone.Add(ctx, st, other); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]tombstone.Entry{
		"zero height":  {Height: 0, Namespace: ns, Commitment: c, Date: "2026-10-26", Reason: "x"},
		"no date":      {Height: 9, Namespace: ns, Commitment: c, Reason: "x"},
		"bad date":     {Height: 9, Namespace: ns, Commitment: c, Date: "26.10.2026", Reason: "x"},
		"no reason":    {Height: 9, Namespace: ns, Commitment: c, Date: "2026-10-26"},
		"blank reason": {Height: 9, Namespace: ns, Commitment: c, Date: "2026-10-26", Reason: "  "},
	} {
		if err := tombstone.Add(ctx, st, bad); err == nil || errors.Is(err, tombstone.ErrDuplicate) {
			t.Fatalf("%s: Add = %v, want a validation error", name, err)
		}
	}

	raw, err := st.Get(ctx, tombstone.Key)
	if err != nil {
		t.Fatal(err)
	}
	var f tombstone.File
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Tombstones) != 2 || f.Tombstones[0].Namespace != strings.ToLower(ns) || f.Tombstones[0].Commitment != strings.ToLower(c) {
		t.Fatalf("file: %+v", f)
	}
	s, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if h := s.Heights(); len(h) != 2 || h[0] != 5 || h[1] != 6 {
		t.Fatalf("Heights = %v", h)
	}
}

// putRecorder records the options of every Put.
type putRecorder struct {
	store.Store
	opts []store.PutOptions
}

func (p *putRecorder) Put(ctx context.Context, key string, body []byte, opts ...store.PutOptions) error {
	p.opts = append(p.opts, opts...)
	return p.Store.Put(ctx, key, body, opts...)
}

func TestAddWritesNoCacheJSON(t *testing.T) {
	st := &putRecorder{Store: store.NewFS(t.TempDir())}
	e := tombstone.Entry{Height: 5, Namespace: strings.Repeat("ab", 29), Commitment: strings.Repeat("cd", 32), Date: "2026-10-26", Reason: "dmca"}
	if err := tombstone.Add(context.Background(), st, e); err != nil {
		t.Fatal(err)
	}
	want := store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}
	if len(st.opts) != 1 || st.opts[0] != want {
		t.Fatalf("Put options = %+v, want [%+v]", st.opts, want)
	}
}

// fibreID returns a 33-byte blob ID filled with b, as lowercase hex.
func fibreID(b byte) string { return hex.EncodeToString(bytes.Repeat([]byte{b}, 33)) }

// mixedFile is a tombstones.json with blob entries, Fibre entries (one in
// upper-case hex) and an unknown top-level key "x".
func mixedFile() string {
	ns := strings.Repeat("00", 28) + "01"
	return `{"tombstones":[` +
		`{"height":5,"namespace":"` + ns + `","commitment":"` + strings.Repeat("ab", 32) + `","date":"2026-10-26","reason":"dmca"},` +
		`{"height":6,"namespace":"` + ns + `","commitment":"` + strings.Repeat("cd", 32) + `","date":"2026-10-26","reason":"court"}],` +
		`"fibre":[{"blob_id":"` + fibreID(0x11) + `","height":1497140,"namespace":"` + ns + `","date":"2026-10-27","reason":"dmca"},` +
		`{"blob_id":"` + strings.ToUpper(fibreID(0x22)) + `","date":"2026-10-27","reason":"court"}],` +
		`"x":{"note":[1,2,3]}}`
}

func writeRaw(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, tombstone.Key), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestOldReaderParsesFileWithFibreEntries(t *testing.T) {
	// Copies of Entry, File and parse as they were before the fibre array.
	type oldEntry struct {
		Height     int64  `json:"height"`
		Namespace  string `json:"namespace"`
		Commitment string `json:"commitment"`
		Date       string `json:"date"`
		Reason     string `json:"reason"`
	}
	type oldFile struct {
		Tombstones []oldEntry `json:"tombstones"`
	}
	oldParse := func(e oldEntry) error {
		ns, err := hex.DecodeString(e.Namespace)
		if err != nil || len(ns) != 29 {
			return errors.New("bad namespace")
		}
		c, err := hex.DecodeString(e.Commitment)
		if err != nil || len(c) != 32 {
			return errors.New("bad commitment")
		}
		if e.Height < 1 {
			return errors.New("bad height")
		}
		return nil
	}
	var f oldFile
	if err := json.Unmarshal([]byte(mixedFile()), &f); err != nil {
		t.Fatal(err)
	}
	want := []oldEntry{
		{5, strings.Repeat("00", 28) + "01", strings.Repeat("ab", 32), "2026-10-26", "dmca"},
		{6, strings.Repeat("00", 28) + "01", strings.Repeat("cd", 32), "2026-10-26", "court"},
	}
	if !reflect.DeepEqual(f.Tombstones, want) {
		t.Fatalf("old reader got %+v", f)
	}
	for i, e := range f.Tombstones {
		if err := oldParse(e); err != nil {
			t.Fatalf("old parse rejects entry %d: %v", i, err)
		}
	}
}

// topLevel decodes the top-level keys of tombstones.json in st.
func topLevel(t *testing.T, st store.Store) map[string]json.RawMessage {
	t.Helper()
	raw, err := st.Get(context.Background(), tombstone.Key)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// decodeAny decodes raw JSON into generic values for comparison.
func decodeAny(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

func TestAddPreservesUnknownAndFibreEntries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeRaw(t, dir, mixedFile())
	st := store.NewFS(dir)
	orig := topLevel(t, st)
	origBlobs := decodeAny(t, orig["tombstones"]).([]any)
	origFibre := decodeAny(t, orig["fibre"]).([]any)
	origX := decodeAny(t, orig["x"])

	check := func(step string, blobs, fibre int) {
		t.Helper()
		m := topLevel(t, st)
		if len(m) != 3 {
			t.Fatalf("%s: top-level keys %v", step, slices.Sorted(maps.Keys(m)))
		}
		gotBlobs := decodeAny(t, m["tombstones"]).([]any)
		gotFibre := decodeAny(t, m["fibre"]).([]any)
		if len(gotBlobs) != blobs || len(gotFibre) != fibre {
			t.Fatalf("%s: %d blob and %d fibre entries, want %d and %d", step, len(gotBlobs), len(gotFibre), blobs, fibre)
		}
		if !reflect.DeepEqual(gotBlobs[:2], origBlobs) || !reflect.DeepEqual(gotFibre[:2], origFibre) {
			t.Fatalf("%s: existing entries changed:\n%v\n%v", step, gotBlobs, gotFibre)
		}
		if x := decodeAny(t, m["x"]); !reflect.DeepEqual(x, origX) {
			t.Fatalf("%s: unknown key x = %s, want %s", step, m["x"], orig["x"])
		}
	}

	e := tombstone.Entry{Height: 9, Namespace: strings.Repeat("00", 28) + "02", Commitment: strings.Repeat("ef", 32), Date: "2026-10-27", Reason: "dmca"}
	if err := tombstone.Add(ctx, st, e); err != nil {
		t.Fatal(err)
	}
	check("after Add", 3, 2)
	if err := tombstone.AddFibre(ctx, st, tombstone.FibreEntry{BlobID: fibreID(0x33), Date: "2026-10-27", Reason: "court"}); err != nil {
		t.Fatal(err)
	}
	check("after AddFibre", 3, 3)

	// Key order: tombstones, fibre, then the unknown keys sorted.
	raw, err := st.Get(ctx, tombstone.Key)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if i, j, k := strings.Index(s, `"tombstones"`), strings.Index(s, `"fibre"`), strings.Index(s, `"x"`); i < 0 || i > j || j > k {
		t.Fatalf("key order wrong:\n%s", s)
	}
	set, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 3 || set.FibreLen() != 3 {
		t.Fatalf("Len %d FibreLen %d, want 3 and 3", set.Len(), set.FibreLen())
	}
}

func TestLoadRejectsMalformedFibreEntry(t *testing.T) {
	for name, id := range map[string]string{
		"32 bytes": strings.Repeat("ab", 32),
		"non-hex":  "zz" + fibreID(0x11)[2:],
		"empty":    "",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeRaw(t, dir, `{"tombstones":[],"fibre":[{"blob_id":"`+id+`","date":"2026-10-27","reason":"x"}]}`)
			if _, err := tombstone.Load(context.Background(), store.NewFS(dir)); err == nil || !strings.Contains(err.Error(), "fibre tombstone") {
				t.Fatalf("Load = %v, want a fibre tombstone error", err)
			}
		})
	}
}

func TestLoadRejectsCaseVariantKey(t *testing.T) {
	// An old Go reader would read "Tombstones" as entries; this reader must not
	// keep it as an unknown key and silently un-withdraw them.
	for _, k := range []string{"Tombstones", "FIBRE"} {
		dir := t.TempDir()
		writeRaw(t, dir, `{"`+k+`":[]}`)
		if _, err := tombstone.Load(context.Background(), store.NewFS(dir)); err == nil {
			t.Fatalf("Load accepted top-level key %q", k)
		}
	}
}

func TestHasFibreCaseInsensitive(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, mixedFile())
	s, err := tombstone.Load(context.Background(), store.NewFS(dir))
	if err != nil {
		t.Fatal(err)
	}
	// 0x11 is listed in lower case, 0x22 in upper case.
	for _, b := range []byte{0x11, 0x22} {
		if !s.HasFibre(bytes.Repeat([]byte{b}, 33)) {
			t.Fatalf("HasFibre(%#x...) = false", b)
		}
	}
	if s.HasFibre(bytes.Repeat([]byte{0x33}, 33)) || s.HasFibre(bytes.Repeat([]byte{0x11}, 32)) || s.HasFibre(nil) {
		t.Fatal("HasFibre matched an unlisted ID")
	}
	ids := s.FibreIDs()
	if len(ids) != 2 || ids[0] != [33]byte(bytes.Repeat([]byte{0x11}, 33)) || ids[1] != [33]byte(bytes.Repeat([]byte{0x22}, 33)) {
		t.Fatalf("FibreIDs = %x", ids)
	}
	if s.FibreLen() != 2 || s.Len() != 2 {
		t.Fatalf("FibreLen %d Len %d, want 2 and 2", s.FibreLen(), s.Len())
	}
}

func TestAddFibreDuplicate(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	e := tombstone.FibreEntry{BlobID: strings.ToUpper(fibreID(0xab)), Height: 7, Namespace: strings.Repeat("0A", 29), Date: "2026-10-27", Reason: "dmca"}
	if err := tombstone.AddFibre(ctx, st, e); err != nil {
		t.Fatal(err)
	}
	// The same ID in other hex case, at another height, is still a duplicate.
	dup := tombstone.FibreEntry{BlobID: fibreID(0xab), Height: 8, Date: "2026-10-28", Reason: "again"}
	if err := tombstone.AddFibre(ctx, st, dup); !errors.Is(err, tombstone.ErrDuplicate) {
		t.Fatalf("AddFibre duplicate = %v, want ErrDuplicate", err)
	}
	raw, err := st.Get(ctx, tombstone.Key)
	if err != nil {
		t.Fatal(err)
	}
	var f tombstone.File
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Fibre) != 1 || f.Fibre[0].BlobID != fibreID(0xab) || f.Fibre[0].Namespace != strings.Repeat("0a", 29) || f.Fibre[0].Height != 7 {
		t.Fatalf("file: %+v", f.Fibre)
	}
	// A file holding only Fibre entries still has a tombstones array.
	if m := topLevel(t, st); string(m["tombstones"]) != "[]" {
		t.Fatalf("tombstones = %s, want []", m["tombstones"])
	}
}

func TestAddFibreValidatesDateReason(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	id := fibreID(0x01)
	for name, bad := range map[string]tombstone.FibreEntry{
		"bad id":       {BlobID: id[2:], Date: "2026-10-27", Reason: "x"},
		"no date":      {BlobID: id, Reason: "x"},
		"bad date":     {BlobID: id, Date: "27.10.2026", Reason: "x"},
		"no reason":    {BlobID: id, Date: "2026-10-27"},
		"blank reason": {BlobID: id, Date: "2026-10-27", Reason: " \t"},
		"bad ns":       {BlobID: id, Namespace: "abcd", Date: "2026-10-27", Reason: "x"},
		"neg height":   {BlobID: id, Height: -1, Date: "2026-10-27", Reason: "x"},
	} {
		if err := tombstone.AddFibre(ctx, st, bad); err == nil || errors.Is(err, tombstone.ErrDuplicate) {
			t.Fatalf("%s: AddFibre = %v, want a validation error", name, err)
		}
	}
	if _, err := st.Get(ctx, tombstone.Key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a rejected AddFibre wrote the file: %v", err)
	}
}

func TestVersionChangesOnFibreAdd(t *testing.T) {
	ctx := context.Background()
	st := store.NewFS(t.TempDir())
	e := tombstone.Entry{Height: 5, Namespace: strings.Repeat("ab", 29), Commitment: strings.Repeat("cd", 32), Date: "2026-10-27", Reason: "x"}
	if err := tombstone.Add(ctx, st, e); err != nil {
		t.Fatal(err)
	}
	before, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if err := tombstone.AddFibre(ctx, st, tombstone.FibreEntry{BlobID: fibreID(0x01), Date: "2026-10-27", Reason: "x"}); err != nil {
		t.Fatal(err)
	}
	after, err := tombstone.Load(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if before.Version() == after.Version() {
		t.Fatal("Version unchanged after AddFibre")
	}
	if after.Len() != before.Len() {
		t.Fatalf("Len changed from %d to %d by a Fibre entry", before.Len(), after.Len())
	}
}
