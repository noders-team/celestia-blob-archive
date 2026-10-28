// Package tombstone reads and writes tombstones.json, the public list of
// blobs withdrawn from the archive, and scrubs withdrawn blob records out of
// segments in place. See docs/format.md.
package tombstone

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// Key is the object key of the tombstone file.
const Key = "tombstones.json"

// ErrDuplicate means Add or AddFibre was given a blob that is already
// tombstoned.
var ErrDuplicate = errors.New("tombstone: blob is already tombstoned")

// Entry withdraws one blob from the archive.
type Entry struct {
	Height     int64  `json:"height"`
	Namespace  string `json:"namespace"`  // hex, 29 bytes
	Commitment string `json:"commitment"` // hex, 32 bytes
	Date       string `json:"date"`
	Reason     string `json:"reason"`
}

// FibreEntry withdraws one Fibre blob ID wherever it appears. Height and
// Namespace are informational: one tx carrying the ID.
type FibreEntry struct {
	BlobID    string `json:"blob_id"` // hex, 33 bytes
	Height    int64  `json:"height,omitempty"`
	Namespace string `json:"namespace,omitempty"` // hex, 29 bytes
	Date      string `json:"date"`
	Reason    string `json:"reason"`
}

// File is the content of tombstones.json. Unknown top-level keys survive a
// decode and encode unchanged, so a writer never drops what a newer writer
// added.
type File struct {
	Tombstones []Entry      `json:"tombstones"`
	Fibre      []FibreEntry `json:"fibre,omitempty"`
	extra      map[string]json.RawMessage
}

// UnmarshalJSON decodes the known arrays and keeps every other top-level key.
// A key that differs from "tombstones" or "fibre" only in case is rejected:
// encoding/json matches struct keys case-insensitively, so readers before the
// fibre array would take it for entries that this reader would keep as unknown.
func (f *File) UnmarshalJSON(b []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	*f = File{}
	for k, v := range m {
		var err error
		switch {
		case k == "tombstones":
			err = json.Unmarshal(v, &f.Tombstones)
		case k == "fibre":
			err = json.Unmarshal(v, &f.Fibre)
		case strings.EqualFold(k, "tombstones") || strings.EqualFold(k, "fibre"):
			err = errors.New("key must be lowercase")
		default:
			if f.extra == nil {
				f.extra = map[string]json.RawMessage{}
			}
			f.extra[k] = v
		}
		if err != nil {
			return fmt.Errorf("key %q: %w", k, err)
		}
	}
	return nil
}

// MarshalJSON writes "tombstones" (an empty array rather than null), "fibre"
// when non-empty, then the unknown keys sorted.
func (f File) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	field := func(k string, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("key %q: %w", k, err)
		}
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(b)
		return nil
	}
	ts := f.Tombstones
	if ts == nil {
		ts = []Entry{}
	}
	if err := field("tombstones", ts); err != nil {
		return nil, err
	}
	if len(f.Fibre) > 0 {
		if err := field("fibre", f.Fibre); err != nil {
			return nil, err
		}
	}
	for _, k := range slices.Sorted(maps.Keys(f.extra)) {
		if err := field(k, f.extra[k]); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// key identifies a tombstoned blob by decoded bytes.
type key struct {
	height     int64
	namespace  string // raw 29 bytes
	commitment string // raw 32 bytes
}

// parse validates e and returns its key. Hex is case-insensitive.
func parse(e Entry) (key, error) {
	ns, err := hex.DecodeString(e.Namespace)
	if err != nil || len(ns) != 29 {
		return key{}, fmt.Errorf("namespace must be 29 bytes of hex, got %q", e.Namespace)
	}
	c, err := hex.DecodeString(e.Commitment)
	if err != nil || len(c) != 32 {
		return key{}, fmt.Errorf("commitment must be 32 bytes of hex, got %q", e.Commitment)
	}
	if e.Height < 1 {
		return key{}, fmt.Errorf("height must be positive, got %d", e.Height)
	}
	return key{e.Height, string(ns), string(c)}, nil
}

// blobID is a decoded Fibre blob ID.
type blobID = [fibrearchive.BlobIDSize]byte

// parseFibre validates the blob ID of e and returns it decoded. Hex is
// case-insensitive.
func parseFibre(e FibreEntry) (blobID, error) {
	id, err := hex.DecodeString(e.BlobID)
	if err != nil || len(id) != fibrearchive.BlobIDSize {
		return blobID{}, fmt.Errorf("blob_id must be %d bytes of hex, got %q", fibrearchive.BlobIDSize, e.BlobID)
	}
	return blobID(id), nil
}

// Set is a loaded, validated tombstone file. It is read-only after Load.
type Set struct {
	keys    map[key]bool
	fibre   map[blobID]bool
	version [32]byte
}

// Load reads tombstones.json from st. A missing file is an empty set; a
// malformed entry fails the whole load, so a typo can never silently
// un-withdraw a blob.
func Load(ctx context.Context, st store.Store) (*Set, error) {
	_, s, err := load(ctx, st)
	return s, err
}

func load(ctx context.Context, st store.Store) (*File, *Set, error) {
	s := &Set{keys: map[key]bool{}, fibre: map[blobID]bool{}}
	b, err := st.Get(ctx, Key)
	if errors.Is(err, store.ErrNotFound) {
		return &File{}, s, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read tombstones: %w", err)
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, nil, fmt.Errorf("decode tombstones: %w", err)
	}
	for i, e := range f.Tombstones {
		k, err := parse(e)
		if err != nil {
			return nil, nil, fmt.Errorf("tombstone %d: %w", i, err)
		}
		s.keys[k] = true
	}
	for i, e := range f.Fibre {
		id, err := parseFibre(e)
		if err != nil {
			return nil, nil, fmt.Errorf("fibre tombstone %d: %w", i, err)
		}
		s.fibre[id] = true
	}
	s.version = sha256.Sum256(b)
	return &f, s, nil
}

// Has reports whether the blob (height, ns, commitment) is tombstoned.
func (s *Set) Has(height int64, ns, commitment []byte) bool {
	return s.keys[key{height, string(ns), string(commitment)}]
}

// Heights returns the distinct tombstoned heights in ascending order.
func (s *Set) Heights() []int64 {
	var hs []int64
	for k := range s.keys {
		hs = append(hs, k.height)
	}
	slices.Sort(hs)
	return slices.Compact(hs)
}

// Withdrawn identifies one tombstoned blob by decoded bytes.
type Withdrawn struct {
	Height     int64
	Namespace  [29]byte
	Commitment [32]byte
}

// Entries returns the tombstoned blobs, sorted by height, then namespace,
// then commitment.
func (s *Set) Entries() []Withdrawn {
	out := make([]Withdrawn, 0, len(s.keys))
	for k := range s.keys {
		// parse checked the lengths.
		out = append(out, Withdrawn{k.height, [29]byte([]byte(k.namespace)), [32]byte([]byte(k.commitment))})
	}
	slices.SortFunc(out, func(a, b Withdrawn) int {
		return cmp.Or(cmp.Compare(a.Height, b.Height),
			bytes.Compare(a.Namespace[:], b.Namespace[:]), bytes.Compare(a.Commitment[:], b.Commitment[:]))
	})
	return out
}

// Len returns the number of tombstoned blobs, not counting Fibre entries.
func (s *Set) Len() int { return len(s.keys) }

// HasFibre reports whether the Fibre blob ID id is tombstoned.
func (s *Set) HasFibre(id []byte) bool {
	return len(id) == fibrearchive.BlobIDSize && s.fibre[blobID(id)]
}

// FibreIDs returns the tombstoned Fibre blob IDs in ascending order.
func (s *Set) FibreIDs() [][fibrearchive.BlobIDSize]byte {
	out := slices.Collect(maps.Keys(s.fibre))
	slices.SortFunc(out, func(a, b blobID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

// FibreLen returns the number of tombstoned Fibre blob IDs.
func (s *Set) FibreLen() int { return len(s.fibre) }

// Version is the sha256 of the loaded file bytes, or zero for a missing file.
func (s *Set) Version() [32]byte { return s.version }

// Add validates e and appends it to tombstones.json with lowercase hex. Unlike
// Load, it also requires a YYYY-MM-DD date and a non-empty reason. It returns
// ErrDuplicate if the blob is already listed. Fibre entries and unknown keys
// are kept. The read-modify-write is not atomic: only one operator command may
// write the file at a time.
func Add(ctx context.Context, st store.Store, e Entry) error {
	k, err := parse(e)
	if err != nil {
		return fmt.Errorf("tombstone: %w", err)
	}
	if err := checkDateReason(e.Date, e.Reason); err != nil {
		return err
	}
	f, s, err := load(ctx, st)
	if err != nil {
		return err
	}
	if s.keys[k] {
		return fmt.Errorf("%w: height %d commitment %s", ErrDuplicate, e.Height, strings.ToLower(e.Commitment))
	}
	e.Namespace = strings.ToLower(e.Namespace)
	e.Commitment = strings.ToLower(e.Commitment)
	f.Tombstones = append(f.Tombstones, e)
	return save(ctx, st, f)
}

// AddFibre validates e and appends it to the fibre array of tombstones.json
// with lowercase hex, under the same rules as Add; a non-empty Namespace must
// also be 29 bytes of hex and Height must not be negative. It returns
// ErrDuplicate if the blob ID is already listed. Blob entries and unknown keys
// are kept.
func AddFibre(ctx context.Context, st store.Store, e FibreEntry) error {
	id, err := parseFibre(e)
	if err != nil {
		return fmt.Errorf("fibre tombstone: %w", err)
	}
	if e.Namespace != "" {
		if ns, err := hex.DecodeString(e.Namespace); err != nil || len(ns) != 29 {
			return fmt.Errorf("fibre tombstone: namespace must be 29 bytes of hex, got %q", e.Namespace)
		}
	}
	if e.Height < 0 {
		return fmt.Errorf("fibre tombstone: height must not be negative, got %d", e.Height)
	}
	if err := checkDateReason(e.Date, e.Reason); err != nil {
		return err
	}
	f, s, err := load(ctx, st)
	if err != nil {
		return err
	}
	if s.fibre[id] {
		return fmt.Errorf("%w: fibre blob %x", ErrDuplicate, id)
	}
	e.BlobID = hex.EncodeToString(id[:])
	e.Namespace = strings.ToLower(e.Namespace)
	f.Fibre = append(f.Fibre, e)
	return save(ctx, st, f)
}

// checkDateReason requires a YYYY-MM-DD date and a non-empty reason.
func checkDateReason(date, reason string) error {
	if _, err := time.Parse(time.DateOnly, date); err != nil {
		return fmt.Errorf("tombstone: date must be YYYY-MM-DD, got %q", date)
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("tombstone: reason is required")
	}
	return nil
}

// save writes f to tombstones.json, indented and uncached.
func save(ctx context.Context, st store.Store, f *File) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("encode tombstones: %w", err)
	}
	b = append(b, '\n')
	if err := st.Put(ctx, Key, b, store.PutOptions{CacheControl: store.CacheNoCache, ContentType: store.TypeJSON}); err != nil {
		return fmt.Errorf("write tombstones: %w", err)
	}
	return nil
}

// Scrub returns a copy of seg in which every blob record listed in s is
// overwritten by a tombstone record of the same length (segment.TombstoneFor),
// and how many records it overwrote. Offsets and every other byte stay
// unchanged, so the segment's .idx2 and the height directory remain valid.
// Records that already are tombstones are left alone, so Scrub is idempotent.
//
// A listed record that cannot be replaced does not stop the others: Scrub
// still returns out and changed, with err joining one error per failed record
// (each naming its height, namespace and commitment). Callers write out when
// changed > 0 and log and count err. Only a segment that cannot be parsed at
// all gives a nil out.
func Scrub(seg []byte, s *Set) (out []byte, changed int, err error) {
	rows, err := segment.ScanRows(seg)
	if err != nil {
		return nil, 0, fmt.Errorf("scrub: %w", err)
	}
	out = slices.Clone(seg)
	var errs []error
	for _, r := range rows {
		if r.Kind != segment.KindBlob || !s.Has(int64(r.Height), r.Namespace[:], r.Commitment[:]) {
			continue
		}
		t, err := tombstoneAt(seg, r)
		if err != nil {
			errs = append(errs, fmt.Errorf("scrub: height %d namespace %x commitment %x: %w", r.Height, r.Namespace, r.Commitment, err))
			continue
		}
		if t == nil {
			continue // already a tombstone
		}
		copy(out[r.Offset:], t)
		changed++
	}
	return out, changed, errors.Join(errs...)
}

// tombstoneAt returns the tombstone bytes replacing the blob record of row r,
// or nil if the record already is a tombstone.
func tombstoneAt(seg []byte, r segment.Row) ([]byte, error) {
	rec, err := segment.ReadAt(seg, r.Offset, r.Length)
	if err != nil {
		return nil, err
	}
	b := rec.GetBlob()
	if b == nil {
		return nil, nil
	}
	return segment.TombstoneFor(b, r.Length)
}
