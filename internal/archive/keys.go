// Package archive builds immutable segments and maintains the catalog and
// manifest that point at them.
package archive

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// ManifestKey is the small mutable pointer object.
	ManifestKey = "manifest.json"
	// FormatVersion is the value of Manifest.FormatVersion.
	FormatVersion = 2
	// ChunkSize is the number of segment refs per catalog chunk.
	ChunkSize = 1000
)

// SegmentKey is the object key of the segment covering [first, last].
func SegmentKey(first, last int64) string {
	return fmt.Sprintf("segments/%012d-%012d.seg", first, last)
}

// ParseSegmentKey returns the range of a key written by SegmentKey; ok is
// false for any other key.
func ParseSegmentKey(key string) (first, last int64, ok bool) {
	name, ok := strings.CutPrefix(key, "segments/")
	if !ok {
		return 0, 0, false
	}
	name, ok = strings.CutSuffix(name, ".seg")
	if !ok {
		return 0, 0, false
	}
	a, b, ok := strings.Cut(name, "-")
	if !ok {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || SegmentKey(first, last) != key {
		return 0, 0, false
	}
	return first, last, true
}

// IndexKey is the object key of the row index of the segment covering
// [first, last].
func IndexKey(first, last int64) string {
	return fmt.Sprintf("segments/%012d-%012d.idx2", first, last)
}

// DirKey is the object key of height directory bucket b (see DirSlot).
func DirKey(bucket int64) string { return fmt.Sprintf("dir/%08d.dir", bucket) }

// CatalogKey is the object key of catalog chunk n.
func CatalogKey(n int) string { return fmt.Sprintf("catalog/%08d.json", n) }
