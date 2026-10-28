// Package fibrearchive defines the fibre/ key space of the archive: object
// keys, FibreRecord encoding, index buckets and the Fibre manifest (see
// docs/format.md, Fibre). fibre/ is outside format_version 2.
package fibrearchive

import (
	"encoding/hex"
	"fmt"
)

// Key prefixes and the manifest key of the fibre/ space.
const (
	DataPrefix   = "fibre/data/"
	RecordPrefix = "fibre/records/"
	IndexPrefix  = "fibre/index/"
	ManifestKey  = "fibre/manifest.json"
)

// BucketSize is the number of heights in one index bucket.
const BucketSize = 10000

// BlobIDSize is the length of a Fibre blob ID: version byte ‖ 32-byte
// commitment.
const BlobIDSize = 33

// DataKey returns the key of the payload of blob id:
// "fibre/data/<66 lowercase hex>".
func DataKey(id [BlobIDSize]byte) string {
	return DataPrefix + hex.EncodeToString(id[:])
}

// RecordKey returns the key of the record of message msgIndex of tx txIndex
// at height: "fibre/records/<height:012d>-<tx_index:05d>-<msg_index:03d>.pb".
func RecordKey(height int64, txIndex, msgIndex uint32) string {
	return fmt.Sprintf("%s%012d-%05d-%03d.pb", RecordPrefix, height, txIndex, msgIndex)
}

// IndexKey returns the key of index bucket b: "fibre/index/<b:08d>.json".
func IndexKey(bucket int64) string {
	return fmt.Sprintf("%s%08d.json", IndexPrefix, bucket)
}

// Bucket returns the index bucket holding height h (h >= 1): bucket b holds
// heights b*10000+1 .. (b+1)*10000.
func Bucket(height int64) int64 {
	return (height - 1) / BucketSize
}
