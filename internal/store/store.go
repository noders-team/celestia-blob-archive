// Package store abstracts the object storage the archive is written to.
package store

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrExists means the key already exists with different bytes.
	ErrExists = errors.New("store: key exists with different content")
	// ErrNotFound means the key does not exist.
	ErrNotFound = errors.New("store: key not found")
	// ErrOutOfRange means a range read asked for bytes outside the object
	// (the object is shorter than off+n, or the range is invalid).
	ErrOutOfRange = errors.New("store: range out of bounds")
)

// Object header values for PutOptions.
const (
	// CacheImmutable is for objects that are never written again.
	CacheImmutable = "public, max-age=31536000, immutable"
	// CacheSegment is for segments: written once, but a takedown may rewrite
	// one in place, so caches must not keep it for long.
	CacheSegment = "public, max-age=86400"
	// CacheOP is for OP objects: a takedown deletes one, so a downstream
	// cache must drop it within minutes (the edge never caches op/).
	CacheOP = "public, max-age=300"
	// CacheNoCache is for objects that are rewritten in place.
	CacheNoCache = "no-cache"
	TypeJSON     = "application/json"
	TypeBinary   = "application/octet-stream"
)

// PutOptions are optional HTTP headers stored with an object. Backends that
// do not serve objects over HTTP ignore them.
type PutOptions struct {
	CacheControl string
	ContentType  string
}

// Store is a minimal object store.
type Store interface {
	// PutIfAbsent writes body under key. If the key already holds identical
	// bytes it returns nil; if it holds different bytes it returns ErrExists.
	// At most one PutOptions is used.
	PutIfAbsent(ctx context.Context, key string, body []byte, opts ...PutOptions) error
	// Put writes body under key, overwriting any existing object.
	Put(ctx context.Context, key string, body []byte, opts ...PutOptions) error
	// Get returns the object's bytes or ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange returns n bytes starting at off, ErrNotFound, or
	// ErrOutOfRange if the object does not hold all of [off, off+n).
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
	// Exists reports whether the key exists.
	Exists(ctx context.Context, key string) (bool, error)
	// List returns every key that starts with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes the key. Deleting a missing key returns nil.
	Delete(ctx context.Context, key string) error
}

// ErrTooLarge means an object is larger than the caller's limit.
var ErrTooLarge = errors.New("store: object too large")

// LimitGetter is a Store that can refuse an object over a size limit
// without reading it all into memory.
type LimitGetter interface {
	// GetLimit is Get, failing with ErrTooLarge if the object is larger
	// than max bytes.
	GetLimit(ctx context.Context, key string, max int64) ([]byte, error)
}

// GetLimit reads key from st, failing with ErrTooLarge if it is larger than
// max bytes. A Store that is not a LimitGetter is read whole first.
func GetLimit(ctx context.Context, st Store, key string, max int64) ([]byte, error) {
	if lg, ok := st.(LimitGetter); ok {
		return lg.GetLimit(ctx, key, max)
	}
	b, err := st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("%w: %s is %d bytes, limit %d", ErrTooLarge, key, len(b), max)
	}
	return b, nil
}

func firstOpts(opts []PutOptions) PutOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	return PutOptions{}
}
