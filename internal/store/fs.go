package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type fsStore struct{ root string }

// NewFS returns a Store backed by a directory. Keys are slash-separated
// relative paths.
func NewFS(root string) Store { return &fsStore{root: root} }

func (s *fsStore) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsRune(key, '\\') || strings.ContainsRune(key, 0) {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return "", fmt.Errorf("store: invalid key %q: path traversal", key)
		}
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

// writeTemp writes body to a fresh temp file next to dst and returns its name.
func writeTemp(dst string, body []byte) (string, error) {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// Put ignores opts: a directory has no object headers.
func (s *fsStore) Put(_ context.Context, key string, body []byte, _ ...PutOptions) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	tmp, err := writeTemp(p, body)
	if err != nil {
		return fmt.Errorf("store: put %q: %w", key, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("store: put %q: %w", key, err)
	}
	return nil
}

// PutIfAbsent publishes the object with a hard link from a fully written temp
// file; link fails with EEXIST if the key is taken, so concurrent callers
// never observe partial content and exactly one differing writer wins.
func (s *fsStore) PutIfAbsent(ctx context.Context, key string, body []byte, _ ...PutOptions) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	tmp, err := writeTemp(p, body)
	if err != nil {
		return fmt.Errorf("store: put %q: %w", key, err)
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, p); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("store: put %q: %w", key, err)
		}
		existing, gerr := s.Get(ctx, key)
		if gerr != nil {
			return gerr
		}
		if bytes.Equal(existing, body) {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrExists, key)
	}
	return nil
}

func (s *fsStore) Get(_ context.Context, key string) ([]byte, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("store: get %q: %w", key, err)
	}
	return b, nil
}

func (s *fsStore) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n <= 0 {
		return nil, fmt.Errorf("%w: invalid range off=%d n=%d", ErrOutOfRange, off, n)
	}
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("store: get %q: %w", key, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("store: stat %q: %w", key, err)
	}
	if off > fi.Size() || n > fi.Size()-off {
		return nil, fmt.Errorf("%w: %q [%d,+%d) of %d bytes", ErrOutOfRange, key, off, n, fi.Size())
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(f, off, n), buf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			err = fmt.Errorf("%w: %w", ErrOutOfRange, err)
		}
		return nil, fmt.Errorf("store: get range %q [%d,+%d): %w", key, off, n, err)
	}
	return buf, nil
}

// List walks the directory of prefix's last "/" and keeps the keys that start
// with prefix. Temp files of unfinished writes are skipped. A missing start
// directory is an empty list; any error during the walk, including an entry
// that vanished, is returned rather than skipped, so a caller never acts on a
// listing that silently lacks keys (a takedown retries on its next poll).
func (s *fsStore) List(_ context.Context, prefix string) ([]string, error) {
	dir := ""
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		dir = prefix[:i]
	}
	start := s.root
	if dir != "" {
		p, err := s.path(dir)
		if err != nil {
			return nil, err
		}
		start = p
	}
	if _, err := os.Stat(start); errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	var keys []string
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		if k := filepath.ToSlash(rel); strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: list %q: %w", prefix, err)
	}
	slices.Sort(keys)
	return keys, nil
}

func (s *fsStore) Exists(_ context.Context, key string) (bool, error) {
	p, err := s.path(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: exists %q: %w", key, err)
	}
	return true, nil
}

func (s *fsStore) Delete(_ context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("store: delete %q: %w", key, err)
	}
	return nil
}
