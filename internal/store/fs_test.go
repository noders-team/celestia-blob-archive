package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func TestPutIfAbsentIdempotent(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	if err := s.PutIfAbsent(ctx, "a/b/c.bin", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutIfAbsent(ctx, "a/b/c.bin", []byte("one")); err != nil {
		t.Fatalf("identical bytes: %v", err)
	}
	if err := s.PutIfAbsent(ctx, "a/b/c.bin", []byte("two")); !errors.Is(err, ErrExists) {
		t.Fatalf("different bytes: got %v, want ErrExists", err)
	}
	got, _ := s.Get(ctx, "a/b/c.bin")
	if string(got) != "one" {
		t.Fatalf("content changed: %q", got)
	}
}

func TestPutOverwrites(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	_ = s.Put(ctx, "k", []byte("one"))
	if err := s.Put(ctx, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "k")
	if string(got) != "two" {
		t.Fatalf("got %q", got)
	}
}

func TestGetRange(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	_ = s.Put(ctx, "k", []byte("0123456789"))
	got, err := s.GetRange(ctx, "k", 3, 4)
	if err != nil || string(got) != "3456" {
		t.Fatalf("got %q, %v", got, err)
	}
	for _, c := range [][2]int64{{-1, 2}, {0, 0}, {0, -1}} {
		if _, err := s.GetRange(ctx, "k", c[0], c[1]); err == nil {
			t.Fatalf("off=%d n=%d: expected error", c[0], c[1])
		}
	}
	if _, err := s.GetRange(ctx, "k", 8, 5); err == nil {
		t.Fatal("range past EOF: expected error")
	}
	if _, err := s.GetRange(ctx, "missing", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestNotFoundAndExists(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	ok, err := s.Exists(ctx, "nope")
	if err != nil || ok {
		t.Fatalf("exists: %v %v", ok, err)
	}
	_ = s.Put(ctx, "yes", []byte("x"))
	ok, err = s.Exists(ctx, "yes")
	if err != nil || !ok {
		t.Fatalf("exists: %v %v", ok, err)
	}
}

func TestFSDelete(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	if err := s.Put(ctx, "a/k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a/k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "a/k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := s.Delete(ctx, "a/k"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if err := s.Delete(ctx, "../x"); err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestPathTraversalRejected(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	for _, key := range []string{"../x", "a/../../x", "/abs/x", "", "a/..", ".."} {
		if err := s.Put(ctx, key, []byte("x")); err == nil {
			t.Errorf("Put(%q): expected error", key)
		}
		if err := s.PutIfAbsent(ctx, key, []byte("x")); err == nil {
			t.Errorf("PutIfAbsent(%q): expected error", key)
		}
		if _, err := s.Get(ctx, key); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%q): expected validation error, got %v", key, err)
		}
		if _, err := s.Exists(ctx, key); err == nil {
			t.Errorf("Exists(%q): expected error", key)
		}
	}
}

func TestConcurrentPutIfAbsentIdentical(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	body := bytes.Repeat([]byte("x"), 1<<16)
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.PutIfAbsent(ctx, "race/key", body)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	got, err := s.Get(ctx, "race/key")
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("final content wrong: %v", err)
	}
}

func TestConcurrentPutIfAbsentDifferent(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.PutIfAbsent(ctx, "k", []byte{byte('a' + i)})
		}()
	}
	wg.Wait()
	nilCount := 0
	for _, err := range errs {
		if err == nil {
			nilCount++
		} else if !errors.Is(err, ErrExists) {
			t.Fatalf("unexpected: %v", err)
		}
	}
	if nilCount != 1 {
		t.Fatalf("exactly one writer must win, got %d", nilCount)
	}
}

func TestFSGetRangeOutOfRange(t *testing.T) {
	s := NewFS(t.TempDir())
	ctx := context.Background()
	if err := s.Put(ctx, "k", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]int64{{8, 5}, {10, 1}, {50, 1}} {
		if _, err := s.GetRange(ctx, "k", c[0], c[1]); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("off=%d n=%d: got %v, want ErrOutOfRange", c[0], c[1], err)
		}
	}
}

func TestFSList(t *testing.T) {
	root := t.TempDir()
	s := NewFS(root)
	ctx := context.Background()
	if keys, err := s.List(ctx, "segments/"); err != nil || len(keys) != 0 {
		t.Fatalf("empty store: %v, %v", keys, err)
	}
	for _, k := range []string{"segments/b.seg", "segments/a.seg", "segments/a.idx2", "segments/sub/c.seg", "seg.json", "dir/x.dir"} {
		if err := s.Put(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	// A leftover temp file of an interrupted write is not an object.
	if err := os.WriteFile(filepath.Join(root, "segments", ".tmp-123"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		prefix string
		want   []string
	}{
		{"segments/", []string{"segments/a.idx2", "segments/a.seg", "segments/b.seg", "segments/sub/c.seg"}},
		{"segments/a", []string{"segments/a.idx2", "segments/a.seg"}},
		{"seg", []string{"seg.json", "segments/a.idx2", "segments/a.seg", "segments/b.seg", "segments/sub/c.seg"}},
		{"", []string{"dir/x.dir", "seg.json", "segments/a.idx2", "segments/a.seg", "segments/b.seg", "segments/sub/c.seg"}},
		{"nothing/", nil},
	}
	for _, c := range cases {
		got, err := s.List(ctx, c.prefix)
		if err != nil {
			t.Fatalf("List(%q): %v", c.prefix, err)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("List(%q) = %v, want %v", c.prefix, got, c.want)
		}
	}
}
