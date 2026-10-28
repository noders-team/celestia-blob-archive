package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/noders-team/celestia-blob-archive/internal/store"
)

func newServer(t *testing.T) (*httptest.Server, *string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "obj"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	var ua string
	fs := http.FileServer(http.Dir(dir))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
		fs.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &ua
}

func TestGetAndRange(t *testing.T) {
	srv, ua := newServer(t)
	c := New(srv.URL + "/")
	ctx := context.Background()
	b, err := c.Get(ctx, "a/obj")
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("Get: %q %v", b, err)
	}
	if !strings.HasPrefix(*ua, "cba/") {
		t.Fatalf("user agent %q", *ua)
	}
	r, err := c.GetRange(ctx, "a/obj", 3, 4)
	if err != nil || string(r) != "3456" {
		t.Fatalf("GetRange: %q %v", r, err)
	}
	// Range beyond the object end: server returns fewer bytes (206) or 416.
	if _, err := c.GetRange(ctx, "a/obj", 8, 5); !errors.Is(err, store.ErrOutOfRange) {
		t.Fatalf("short range: %v, want ErrOutOfRange", err)
	}
	if _, err := c.GetRange(ctx, "a/obj", 20, 5); !errors.Is(err, store.ErrOutOfRange) {
		t.Fatalf("range past end: %v, want ErrOutOfRange", err)
	}
	ok, err := c.Exists(ctx, "a/obj")
	if err != nil || !ok {
		t.Fatalf("Exists: %v %v", ok, err)
	}
}

func TestGetLimit(t *testing.T) {
	srv, _ := newServer(t)
	c := New(srv.URL)
	ctx := context.Background()
	if b, err := store.GetLimit(ctx, c, "a/obj", 10); err != nil || string(b) != "0123456789" {
		t.Fatalf("at the limit: %q, %v", b, err)
	}
	if _, err := store.GetLimit(ctx, c, "a/obj", 9); !errors.Is(err, store.ErrTooLarge) {
		t.Fatalf("over the limit: %v, want ErrTooLarge", err)
	}
	if _, err := store.GetLimit(ctx, c, "a/missing", 9); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v, want ErrNotFound", err)
	}
}

func TestNotFound(t *testing.T) {
	srv, _ := newServer(t)
	c := New(srv.URL)
	ctx := context.Background()
	if _, err := c.Get(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.GetRange(ctx, "nope", 0, 1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRange: %v", err)
	}
	if ok, err := c.Exists(ctx, "nope"); err != nil || ok {
		t.Fatalf("Exists: %v %v", ok, err)
	}
}

func TestRangeIgnoredByServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("0123456789")) // 200, ignores Range
	}))
	defer srv.Close()
	if _, err := New(srv.URL).GetRange(context.Background(), "k", 1, 2); err == nil {
		t.Fatal("200 response to a range request accepted")
	}
}

func TestServerErrorAndReadOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := New(srv.URL)
	ctx := context.Background()
	if _, err := c.Get(ctx, "k"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get 500: %v", err)
	}
	if err := c.Put(ctx, "k", nil); err == nil {
		t.Fatal("Put succeeded")
	}
	if err := c.PutIfAbsent(ctx, "k", nil); err == nil {
		t.Fatal("PutIfAbsent succeeded")
	}
}

func TestClientDeleteReadOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()
	if err := New(srv.URL).Delete(context.Background(), "k"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Delete: %v", err)
	}
}

func TestNewWithClientUsesGivenClient(t *testing.T) {
	srv, _ := newServer(t)
	var used bool
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used = true
		return http.DefaultTransport.RoundTrip(r)
	})}
	c := NewWithClient(srv.URL+"/", hc)
	b, err := c.Get(context.Background(), "a/obj")
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("Get = %q, %v", b, err)
	}
	if !used {
		t.Fatal("NewWithClient did not use the given client")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
