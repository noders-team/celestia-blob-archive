// Package client is a read-only store.Store over HTTP for the public bucket.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/version"
)

// maxObject bounds what Get will read into memory.
const maxObject = 512 << 20

// ErrReadOnly is returned by all write methods.
var ErrReadOnly = errors.New("client: archive is read-only over HTTP")

// Client reads archive objects from a public base URL.
type Client struct {
	base string
	http *http.Client
}

var (
	_ store.Store       = (*Client)(nil)
	_ store.LimitGetter = (*Client)(nil)
)

// New returns a Client for the bucket base URL (trailing slash optional).
func New(baseURL string) *Client {
	return NewWithClient(baseURL, &http.Client{Timeout: 60 * time.Second})
}

// NewWithClient returns a Client for the bucket base URL that sends its
// requests with hc. Calls are bounded by their context and by hc.Timeout.
func NewWithClient(baseURL string, hc *http.Client) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), http: hc}
}

func (c *Client) do(ctx context.Context, method, key string, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+"/"+key, nil)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", key, err)
	}
	req.Header.Set("User-Agent", "cba/"+version.Version)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, key, err)
	}
	return resp, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.GetLimit(ctx, key, maxObject)
}

// GetLimit is Get reading at most max bytes (and never more than maxObject)
// of the body; a larger object fails with store.ErrTooLarge.
func (c *Client) GetLimit(ctx context.Context, key string, max int64) ([]byte, error) {
	max = min(max, maxObject)
	resp, err := c.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, fmt.Errorf("%s: %w", key, store.ErrNotFound)
	default:
		return nil, fmt.Errorf("GET %s: unexpected status %s", key, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", key, err)
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("GET %s: %w: larger than %d bytes", key, store.ErrTooLarge, max)
	}
	return b, nil
}

func (c *Client) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n <= 0 || n > maxObject {
		return nil, fmt.Errorf("GET %s: %w: invalid range off=%d n=%d", key, store.ErrOutOfRange, off, n)
	}
	resp, err := c.do(ctx, http.MethodGet, key, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-%d", off, off+n-1),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusNotFound:
		return nil, fmt.Errorf("%s: %w", key, store.ErrNotFound)
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, fmt.Errorf("GET %s range [%d,+%d): %w", key, off, n, store.ErrOutOfRange)
	default:
		return nil, fmt.Errorf("GET %s range: want status 206, got %s", key, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, n+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s range: read body: %w", key, err)
	}
	if int64(len(b)) < n {
		return nil, fmt.Errorf("GET %s range [%d,+%d): got %d bytes: %w", key, off, n, len(b), store.ErrOutOfRange)
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("GET %s range [%d,+%d): got %d bytes", key, off, n, len(b))
	}
	return b, nil
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	resp, err := c.do(ctx, http.MethodHead, key, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("HEAD %s: unexpected status %s", key, resp.Status)
	}
}

// List is not available over the public HTTP endpoint.
func (c *Client) List(context.Context, string) ([]string, error) {
	return nil, ErrReadOnly
}

func (c *Client) Put(context.Context, string, []byte, ...store.PutOptions) error {
	return ErrReadOnly
}

func (c *Client) PutIfAbsent(context.Context, string, []byte, ...store.PutOptions) error {
	return ErrReadOnly
}

func (c *Client) Delete(context.Context, string) error {
	return ErrReadOnly
}
