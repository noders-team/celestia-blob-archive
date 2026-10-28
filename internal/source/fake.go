package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrFakeTransport is returned by Fake.Block for injected failures.
var ErrFakeTransport = errors.New("fake source: transport error")

// Fake is an in-memory Source for tests. Safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	blocks map[int64]*Block
	head   int64
	fails  map[int64]int
}

// NewFake returns a Fake serving the given blocks with the given head.
func NewFake(blocks map[int64]*Block, head int64) *Fake {
	cp := make(map[int64]*Block, len(blocks))
	for h, b := range blocks {
		cp[h] = b
	}
	return &Fake{blocks: cp, head: head, fails: map[int64]int{}}
}

var _ Source = (*Fake)(nil)

// SetHead changes the reported head height.
func (f *Fake) SetHead(h int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.head = h
}

// FailNext makes the next n Block(height) calls fail with ErrFakeTransport.
func (f *Fake) FailNext(height int64, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails[height] = n
}

func (f *Fake) Head(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head, nil
}

func (f *Fake) Block(ctx context.Context, height int64) (*Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails[height] > 0 {
		f.fails[height]--
		return nil, fmt.Errorf("block %d: %w", height, ErrFakeTransport)
	}
	b, ok := f.blocks[height]
	if !ok {
		return nil, fmt.Errorf("block %d: not found", height)
	}
	return b, nil
}
