package source

import (
	"context"
	"sync"
	"testing"
)

func TestFakeBasics(t *testing.T) {
	f := NewFake(map[int64]*Block{5: {Height: 5}}, 5)
	ctx := context.Background()
	if h, err := f.Head(ctx); err != nil || h != 5 {
		t.Fatalf("Head = %d, %v", h, err)
	}
	f.SetHead(9)
	if h, _ := f.Head(ctx); h != 9 {
		t.Fatalf("Head after SetHead = %d", h)
	}
	if b, err := f.Block(ctx, 5); err != nil || b.Height != 5 {
		t.Fatalf("Block(5) = %v, %v", b, err)
	}
	if _, err := f.Block(ctx, 6); err == nil {
		t.Fatal("expected error for missing block")
	}
}

func TestFakeFailNext(t *testing.T) {
	f := NewFake(map[int64]*Block{1: {Height: 1}, 2: {Height: 2}}, 2)
	ctx := context.Background()
	f.FailNext(1, 2)
	for i := 0; i < 2; i++ {
		if _, err := f.Block(ctx, 1); err == nil {
			t.Fatalf("call %d: expected failure", i)
		}
	}
	if _, err := f.Block(ctx, 2); err != nil {
		t.Fatalf("other height must not fail: %v", err)
	}
	if _, err := f.Block(ctx, 1); err != nil {
		t.Fatalf("expected success after 2 failures: %v", err)
	}
}

func TestFakeConcurrent(t *testing.T) {
	f := NewFake(map[int64]*Block{1: {Height: 1}}, 1)
	f.FailNext(1, 50)
	var wg sync.WaitGroup
	var mu sync.Mutex
	fails := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.SetHead(int64(i))
			f.Head(context.Background())
			if _, err := f.Block(context.Background(), 1); err != nil {
				mu.Lock()
				fails++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if fails != 50 {
		t.Fatalf("fails = %d, want exactly 50", fails)
	}
}
