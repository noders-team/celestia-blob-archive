//go:build integration

package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestR2Integration(t *testing.T) {
	acc, id, secret, bucket := os.Getenv("R2_ACCOUNT_ID"), os.Getenv("R2_ACCESS_KEY_ID"),
		os.Getenv("R2_SECRET_ACCESS_KEY"), os.Getenv("R2_TEST_BUCKET")
	if acc == "" || id == "" || secret == "" || bucket == "" {
		t.Skip("R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_TEST_BUCKET not all set")
	}
	for _, cond := range []bool{false, true} {
		name := "head-then-put"
		if cond {
			name = "if-none-match"
		}
		t.Run(name, func(t *testing.T) {
			s, err := NewR2(acc, id, secret, bucket, Options{UseConditionalPut: cond})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			var r [8]byte
			_, _ = rand.Read(r[:])
			key := "test/" + hex.EncodeToString(r[:]) + "/obj.bin"
			t.Cleanup(func() { cleanupR2(t, s, key) })

			if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing get: %v", err)
			}
			if err := s.PutIfAbsent(ctx, key, []byte("0123456789")); err != nil {
				t.Fatal(err)
			}
			if err := s.PutIfAbsent(ctx, key, []byte("0123456789")); err != nil {
				t.Fatalf("identical: %v", err)
			}
			if err := s.PutIfAbsent(ctx, key, []byte("other")); !errors.Is(err, ErrExists) {
				t.Fatalf("different: %v", err)
			}
			got, err := s.GetRange(ctx, key, 3, 4)
			if err != nil || string(got) != "3456" {
				t.Fatalf("range: %q %v", got, err)
			}
			if ok, err := s.Exists(ctx, key); err != nil || !ok {
				t.Fatalf("exists: %v %v", ok, err)
			}
		})
	}
}

// cleanupR2 deletes the test object with a raw client (Store has no Delete).
func cleanupR2(t *testing.T, s Store, key string) {
	t.Helper()
	st := s.(*s3Store)
	c, ok := st.api.(*s3.Client)
	if !ok {
		t.Errorf("cleanup: unexpected api type %T", st.api)
		return
	}
	if _, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &st.bucket, Key: &key}); err != nil {
		t.Errorf("cleanup %q: %v", key, err)
	}
}
