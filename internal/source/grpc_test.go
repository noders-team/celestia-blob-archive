package source

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
)

// The hash of block h's header is the LastBlockId of block h+1, which the
// fixture holds as next.
func TestCheckHeaderHash(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "testdata", "blocks", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("fixtures: %v %v", paths, err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var f struct{ Cur, Next *Block }
			if err := json.Unmarshal(raw, &f); err != nil {
				t.Fatal(err)
			}
			var cur, next cmtproto.Header
			if err := cur.Unmarshal(f.Cur.HeaderProto); err != nil {
				t.Fatal(err)
			}
			if err := next.Unmarshal(f.Next.HeaderProto); err != nil {
				t.Fatal(err)
			}
			want := next.LastBlockId.Hash
			if len(want) != 32 {
				t.Fatalf("next LastBlockId hash %x", want)
			}
			if err := checkHeaderHash(&cur, want); err != nil {
				t.Fatalf("genuine header: %v", err)
			}
			bad := append([]byte(nil), want...)
			bad[0] ^= 1
			if err := checkHeaderHash(&cur, bad); err == nil {
				t.Fatal("wrong block id hash accepted")
			}
			tampered := cur
			tampered.DataHash = append([]byte(nil), cur.DataHash...)
			tampered.DataHash[0] ^= 1
			if err := checkHeaderHash(&tampered, want); err == nil {
				t.Fatal("tampered header accepted")
			}
			if err := checkHeaderHash(&cur, nil); err == nil {
				t.Fatal("missing block id accepted")
			}
		})
	}
}
