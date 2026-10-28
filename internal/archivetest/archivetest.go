// Package archivetest builds small archives from real block fixtures for
// tests. It must only be imported from test code.
package archivetest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	"github.com/cometbft/cometbft/crypto"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// ChainID is the chain id of archives built here.
const ChainID = "test-chain"

func fixture(tb testing.TB, name string) *source.Block {
	tb.Helper()
	_, file, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "testdata", "blocks", name+".json"))
	if err != nil {
		tb.Fatal(err)
	}
	var f struct{ Cur *source.Block }
	if err := json.Unmarshal(raw, &f); err != nil || f.Cur == nil {
		tb.Fatalf("fixture %s: %v", name, err)
	}
	return f.Cur
}

// Blocks returns a chain of blocks 1..n. Heights in blobAt carry the
// transactions of the named fixture (e.g. "mocha-1834013") re-labelled to that
// height; all others are empty. Process only checks that the commit matches
// the header hash, so a re-labelled block still validates against its data
// hash.
func Blocks(tb testing.TB, n int64, blobAt map[int64]string) map[int64]*source.Block {
	tb.Helper()
	eds, err := da.ConstructEDS(nil, 10, -1)
	if err != nil {
		tb.Fatal(err)
	}
	dah, err := da.NewDataAvailabilityHeader(eds)
	if err != nil {
		tb.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	m := make(map[int64]*source.Block, n)
	for h := int64(1); h <= n; h++ {
		b := &source.Block{
			Height: h, Time: base.Add(time.Duration(h) * 6 * time.Second), AppVersion: 10,
			DataHash: dah.Hash(),
		}
		if name, ok := blobAt[h]; ok {
			fb := *fixture(tb, name)
			fb.Height, fb.Time = h, b.Time
			b = &fb
		}
		m[h] = b
	}
	Link(tb, m)
	return m
}

// Link gives every block without a HeaderProto a synthetic CometBFT header
// and sets each block's LastCommitProto to a commit for the previous block's
// header, so Process accepts every (h, h+1) pair. Fixture blocks keep their
// real header. Commits carry no signatures.
func Link(tb testing.TB, blocks map[int64]*source.Block) {
	tb.Helper()
	heights := make([]int64, 0, len(blocks))
	for h := range blocks {
		heights = append(heights, h)
	}
	slices.Sort(heights)
	var prev cmtproto.BlockID
	for _, h := range heights {
		b := blocks[h]
		if b.HeaderProto == nil {
			hp := cmtproto.Header{
				Version:         cmtversion.Consensus{Block: version.BlockProtocol, App: b.AppVersion},
				ChainID:         ChainID,
				Height:          h,
				Time:            b.Time,
				LastBlockId:     prev,
				DataHash:        b.DataHash,
				ValidatorsHash:  make([]byte, 32), // Header.Hash is empty without it
				ProposerAddress: make([]byte, crypto.AddressSize),
			}
			raw, err := hp.Marshal()
			if err != nil {
				tb.Fatal(err)
			}
			b.HeaderProto = raw
		}
		c := cmtproto.Commit{Height: h - 1, BlockID: prev}
		raw, err := c.Marshal()
		if err != nil {
			tb.Fatal(err)
		}
		b.LastCommitProto = raw
		var hp cmtproto.Header
		if err := hp.Unmarshal(b.HeaderProto); err != nil {
			tb.Fatal(err)
		}
		hdr, err := cmttypes.HeaderFromProto(&hp)
		if err != nil {
			tb.Fatalf("block %d header: %v", h, err)
		}
		prev = cmtproto.BlockID{Hash: hdr.Hash()}
	}
}

// Mutate may replace the records of a height before they are archived.
type Mutate func(*archivev1.HeightRecord, []*archivev1.BlobRecord) (*archivev1.HeightRecord, []*archivev1.BlobRecord)

// Build processes heights first..last of blocks (block last+1 must exist) and
// commits them to st in segments of segLen heights. mut may be nil.
func Build(tb testing.TB, st store.Store, blocks map[int64]*source.Block, first, last int64, segLen int, mut Mutate) {
	tb.Helper()
	ctx := context.Background()
	c, err := archive.NewCommitter(ctx, st, ChainID)
	if err != nil {
		tb.Fatal(err)
	}
	b := archive.NewBuilder(nil)
	flush := func() {
		ref, err := b.Flush(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if err := c.Commit(ctx, *ref); err != nil {
			tb.Fatal(err)
		}
	}
	n := 0
	for h := first; h <= last; h++ {
		hr, blobs, err := process.Process(blocks[h], blocks[h+1])
		if err != nil {
			tb.Fatalf("process %d: %v", h, err)
		}
		if mut != nil {
			hr, blobs = mut(hr, blobs)
		}
		if err := b.Add(hr, blobs); err != nil {
			tb.Fatal(err)
		}
		if n++; n == segLen {
			flush()
			n = 0
		}
	}
	if n > 0 {
		flush()
	}
}

// Withdraw overwrites blob records of the segment segKey in place with
// tombstones of the same length, as a takedown scrub does. f gets a copy of
// each blob record; it returns whether to withdraw it and may change the
// copy (e.g. its share range) to forge the tombstone. It returns how many
// records were replaced.
func Withdraw(tb testing.TB, st store.Store, segKey string, f func(*archivev1.BlobRecord) bool) int {
	tb.Helper()
	ctx := context.Background()
	seg, err := st.Get(ctx, segKey)
	if err != nil {
		tb.Fatal(err)
	}
	rows, err := segment.ScanRows(seg)
	if err != nil {
		tb.Fatal(err)
	}
	out := slices.Clone(seg)
	n := 0
	for _, r := range rows {
		if r.Kind != segment.KindBlob {
			continue
		}
		rec, err := segment.ReadAt(seg, r.Offset, r.Length)
		if err != nil {
			tb.Fatal(err)
		}
		b := rec.GetBlob()
		if b == nil {
			continue
		}
		b = proto.Clone(b).(*archivev1.BlobRecord)
		if !f(b) {
			continue
		}
		t, err := segment.TombstoneFor(b, r.Length)
		if err != nil {
			tb.Fatal(err)
		}
		copy(out[r.Offset:], t)
		n++
	}
	if err := st.Put(ctx, segKey, out); err != nil {
		tb.Fatal(err)
	}
	return n
}
