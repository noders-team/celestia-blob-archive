package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// HeadSource reports the chain head height (e.g. a gRPC node).
type HeadSource interface {
	Head(ctx context.Context) (int64, error)
}

// Inspect prints the manifest, the segment count, the committed ranges and
// gaps of the catalog and, if head is not nil, the lag behind the chain head.
func Inspect(ctx context.Context, st store.Store, head HeadSource, w io.Writer) error {
	man, err := archiveread.LoadManifest(ctx, st)
	if err != nil {
		return err
	}
	refs, err := archiveread.LoadCatalog(ctx, st)
	if err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}
	committed, gaps := archiveread.Ranges(refs)
	fmt.Fprintf(w, "chain_id:           %s\n", man.ChainID)
	fmt.Fprintf(w, "format_version:     %d\n", man.FormatVersion)
	fmt.Fprintf(w, "highest_height:     %d\n", man.HighestHeight)
	fmt.Fprintf(w, "contiguous_until:   %d\n", man.ContiguousUntil)
	fmt.Fprintf(w, "last_catalog_chunk: %d\n", man.LastCatalogChunk)
	fmt.Fprintf(w, "updated_at:         %s\n", man.UpdatedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "segments:           %d\n", len(refs))
	fmt.Fprintf(w, "committed ranges:   %s\n", archiveread.JoinRanges(committed))
	fmt.Fprintf(w, "gaps:               %s\n", archiveread.JoinRanges(gaps))
	if head != nil {
		h, err := head.Head(ctx)
		if err != nil {
			return fmt.Errorf("get chain head: %w", err)
		}
		fmt.Fprintf(w, "chain head:         %d\n", h)
		fmt.Fprintf(w, "lag:                %d blocks behind highest, %d behind contiguous\n",
			h-man.HighestHeight, h-man.ContiguousUntil)
	}
	return nil
}
