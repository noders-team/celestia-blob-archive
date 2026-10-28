package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
)

// checkHeightRecord requires hr to hold the header and commit of height:
// the header is of that height, its DataHash is the record's data_hash (the
// root blob proofs validate against) and the commit signs that header.
func checkHeightRecord(hr *archivev1.HeightRecord, height int64) error {
	if hr == nil {
		return errors.New("no height record")
	}
	var hp cmtproto.Header
	if err := hp.Unmarshal(hr.Header); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}
	h, err := cmttypes.HeaderFromProto(&hp)
	if err != nil {
		return fmt.Errorf("header: %w", err)
	}
	var cp cmtproto.Commit
	if err := cp.Unmarshal(hr.Commit); err != nil {
		return fmt.Errorf("decode commit: %w", err)
	}
	switch {
	case h.Height != height || hr.Height != uint64(height):
		return fmt.Errorf("record of height %d holds header of height %d", hr.Height, h.Height)
	case !bytes.Equal(h.DataHash, hr.DataHash):
		return fmt.Errorf("header data hash %X is not the record's data hash %X", h.DataHash, hr.DataHash)
	case cp.Height != height:
		return fmt.Errorf("commit of height %d for header of height %d", cp.Height, height)
	case !bytes.Equal(cp.BlockID.Hash, h.Hash()):
		return fmt.Errorf("commit block hash %X is not the header hash %X", cp.BlockID.Hash, h.Hash())
	}
	return nil
}

// header returns the trimmed ExtendedHeader of height h (see HeaderFrom).
func (s *Server) header(ctx context.Context, h uint64) (json.RawMessage, *rpcError) {
	height, rerr := s.archived(h)
	if rerr != nil {
		return nil, rerr
	}
	e, err := archiveread.Locate(ctx, s.st, height)
	if err != nil {
		return nil, s.readErr(height, nil, nil, err)
	}
	rows, err := archiveread.LoadRows(ctx, s.st, e, height)
	if err != nil {
		return nil, s.readErr(height, nil, nil, err)
	}
	rec, err := archiveread.ReadRecord(ctx, s.st, archive.SegmentKey(e.SegFirst, e.SegLast), rows[0])
	if err != nil {
		return nil, s.readErr(height, nil, nil, err)
	}
	hr := rec.GetHeight()
	if err := checkHeightRecord(hr, height); err != nil {
		return nil, s.verifyFailed(height, nil, nil, err)
	}
	out, err := HeaderFrom(hr)
	if err != nil {
		return nil, s.verifyFailed(height, nil, nil, err)
	}
	return out, nil
}

// headerGetByHeight serves header.GetByHeight with the archive's trimmed
// header: header and canonical commit, validator_set and dah null.
func (s *Server) headerGetByHeight(ctx context.Context, p []json.RawMessage, _ reserveFunc) (json.RawMessage, *rpcError) {
	h, rerr := parseHeight(p[0])
	if rerr != nil {
		return nil, rerr
	}
	return s.header(ctx, h)
}

// headerNetworkHead serves header.NetworkHead as the header of the
// manifest's highest_height: the archive's head, which trails the chain's.
func (s *Server) headerNetworkHead(ctx context.Context, _ []json.RawMessage, _ reserveFunc) (json.RawMessage, *rpcError) {
	highest, _ := s.state().heights()
	if highest < 1 {
		s.log.Warn("header.NetworkHead on an empty archive")
		return nil, errUpstream
	}
	return s.header(ctx, uint64(highest))
}
