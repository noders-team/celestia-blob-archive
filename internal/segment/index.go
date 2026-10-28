package segment

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

// EncodeIndex marshals a SegmentIndex.
func EncodeIndex(idx *archivev1.SegmentIndex) ([]byte, error) {
	b, err := proto.Marshal(idx)
	if err != nil {
		return nil, fmt.Errorf("marshal index: %w", err)
	}
	return b, nil
}

// DecodeIndex unmarshals a SegmentIndex.
func DecodeIndex(b []byte) (*archivev1.SegmentIndex, error) {
	idx := &archivev1.SegmentIndex{}
	if err := proto.Unmarshal(b, idx); err != nil {
		return nil, fmt.Errorf("decode index: %w", err)
	}
	return idx, nil
}
