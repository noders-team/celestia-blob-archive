package fibrecapture

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/fibre"
)

// ErrMismatch means downloaded bytes do not match the payment promise.
var ErrMismatch = errors.New("fibrecapture: payload does not match the promise")

// VerifyPayload checks that data re-encodes to the blob ID id and that its
// padded upload size equals the promise's blobSize. data is not modified.
func VerifyPayload(data []byte, id [33]byte, blobSize uint32) error {
	cfg, err := fibre.BlobConfigForVersion(id[0])
	if err != nil {
		return fmt.Errorf("verify blob %x: %w", id, err)
	}
	if got := cfg.UploadSize(len(data)); got != int(blobSize) {
		return fmt.Errorf("%w: blob %x: upload size %d, promise blob_size %d", ErrMismatch, id, got, blobSize)
	}
	// NewBlob takes ownership of its input and may reuse it as row storage,
	// so it gets a copy: data is written to the store afterwards.
	b, err := fibre.NewBlob(bytes.Clone(data), cfg)
	if err != nil {
		// Empty or oversized: no blob with this ID can hold these bytes.
		return fmt.Errorf("%w: blob %x: encode %d bytes: %v", ErrMismatch, id, len(data), err)
	}
	defer b.Free()
	if got := b.ID(); !bytes.Equal(got, id[:]) {
		return fmt.Errorf("%w: payload encodes to blob %x, promise has %x", ErrMismatch, []byte(got), id)
	}
	return nil
}
