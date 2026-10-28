// Package nsrules holds the rule for which namespaces can hold a blob. It is
// a leaf package so both verify and oplayer can use it without a cycle.
package nsrules

import (
	"errors"
	"fmt"

	"github.com/celestiaorg/go-square/v4/share"
)

// ErrReserved: the namespace is reserved for the protocol (tx, PayForBlob,
// padding, parity, ...) and so cannot hold a blob.
var ErrReserved = errors.New("namespace is reserved, not a blob namespace")

// CheckBlobNamespace requires ns to be a valid namespace outside the primary
// and secondary reserved ranges (which include tx, PFB, padding, parity and
// tail padding). IsUsableNamespace alone only excludes parity and tail padding.
// Errors wrap ErrReserved.
func CheckBlobNamespace(ns []byte) error {
	n, err := share.NewNamespaceFromBytes(ns)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrReserved, err)
	}
	if n.IsReserved() || !n.IsUsableNamespace() {
		return fmt.Errorf("%w: %x", ErrReserved, ns)
	}
	return nil
}
