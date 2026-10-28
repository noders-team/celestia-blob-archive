package nsrules_test

import (
	"errors"
	"testing"

	"github.com/celestiaorg/go-square/v4/share"

	"github.com/noders-team/celestia-blob-archive/internal/nsrules"
)

func TestCheckBlobNamespace(t *testing.T) {
	user := share.MustNewV0Namespace([]byte("rollup"))
	if err := nsrules.CheckBlobNamespace(user.Bytes()); err != nil {
		t.Errorf("user namespace %x: %v", user.Bytes(), err)
	}
	for name, ns := range map[string][]byte{
		"tx":           share.TxNamespace.Bytes(),
		"pay for blob": share.PayForBlobNamespace.Bytes(),
		"padding":      share.PrimaryReservedPaddingNamespace.Bytes(),
		"tail padding": share.TailPaddingNamespace.Bytes(),
		"parity":       share.ParitySharesNamespace.Bytes(),
		"28 bytes":     user.Bytes()[1:],
		"empty":        nil,
	} {
		if err := nsrules.CheckBlobNamespace(ns); !errors.Is(err, nsrules.ErrReserved) {
			t.Errorf("%s %x: got %v, want ErrReserved", name, ns, err)
		}
	}
}
