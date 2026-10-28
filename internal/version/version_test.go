package version_test

import (
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/da"
	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/noders-team/celestia-blob-archive/internal/version"
)

var _ = proof.NewShareInclusionProof

func TestImportsCompile(t *testing.T) {
	if version.Version == "" {
		t.Fatal("empty version")
	}
	if _, err := da.ConstructEDS(nil, 10, -1); err != nil {
		t.Fatalf("ConstructEDS on empty block: %v", err)
	}
}
