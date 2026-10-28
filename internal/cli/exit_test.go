package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

func TestExitStatus(t *testing.T) {
	cases := []struct {
		err    error
		code   int
		prefix string
	}{
		{nil, 0, ""},
		{errors.New("--namespace: invalid base64"), 1, "error: "},
		{fmt.Errorf("load catalog: %w", store.ErrNotFound), 1, "error: "},
		{fmt.Errorf("fetch header 3: %w", context.DeadlineExceeded), 1, "error: "},
		{fmt.Errorf("%w: height 3 is not archived", verify.ErrNotArchived), 1, "error: "},
	}
	for _, s := range []error{
		verify.ErrNotInArchive, verify.ErrSharesMismatch, verify.ErrCommitmentMismatch, verify.ErrProofInvalid,
		verify.ErrDataRootMismatch, verify.ErrProofLayout, verify.ErrReservedNamespace, ErrFibreCheck,
	} {
		cases = append(cases, struct {
			err    error
			code   int
			prefix string
		}{fmt.Errorf("%w: detail", s), 2, "verification FAILED: " + s.Error()})
	}
	for _, c := range cases {
		code, msg := ExitStatus(c.err)
		if code != c.code || !strings.HasPrefix(msg, c.prefix) || (c.err == nil) != (msg == "") {
			t.Errorf("ExitStatus(%v) = %d %q, want %d with prefix %q", c.err, code, msg, c.code, c.prefix)
		}
	}
}
