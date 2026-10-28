package cli

import (
	"errors"

	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

// Exit statuses of cba.
const (
	ExitOK = 0
	// ExitError: the command could not run (usage, network, I/O, archive
	// unreadable). Nothing was verified.
	ExitError = 1
	// ExitVerifyFailed: verification ran and a check failed.
	ExitVerifyFailed = 2
)

// ExitStatus maps a command error to the process exit status and the message
// to print on stderr ("" for success).
func ExitStatus(err error) (int, string) {
	switch {
	case err == nil:
		return ExitOK, ""
	case verify.IsFailure(err), errors.Is(err, ErrFibreCheck):
		return ExitVerifyFailed, "verification FAILED: " + err.Error()
	default:
		return ExitError, "error: " + err.Error()
	}
}
