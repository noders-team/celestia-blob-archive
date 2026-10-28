package api

import (
	"fmt"
	"regexp"
)

var methodName = regexp.MustCompile(`^[A-Za-z0-9_.]{1,64}$`)

// JSON-RPC 2.0 error codes. App errors use code 1, as celestia-node's
// go-jsonrpc does for every error it has no registered code for.
const (
	codeApp            = 1
	codeParse          = -32700
	codeInvalidRequest = -32600
	codeNoMethod       = -32601
	codeInvalidParams  = -32602
)

// Outcomes of a call, as cba_api_requests_total labels them (Task 11).
const (
	outcomeOK           = "ok"
	outcomeNotFound     = "not_found"
	outcomeNotArchived  = "not_archived"
	outcomeWithdrawn    = "withdrawn"
	outcomeBadRequest   = "bad_request"
	outcomeUpstream     = "upstream"
	outcomeVerifyFailed = "verify_failed"
)

// rpcError is an error returned to the client. No message except
// msgNotFound may contain "not found": op-alt-da treats that substring as
// final and ev-node skips the height.
type rpcError struct {
	code    int
	msg     string
	outcome string
}

func (e *rpcError) Error() string { return e.msg }

func (e *rpcError) wire() *JSONRPCError { return &JSONRPCError{Code: e.code, Message: e.msg} }

// msgNotFound is celestia-node's error for an archived height without the
// blob, byte for byte.
const msgNotFound = "blob: not found"

var (
	errNotFound  = &rpcError{codeApp, msgNotFound, outcomeNotFound}
	errHeight0   = &rpcError{codeApp, "header: height is equal to 0", outcomeBadRequest}
	errWithdrawn = &rpcError{codeApp,
		"blob withdrawn from this archive (see tombstones.json); fetch it from another source", outcomeWithdrawn}
	errUpstream   = &rpcError{codeApp, "archive: upstream unavailable", outcomeUpstream}
	errParse      = &rpcError{codeParse, "parse error", outcomeBadRequest}
	errEmptyBatch = &rpcError{codeInvalidRequest, "invalid request: empty batch", outcomeBadRequest}
	errBadRequest = &rpcError{codeInvalidRequest, "invalid request: want a JSON-RPC 2.0 call", outcomeBadRequest}
)

// errFuture: height is above the manifest's highest_height. The message
// carries the text ev-node retries on.
func errFuture(height uint64, highest int64) *rpcError {
	return &rpcError{codeApp, fmt.Sprintf(
		"archive: height %d not archived yet (header: given height is from the future: highest archived height %d)",
		height, highest), outcomeNotArchived}
}

// errGap: height is at most highest_height but not archived.
func errGap(height uint64, contiguous, highest int64) *rpcError {
	return &rpcError{codeApp, fmt.Sprintf(
		"archive: height %d not archived (gap in the archive; contiguous up to %d, highest archived %d)",
		height, contiguous, highest), outcomeNotArchived}
}

func errVerify(height int64) *rpcError {
	return &rpcError{codeApp, fmt.Sprintf("archive: stored record for height %d failed verification", height), outcomeVerifyFailed}
}

func errParams(format string, args ...any) *rpcError {
	return &rpcError{codeInvalidParams, "invalid params: " + fmt.Sprintf(format, args...), outcomeBadRequest}
}

func errBatch(max int) *rpcError {
	return &rpcError{codeInvalidRequest, fmt.Sprintf("invalid request: more than %d calls in a batch", max), outcomeBadRequest}
}

// errMethod names the method only if it looks like one, so a client cannot
// make the message say anything (e.g. "not found").
func errMethod(name string) *rpcError {
	if !methodName.MatchString(name) {
		name = "(invalid name)"
	}
	return &rpcError{codeNoMethod, fmt.Sprintf("unsupported method %s: the archive API serves "+
		"blob.Get, blob.GetAll, blob.GetProof, blob.Included, header.NetworkHead, header.GetByHeight", name), outcomeBadRequest}
}
