package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
)

// Config holds the request limits and reload intervals; zero fields take
// the defaults.
type Config struct {
	// ChainID is the chain the archive must hold (required).
	ChainID string
	// MaxBodyBytes bounds a request body (default 1 MiB).
	MaxBodyBytes int64
	// MaxBatch bounds the calls of a batch (default 16); MaxNamespaces the
	// namespaces of one blob.GetAll (default 16).
	MaxBatch, MaxNamespaces int
	// RequestTimeout bounds one call (default 30 s).
	RequestTimeout time.Duration
	// MaxInflightBytes bounds the memory held by blob calls across
	// requests (default 1 GiB); a call reserves 3× the segment bytes it
	// reads (see reserveFactor).
	MaxInflightBytes int64
	// ManifestPoll and TombstonePoll are the reload intervals of
	// manifest.json (default 6 s) and tombstones.json (default 1 m).
	ManifestPoll, TombstonePoll time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 16
	}
	if c.MaxNamespaces <= 0 {
		c.MaxNamespaces = 16
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MaxInflightBytes <= 0 {
		c.MaxInflightBytes = 1 << 30
	}
	if c.ManifestPoll <= 0 {
		c.ManifestPoll = 6 * time.Second
	}
	if c.TombstonePoll <= 0 {
		c.TombstonePoll = time.Minute
	}
	return c
}

// archiveState is the archive state the server keeps in the background:
// the loaded manifest and tombstone set (liveState, set by Start).
type archiveState interface {
	// heights returns manifest.highest_height and manifest.contiguous_until.
	heights() (highest, contiguousUntil int64)
	// tombstones returns the loaded tombstone set (never nil).
	tombstones() *tombstone.Set
	// reloadTombstones reloads tombstones.json now, rate-limited; it is
	// called when the archive holds a tombstone record the set does not
	// list. A failure keeps the previous set.
	reloadTombstones(ctx context.Context)
	// manifestAge and tombstonesAge return the time since manifest.json
	// and tombstones.json were last loaded.
	manifestAge() time.Duration
	tombstonesAge() time.Duration
}

// stateBox holds an archiveState in an atomic.Pointer.
type stateBox struct{ archiveState }

// Server answers celestia-node's blob JSON-RPC methods from the archive.
type Server struct {
	st    store.Store
	cfg   Config
	m     *Metrics
	log   *slog.Logger
	sem   *semaphore.Weighted
	clock clock

	cur     atomic.Pointer[stateBox] // nil until Start has loaded the state
	started atomic.Bool

	handlers map[string]method
}

// state returns the loaded archive state, or nil before Start. It is safe
// from any goroutine.
func (s *Server) state() archiveState {
	if b := s.cur.Load(); b != nil {
		return b.archiveState
	}
	return nil
}

func (s *Server) setState(a archiveState) { s.cur.Store(&stateBox{a}) }

// New returns a server reading the archive from st. m and log may be nil.
func New(st store.Store, cfg Config, m *Metrics, log *slog.Logger) (*Server, error) {
	if st == nil {
		return nil, errors.New("api: no store")
	}
	if cfg.ChainID == "" {
		return nil, errors.New("api: chain id is required")
	}
	cfg = cfg.withDefaults()
	if m == nil {
		m = NewMetrics(prometheus.NewRegistry())
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Server{st: st, cfg: cfg, m: m, log: log, sem: semaphore.NewWeighted(cfg.MaxInflightBytes), clock: realClock{}}
	s.handlers = s.methodTable()
	return s, nil
}

// Handler serves JSON-RPC 2.0 over HTTP POST on / and the health check on
// GET /health. Before Start it answers calls as upstream unavailable and
// /health with 503.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				s.httpReject(w, "use HTTP GET", http.StatusMethodNotAllowed)
				return
			}
			s.serveHealth(w)
			return
		}
		if r.URL.Path != "/" {
			// Not http.NotFound: no reply but "blob: not found" may say
			// "not found".
			s.httpReject(w, "unknown path; the archive API serves POST /", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodGet && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			s.httpReject(w, "websocket is not supported; use HTTP POST", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			s.httpReject(w, "use HTTP POST", http.StatusMethodNotAllowed)
			return
		}
		s.serveRPC(w, r)
	})
}

// httpReject answers a request refused before JSON-RPC and counts it as
// method="http", outcome="bad_request".
func (s *Server) httpReject(w http.ResponseWriter, msg string, code int) {
	s.m.request(methodHTTP, outcomeBadRequest, 0)
	http.Error(w, msg, code)
}

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"` // nil: a notification; "null" is an id
	Method  string            `json:"method"`
	Params  json.RawMessage   `json:"params"`
	params  []json.RawMessage // Params as a positional list
}

type rpcReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	err     *rpcError       // the error behind Error, for metrics
}

var nullID = json.RawMessage("null")

func (s *Server) serveRPC(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			s.httpReject(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		s.httpReject(w, "read request body", http.StatusBadRequest)
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) > 0 && body[0] == '[' {
		var raws []json.RawMessage
		if err := json.Unmarshal(body, &raws); err != nil {
			s.reject(w, errParse)
			return
		}
		switch {
		case len(raws) == 0:
			s.reject(w, errEmptyBatch)
			return
		case len(raws) > s.cfg.MaxBatch:
			s.reject(w, errBatch(s.cfg.MaxBatch))
			return
		}
		out := make([]*rpcReply, len(raws))
		// Every call keeps its in-flight bytes until the reply is written.
		releases := make([]func(), len(raws))
		defer func() {
			for _, release := range releases {
				if release != nil {
					release()
				}
			}
		}()
		var wg sync.WaitGroup
		for i, raw := range raws {
			wg.Go(func() { out[i], releases[i] = s.handle(r.Context(), raw) })
		}
		wg.Wait()
		var resps []json.RawMessage
		for i, o := range out {
			if o != nil {
				resps = append(resps, encodeReply(o))
				out[i] = nil // the encoded reply replaces the result
			}
		}
		if len(resps) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeBatch(w, resps)
		return
	}
	if !json.Valid(body) {
		s.reject(w, errParse)
		return
	}
	resp, release := s.handle(r.Context(), body)
	defer release()
	if resp == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, resp)
}

// reject answers a request that runs no call.
func (s *Server) reject(w http.ResponseWriter, e *rpcError) {
	s.m.request(methodOther, e.outcome, 0)
	writeJSON(w, errResponse(nullID, e))
}

// handle answers one call, or returns nil for a notification. A
// notification is not run: every method is a read without side effects.
// A panic in the call is answered as upstream unavailable, counted and
// logged; batch calls run in their own goroutines, where net/http would not
// recover it and the whole process would die.
//
// release returns the call's in-flight bytes, also after a panic; the
// caller runs it once the reply is written (never nil).
func (s *Server) handle(ctx context.Context, raw json.RawMessage) (reply *rpcReply, release func()) {
	release = func() {}
	var req rpcRequest
	// label and start are set once the call runs a served method.
	label := methodOther
	var start time.Time
	defer func() {
		if p := recover(); p != nil {
			s.m.panicked()
			var height json.RawMessage
			if len(req.params) > 0 {
				height = req.params[0]
				if len(height) > 32 {
					height = height[:32]
				}
			}
			s.log.Error("call panicked", "method", req.Method, "height", string(height),
				"panic", p, "stack", string(debug.Stack()))
			id := req.ID
			if id == nil {
				id = nullID
			}
			reply = errResponse(id, errUpstream)
		}
		if reply == nil {
			return // a notification
		}
		outcome := outcomeOK
		if reply.err != nil {
			outcome = reply.err.outcome
		}
		var d time.Duration
		if !start.IsZero() {
			d = max(time.Since(start), time.Nanosecond)
		}
		s.m.request(label, outcome, d)
	}()
	if err := json.Unmarshal(raw, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
		id := nullID
		if err == nil && req.ID != nil {
			id = req.ID
		}
		return errResponse(id, errBadRequest), release
	}
	if req.ID == nil {
		return nil, release
	}
	m, ok := s.handlers[req.Method]
	if !ok {
		return errResponse(req.ID, errMethod(req.Method)), release
	}
	label, start = req.Method, time.Now()
	if len(req.Params) > 0 && !bytes.Equal(req.Params, nullID) {
		if err := json.Unmarshal(req.Params, &req.params); err != nil {
			return errResponse(req.ID, errParams("params must be a positional array")), release
		}
	}
	if len(req.params) != m.nparams {
		return errResponse(req.ID, errParams("%s takes %d params, got %d", req.Method, m.nparams, len(req.params))), release
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	if s.state() == nil {
		s.log.Error("archive state not loaded", "method", req.Method)
		return errResponse(req.ID, errUpstream), release
	}
	var reserve reserveFunc
	reserve, release = s.reserver(ctx)
	res, rerr := m.call(ctx, req.params, reserve)
	if rerr != nil {
		return errResponse(req.ID, rerr), release
	}
	return &rpcReply{JSONRPC: "2.0", ID: req.ID, Result: res}, release
}

// reserveFunc holds n bytes of MaxInflightBytes for a call (see reserver).
type reserveFunc = func(n int64) (release func(), err error)

type method struct {
	nparams int
	// call returns the encoded result. The bytes it reserves with reserve
	// stay held until the HTTP reply holding the result is written.
	call func(ctx context.Context, params []json.RawMessage, reserve reserveFunc) (json.RawMessage, *rpcError)
}

func (s *Server) methodTable() map[string]method {
	return map[string]method{
		"blob.Get":      {3, s.blobGet},
		"blob.GetAll":   {2, s.blobGetAll},
		"blob.GetProof": {3, s.blobGetProof},
		"blob.Included": {4, s.blobIncluded},

		"header.GetByHeight": {1, s.headerGetByHeight},
		"header.NetworkHead": {0, s.headerNetworkHead},
	}
}

func errResponse(id json.RawMessage, e *rpcError) *rpcReply {
	return &rpcReply{JSONRPC: "2.0", ID: id, Error: e.wire(), err: e}
}

// encodeReply returns the JSON of r, or of an upstream unavailable reply
// for its id if r cannot be encoded.
func encodeReply(r *rpcReply) json.RawMessage {
	b, err := json.Marshal(r)
	if err != nil {
		b, _ = json.Marshal(errResponse(r.ID, errUpstream))
	}
	return b
}

// writeBatch writes the encoded replies as a JSON array, piece by piece, so
// that the replies are not copied into one more buffer.
func writeBatch(w http.ResponseWriter, replies []json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	sep := []byte{'['}
	for _, r := range replies {
		if _, err := w.Write(sep); err != nil {
			return
		}
		if _, err := w.Write(r); err != nil {
			return
		}
		sep = []byte{','}
	}
	_, _ = w.Write([]byte{']'})
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
