package fibrecapture

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
)

// Phase is how far an item has progressed through the write order.
type Phase string

const (
	PhasePending Phase = "pending" // not yet downloaded and verified
	PhaseSpooled Phase = "spooled" // verified bytes on disk in spool/
	PhaseFinal   Phase = "final"   // record written, index pending
)

const (
	stateVersion  = 1
	stateName     = "state.json"
	stateTmpName  = "state.json.tmp"
	spoolDirName  = "spool"
	spoolTmpExt   = ".tmp"
	stateFileMode = 0o600
)

// Entry is one item the capture has not yet dropped from its state. The
// embedded Item uses Go's default JSON encoding (byte arrays as number
// arrays): a custom Item.MarshalJSON would be promoted to Entry and hide the
// Entry fields.
type Entry struct {
	Item
	Phase       Phase               `json:"phase"`
	Status      fibrearchive.Status `json:"status,omitempty"` // set once decided; the entry is final once its record is written
	ScannedAt   time.Time           `json:"scanned_at"`       // the retry schedule counts from here
	Deadline    time.Time           `json:"deadline"`
	Attempts    int                 `json:"attempts"`
	NextAttempt time.Time           `json:"next_attempt"`
	DataSize    uint64              `json:"data_size,omitempty"`
	DataSHA256  []byte              `json:"data_sha256,omitempty"`
	LastErr     string              `json:"last_err,omitempty"`
}

// State is the capture's durable local state, persisted as state.json.
type State struct {
	Version      int    `json:"version"`
	ChainID      string `json:"chain_id"`
	FirstHeight  int64  `json:"first_height"`
	ScannedUntil int64  `json:"scanned_until"`
	// FrozenThrough is the watermark below which every index bucket is
	// frozen (or left open below first_height); -1 for none, also when the
	// field is absent. Frozen lists the frozen buckets above it, sorted.
	FrozenThrough int64             `json:"frozen_through"`
	Frozen        []int64           `json:"frozen,omitempty"`
	Day           string            `json:"day"` // YYYY-MM-DD UTC
	DayBytes      uint64            `json:"day_bytes"`
	Counts        map[string]uint64 `json:"counts"`
	BytesArchived uint64            `json:"bytes_archived"`
	Entries       map[string]*Entry `json:"entries"` // by record key
}

// UnmarshalJSON decodes a state, defaulting a missing frozen_through to -1
// so that bucket 0 is never taken as frozen.
func (s *State) UnmarshalJSON(b []byte) error {
	type plain State
	p := plain{FrozenThrough: -1}
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*s = State(p)
	return nil
}

// fsys is the seam through which Dir writes files, so tests can inject
// failures. writeFile must create, write and fsync the file before returning.
type fsys struct {
	writeFile func(name string, data []byte, perm os.FileMode) error
	rename    func(oldpath, newpath string) error
}

var osFS = fsys{writeFile: writeFileSync, rename: os.Rename}

func writeFileSync(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Dir is the capture's state directory: state.json and the spool/ of
// verified payloads awaiting upload.
type Dir struct {
	path string
	fs   fsys
}

// OpenDir opens an existing, writable state directory and creates its
// spool/ (mode 0700).
func OpenDir(path string) (*Dir, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("state dir: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("state dir %s: not a directory", path)
	}
	probe, err := os.CreateTemp(path, ".probe-*")
	if err != nil {
		return nil, fmt.Errorf("state dir %s not writable: %w", path, err)
	}
	_ = probe.Close()
	if err := os.Remove(probe.Name()); err != nil {
		return nil, fmt.Errorf("state dir probe: %w", err)
	}
	spool := filepath.Join(path, spoolDirName)
	if err := os.MkdirAll(spool, 0o700); err != nil {
		return nil, fmt.Errorf("create spool: %w", err)
	}
	if err := os.Chmod(spool, 0o700); err != nil {
		return nil, fmt.Errorf("chmod spool: %w", err)
	}
	return &Dir{path: path, fs: osFS}, nil
}

// Load reads state.json. A missing file gives nil, nil; a corrupt or
// unknown-version file is an error, never an empty state.
func (d *Dir) Load() (*State, error) {
	b, err := os.ReadFile(filepath.Join(d.path, stateName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if s.Version != stateVersion {
		return nil, fmt.Errorf("state version %d: want %d", s.Version, stateVersion)
	}
	for k, e := range s.Entries {
		if e == nil {
			return nil, fmt.Errorf("decode state: entry %q is null", k)
		}
	}
	return &s, nil
}

// Save atomically replaces state.json: tmp write with fsync, rename, then
// fsync of the directory. On failure the previous state.json is unchanged.
// A zero Version is set to the current one; any other mismatch is refused,
// so Save never writes a file Load would reject.
func (d *Dir) Save(s *State) error {
	if s.Version == 0 {
		s.Version = stateVersion
	}
	if s.Version != stateVersion {
		return fmt.Errorf("save state: version %d: want %d", s.Version, stateVersion)
	}
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := filepath.Join(d.path, stateTmpName)
	if err := d.replace(tmp, filepath.Join(d.path, stateName), b, stateFileMode); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	if err := syncDir(d.path); err != nil {
		return fmt.Errorf("sync state dir: %w", err)
	}
	return nil
}

// replace writes data to tmp and renames it over dst, removing tmp on failure.
func (d *Dir) replace(tmp, dst string, data []byte, perm os.FileMode) error {
	if err := d.fs.writeFile(tmp, data, perm); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := d.fs.rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (d *Dir) spoolPath(id [33]byte) string {
	return filepath.Join(d.path, spoolDirName, hex.EncodeToString(id[:]))
}

// SpoolWrite durably stores a verified payload as spool/<hex blob id>.
func (d *Dir) SpoolWrite(id [33]byte, data []byte) error {
	dst := d.spoolPath(id)
	if err := d.replace(dst+spoolTmpExt, dst, data, stateFileMode); err != nil {
		return fmt.Errorf("spool write: %w", err)
	}
	if err := syncDir(filepath.Join(d.path, spoolDirName)); err != nil {
		return fmt.Errorf("sync spool: %w", err)
	}
	return nil
}

// SpoolRead returns a spooled payload; a missing one wraps fs.ErrNotExist.
func (d *Dir) SpoolRead(id [33]byte) ([]byte, error) {
	b, err := os.ReadFile(d.spoolPath(id))
	if err != nil {
		return nil, fmt.Errorf("spool read: %w", err)
	}
	return b, nil
}

// SpoolRemove deletes a spooled payload; a missing one is not an error.
func (d *Dir) SpoolRemove(id [33]byte) error {
	if err := os.Remove(d.spoolPath(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("spool remove: %w", err)
	}
	return nil
}

// SpoolSweep removes leftover spool/*.tmp files and every payload whose blob
// ID is not in keep. Files with other names are left alone.
func (d *Dir) SpoolSweep(keep map[[33]byte]bool) error {
	dir := filepath.Join(d.path, spoolDirName)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("spool list: %w", err)
	}
	var errs []error
	for _, e := range ents {
		name := e.Name()
		if !e.Type().IsRegular() {
			continue
		}
		if !strings.HasSuffix(name, spoolTmpExt) {
			raw, err := hex.DecodeString(name)
			if err != nil || len(raw) != len([33]byte{}) || keep[[33]byte(raw)] {
				continue
			}
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("spool sweep: %w", err))
		}
	}
	return errors.Join(errs...)
}

// SpoolBytes is the total size of the files in spool/.
func (d *Dir) SpoolBytes() (int64, error) {
	ents, err := os.ReadDir(filepath.Join(d.path, spoolDirName))
	if err != nil {
		return 0, fmt.Errorf("spool list: %w", err)
	}
	var n int64
	for _, e := range ents {
		if !e.Type().IsRegular() {
			continue
		}
		fi, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed concurrently
		}
		if err != nil {
			return 0, fmt.Errorf("spool stat: %w", err)
		}
		n += fi.Size()
	}
	return n, nil
}
