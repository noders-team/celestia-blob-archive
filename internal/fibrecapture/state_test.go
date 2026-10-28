package fibrecapture

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
)

func testState() *State {
	var id [33]byte
	for i := range id {
		id[i] = byte(i + 1)
	}
	var ns [29]byte
	for i := range ns {
		ns[i] = byte(0xa0 + i)
	}
	it := Item{
		Height: 1234, TxIndex: 2, MsgIndex: 1, Kind: fibrearchive.KindPayForFibre,
		Tx: []byte{0xde, 0xad}, BlockTime: time.Date(2026, 10, 27, 10, 0, 0, 5, time.UTC),
		BlobID: id, Namespace: ns, BlobSize: 4096, BlobVersion: 0, PromiseHeight: 1230,
		Creation: time.Date(2026, 10, 27, 9, 59, 0, 0, time.UTC),
	}
	bad := Item{Height: 1235, Kind: fibrearchive.KindTimeout, Err: "malformed promise"}
	return &State{
		Version: 1, ChainID: "mocha-4", FirstHeight: 1000, ScannedUntil: 1240,
		Day: "2026-10-27", DayBytes: 4096,
		Counts:        map[string]uint64{"archived": 3, "expired": 1},
		BytesArchived: 12288,
		Entries: map[string]*Entry{
			it.Key(): {
				Item: it, Phase: PhaseSpooled, Attempts: 2,
				Deadline:    time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
				NextAttempt: time.Date(2026, 10, 27, 10, 5, 0, 0, time.UTC),
				DataSize:    4000, DataSHA256: bytes.Repeat([]byte{7}, 32), LastErr: "timeout",
			},
			bad.Key(): {Item: bad, Phase: PhaseFinal, Status: fibrearchive.StatusUnrecoverable},
		},
	}
}

func TestStateRoundTrip(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s, err := d.Load(); err != nil || s != nil {
		t.Fatalf("Load on empty dir = %v, %v; want nil, nil", s, err)
	}
	want := testState()
	if err := d.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := d.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	if _, err := os.Stat(filepath.Join(d.path, stateTmpName)); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}
}

func TestStateSaveIsAtomic(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	prev := testState()
	if err := d.Save(prev); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(d.path, stateName)
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}

	next := testState()
	next.ScannedUntil = 9999
	next.Entries = nil
	injected := errors.New("injected rename failure")
	d.fs.rename = func(string, string) error { return injected }
	if err := d.Save(next); !errors.Is(err, injected) {
		t.Fatalf("Save err = %v; want injected failure", err)
	}
	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("state.json changed by a failed Save")
	}
	d.fs = osFS
	got, err := d.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, prev) {
		t.Fatal("previous state not loadable after a failed Save")
	}

	// A failed tmp write leaves state.json untouched too.
	d.fs.writeFile = func(string, []byte, os.FileMode) error { return injected }
	if err := d.Save(next); !errors.Is(err, injected) {
		t.Fatalf("Save err = %v; want injected failure", err)
	}
	d.fs = osFS
	if after, _ := os.ReadFile(statePath); !bytes.Equal(before, after) {
		t.Fatal("state.json changed by a failed tmp write")
	}

	// A truncated state.json is an error, never an empty state.
	if err := os.WriteFile(statePath, before[:len(before)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	if s, err := d.Load(); err == nil {
		t.Fatalf("Load of truncated state = %+v, nil; want error", s)
	}
	for name, body := range map[string]string{
		"empty":     "",
		"version 2": `{"version":2,"chain_id":"mocha-4"}`,
		"version 0": `{"chain_id":"mocha-4"}`,
	} {
		if err := os.WriteFile(statePath, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if s, err := d.Load(); err == nil {
			t.Fatalf("%s: Load = %+v, nil; want error", name, s)
		}
	}
}

func TestStateSaveEnforcesVersion(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := testState()
	s.Version = 0
	if err := d.Save(s); err != nil {
		t.Fatalf("Save with version 0: %v", err)
	}
	got, err := d.Load()
	if err != nil {
		t.Fatalf("Load after saving version 0: %v", err)
	}
	if got.Version != stateVersion {
		t.Fatalf("loaded version = %d; want %d", got.Version, stateVersion)
	}

	statePath := filepath.Join(d.path, stateName)
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	bad := testState()
	bad.Version = 2
	if err := d.Save(bad); err == nil {
		t.Fatal("Save with version 2: want error")
	}
	if after, _ := os.ReadFile(statePath); !bytes.Equal(before, after) {
		t.Fatal("state.json changed by a rejected Save")
	}
}

func TestStateLoadRejectsNilEntry(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := `{"version":1,"chain_id":"mocha-4","entries":{"000000001234-00002-001.pb":null}}`
	if err := os.WriteFile(filepath.Join(d.path, stateName), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := d.Load()
	if err == nil {
		t.Fatalf("Load with a null entry = %+v, nil; want error", s)
	}
	if !strings.Contains(err.Error(), "000000001234-00002-001.pb") {
		t.Fatalf("error %q does not name the entry", err)
	}
}

func TestOpenDirRefusesBadPath(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDir(file); err == nil {
		t.Fatal("OpenDir on a regular file: want error")
	}
	if _, err := OpenDir(filepath.Join(base, "missing")); err == nil {
		t.Fatal("OpenDir on a missing path: want error")
	}
	if os.Geteuid() != 0 {
		ro := filepath.Join(base, "ro")
		if err := os.Mkdir(ro, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
		if _, err := OpenDir(ro); err == nil {
			t.Fatal("OpenDir on a read-only directory: want error")
		}
	}

	d, err := OpenDir(base)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(d.path, spoolDirName))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Fatalf("spool mode = %v; want dir 0700", fi.Mode())
	}
}

func TestSpoolRoundTripAndRemove(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var id [33]byte
	id[0], id[32] = 0, 0xab
	data := []byte("fibre blob payload")
	if err := d.SpoolWrite(id, data); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(d.path, spoolDirName, strings.Repeat("0", 64)+"ab")
	if _, err := os.Stat(name); err != nil {
		t.Fatalf("spool file not at %s: %v", name, err)
	}
	got, err := d.SpoolRead(id)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("SpoolRead = %q, %v", got, err)
	}
	if err := d.SpoolRemove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SpoolRead(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SpoolRead after remove err = %v; want ErrNotExist", err)
	}
	if err := d.SpoolRemove(id); err != nil {
		t.Fatalf("SpoolRemove of missing = %v; want nil", err)
	}

	// A failed rename leaves no spool file and no tmp file.
	injected := errors.New("injected")
	d.fs.rename = func(string, string) error { return injected }
	if err := d.SpoolWrite(id, data); !errors.Is(err, injected) {
		t.Fatalf("SpoolWrite err = %v; want injected", err)
	}
	d.fs = osFS
	ents, err := os.ReadDir(filepath.Join(d.path, spoolDirName))
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("spool not empty after failed write: %v", ents)
	}
}

func TestSpoolBytes(t *testing.T) {
	d, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := d.SpoolBytes(); err != nil || n != 0 {
		t.Fatalf("SpoolBytes empty = %d, %v", n, err)
	}
	var a, b [33]byte
	a[1], b[1] = 1, 2
	if err := d.SpoolWrite(a, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := d.SpoolWrite(b, make([]byte, 23)); err != nil {
		t.Fatal(err)
	}
	if n, err := d.SpoolBytes(); err != nil || n != 123 {
		t.Fatalf("SpoolBytes = %d, %v; want 123", n, err)
	}
	if err := d.SpoolRemove(a); err != nil {
		t.Fatal(err)
	}
	if n, err := d.SpoolBytes(); err != nil || n != 23 {
		t.Fatalf("SpoolBytes after remove = %d, %v; want 23", n, err)
	}
}
