package archiveread

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// fibreSection returns the "## Fibre" section of docs/format.md, up to the
// next level-2 heading.
func fibreSection(t *testing.T) string {
	t.Helper()
	doc, err := os.ReadFile("../../docs/format.md")
	if err != nil {
		t.Fatal(err)
	}
	_, sec, ok := strings.Cut(string(doc), "\n## Fibre\n")
	if !ok {
		t.Fatal("docs/format.md has no ## Fibre section")
	}
	if i := strings.Index(sec, "\n## "); i >= 0 {
		sec = sec[:i]
	}
	return sec
}

// tableAfter returns the cells of the first markdown table that follows the
// line containing marker in sec, header and separator rows dropped.
func tableAfter(t *testing.T, sec, marker string) [][]string {
	t.Helper()
	_, rest, ok := strings.Cut(sec, marker)
	if !ok {
		t.Fatalf("Fibre section has no %q", marker)
	}
	var rows [][]string
	started := false
	for _, l := range strings.Split(rest, "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "|") {
			if started {
				break
			}
			continue
		}
		started = true
		var cells []string
		for _, c := range strings.Split(strings.Trim(l, "|"), "|") {
			cells = append(cells, strings.TrimSpace(c))
		}
		rows = append(rows, cells)
	}
	if len(rows) < 3 {
		t.Fatalf("table after %q has %d rows", marker, len(rows))
	}
	return rows[2:]
}

func normalizeSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// TestFormatDocFibreKeys pins the Fibre section of docs/format.md: the key
// patterns, the data statuses and the FibreRecord proto, which must be the
// file proto/archive/v1/fibre.proto verbatim (up to whitespace).
func TestFormatDocFibreKeys(t *testing.T) {
	sec := fibreSection(t)

	var keys []string
	for _, r := range tableAfter(t, sec, "### Keys") {
		keys = append(keys, r[0])
	}
	wantKeys := []string{
		"`fibre/data/<blob_id_hex(66)>`",
		"`fibre/records/<height:012d>-<tx_index:05d>-<msg_index:03d>.pb`",
		"`fibre/index/<b:08d>.json`",
		"`fibre/manifest.json`",
	}
	if !slices.Equal(keys, wantKeys) {
		t.Errorf("Fibre key table names %q, want %q", keys, wantKeys)
	}

	var statuses []string
	for _, r := range tableAfter(t, sec, "### Data status") {
		statuses = append(statuses, r[0]+" "+r[1])
	}
	wantStatuses := []string{"1 `archived`", "2 `expired`", "3 `unrecoverable`", "4 `withdrawn`", "5 `skipped_cap`"}
	if !slices.Equal(statuses, wantStatuses) {
		t.Errorf("status table lists %q, want %q", statuses, wantStatuses)
	}

	m := regexp.MustCompile("(?s)```proto\n(.*?)```").FindAllStringSubmatch(sec, -1)
	if len(m) != 1 {
		t.Fatalf("Fibre section has %d ```proto blocks, want 1", len(m))
	}
	file, err := os.ReadFile("../../proto/archive/v1/fibre.proto")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := normalizeSpace(m[0][1]), normalizeSpace(string(file)); got != want {
		t.Errorf("proto block in docs/format.md differs from proto/archive/v1/fibre.proto:\ndoc:  %s\nfile: %s", got, want)
	}
}
