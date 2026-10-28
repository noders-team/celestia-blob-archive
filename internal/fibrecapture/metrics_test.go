package fibrecapture

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestMetricsSeriesExportedAtZero: increase() over a counter series that
// first appears at 1 returns 0, so every labelled series the alerts use must
// exist at 0 from the start.
func TestMetricsSeriesExportedAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	for name, want := range map[string]int{
		"cba_fibre_records_total":           2 * 5,
		"cba_fibre_download_attempts_total": 6,
	} {
		if n, err := testutil.GatherAndCount(reg, name); err != nil || n != want {
			t.Errorf("%s: %d series (err %v), want %d", name, n, err, want)
		}
	}
}
