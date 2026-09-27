package authorization

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSnapshotAgeAdvancesWithoutAuthorizationTraffic(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(snapshotAgeMetric))
	SetSnapshotLoadedAt("test", time.Now().Add(-time.Second))

	first := snapshotAgeForBackend(t, registry, "test")
	time.Sleep(20 * time.Millisecond)
	second := snapshotAgeForBackend(t, registry, "test")

	require.Greater(t, second, first)
}

func snapshotAgeForBackend(t *testing.T, gatherer prometheus.Gatherer, backend string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "pithos_authorization_snapshot_age_seconds" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "backend" && label.GetValue() == backend {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("snapshot age metric for backend %q not found", backend)
	return 0
}
