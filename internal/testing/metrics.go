package testing

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func WithTestRegisterer(t *testing.T, fn func(reg prometheus.Registerer)) {
	reg := prometheus.NewRegistry()
	metrics.WithTestRegisterer(reg, func() {
		fn(reg)
	})
}
