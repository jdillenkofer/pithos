package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	mu                sync.Mutex
	currentRegisterer prometheus.Registerer
	registered        = map[prometheus.Collector]struct{}{}
	testMu            sync.Mutex
)

// Register strictly registers each collector once with the current default registerer.
func Register(collectors ...prometheus.Collector) {
	mu.Lock()
	defer mu.Unlock()
	if currentRegisterer != prometheus.DefaultRegisterer {
		currentRegisterer = prometheus.DefaultRegisterer
		registered = map[prometheus.Collector]struct{}{}
	}
	for _, collector := range collectors {
		if _, ok := registered[collector]; ok {
			continue
		}
		prometheus.DefaultRegisterer.MustRegister(collector)
		registered[collector] = struct{}{}
	}
}

// WithTestRegisterer temporarily swaps the global registerer for an isolated test.
func WithTestRegisterer(registerer prometheus.Registerer, fn func()) {
	testMu.Lock()
	defer testMu.Unlock()
	previous := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = registerer
	defer func() { prometheus.DefaultRegisterer = previous }()
	fn()
}
