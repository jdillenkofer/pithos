package cache

import (
	"sync"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var cacheMetricsOnce sync.Once
var cacheHits, cacheMisses, cacheEvictions *prometheus.CounterVec

func registerCacheMetrics() {
	cacheMetricsOnce.Do(func() {
		cacheHits = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "cache", Name: "hits_total", Help: "Number of cache hits"}, []string{"store"})
		cacheMisses = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "cache", Name: "misses_total", Help: "Number of cache misses"}, []string{"store"})
		cacheEvictions = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "cache", Name: "evictions_total", Help: "Number of cache evictions"}, []string{"store"})
	})
	pithosmetrics.Register(cacheHits, cacheMisses, cacheEvictions)
}

func ObserveHit(store string)  { registerCacheMetrics(); cacheHits.WithLabelValues(store).Inc() }
func ObserveMiss(store string) { registerCacheMetrics(); cacheMisses.WithLabelValues(store).Inc() }
func ObserveEvictions(store string, count int) {
	registerCacheMetrics()
	cacheEvictions.WithLabelValues(store).Add(float64(count))
}
