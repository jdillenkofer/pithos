package authentication

import (
	"sync"
	"time"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var credentialSnapshotMetricsOnce sync.Once
var credentialSnapshotReloadSuccesses *prometheus.CounterVec
var credentialSnapshotReloadFailures *prometheus.CounterVec
var credentialSnapshotAge *snapshotAgeCollector

type snapshotAgeCollector struct {
	description *prometheus.Desc
	mu          sync.RWMutex
	lastSuccess map[string]time.Time
}

func newSnapshotAgeCollector() *snapshotAgeCollector {
	return &snapshotAgeCollector{
		description: prometheus.NewDesc(
			"pithos_authentication_credentials_snapshot_age_seconds",
			"Age of the last successfully loaded credential snapshot",
			[]string{"provider"}, nil,
		),
		lastSuccess: make(map[string]time.Time),
	}
}

func (c *snapshotAgeCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.description
}

func (c *snapshotAgeCollector) Collect(metrics chan<- prometheus.Metric) {
	now := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for provider, lastSuccess := range c.lastSuccess {
		metrics <- prometheus.MustNewConstMetric(c.description, prometheus.GaugeValue, now.Sub(lastSuccess).Seconds(), provider)
	}
}

func (c *snapshotAgeCollector) recordSuccess(provider string) {
	c.mu.Lock()
	c.lastSuccess[provider] = time.Now()
	c.mu.Unlock()
}

func registerCredentialSnapshotMetrics() {
	credentialSnapshotMetricsOnce.Do(func() {
		credentialSnapshotReloadSuccesses = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "authentication", Name: "credentials_snapshot_reload_successes_total", Help: "Number of successful background credential snapshot reloads"}, []string{"provider"})
		credentialSnapshotReloadFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "authentication", Name: "credentials_snapshot_reload_failures_total", Help: "Number of failed background credential snapshot reloads"}, []string{"provider"})
		credentialSnapshotAge = newSnapshotAgeCollector()
	})
	pithosmetrics.Register(credentialSnapshotReloadSuccesses, credentialSnapshotReloadFailures, credentialSnapshotAge)
}

func observeCredentialSnapshotLoaded(provider string) {
	registerCredentialSnapshotMetrics()
	credentialSnapshotAge.recordSuccess(provider)
}

func observeCredentialSnapshotReload(provider string, success bool) {
	registerCredentialSnapshotMetrics()
	if success {
		credentialSnapshotReloadSuccesses.WithLabelValues(provider).Inc()
		credentialSnapshotAge.recordSuccess(provider)
	} else {
		credentialSnapshotReloadFailures.WithLabelValues(provider).Inc()
	}
}
