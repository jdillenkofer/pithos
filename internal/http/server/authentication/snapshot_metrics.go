package authentication

import (
	"sync"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var credentialSnapshotMetricsOnce sync.Once
var credentialSnapshotReloadSuccesses *prometheus.CounterVec
var credentialSnapshotReloadFailures *prometheus.CounterVec
var credentialSnapshotAge *prometheus.GaugeVec

func registerCredentialSnapshotMetrics() {
	credentialSnapshotMetricsOnce.Do(func() {
		credentialSnapshotReloadSuccesses = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "authentication", Name: "credentials_snapshot_reload_successes_total", Help: "Number of successful background credential snapshot reloads"}, []string{"provider"})
		credentialSnapshotReloadFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "authentication", Name: "credentials_snapshot_reload_failures_total", Help: "Number of failed background credential snapshot reloads"}, []string{"provider"})
		credentialSnapshotAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "authentication", Name: "credentials_snapshot_age_seconds", Help: "Age of the last successfully loaded credential snapshot"}, []string{"provider"})
	})
	pithosmetrics.Register(credentialSnapshotReloadSuccesses, credentialSnapshotReloadFailures, credentialSnapshotAge)
}

func observeCredentialSnapshotAge(provider string, ageSeconds float64) {
	registerCredentialSnapshotMetrics()
	credentialSnapshotAge.WithLabelValues(provider).Set(ageSeconds)
}

func observeCredentialSnapshotReload(provider string, success bool, ageSeconds float64) {
	registerCredentialSnapshotMetrics()
	if success {
		credentialSnapshotReloadSuccesses.WithLabelValues(provider).Inc()
	} else {
		credentialSnapshotReloadFailures.WithLabelValues(provider).Inc()
	}
	credentialSnapshotAge.WithLabelValues(provider).Set(ageSeconds)
}
