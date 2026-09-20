package authorization

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	decisionCounter    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_authorization_decisions_total", Help: "Authorization decisions by backend and effect."}, []string{"backend", "effect"})
	evaluationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "pithos_authorization_evaluation_duration_seconds", Help: "Time spent evaluating authorization requests."}, []string{"backend"})
	reloadCounter      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_authorization_reload_total", Help: "Authorization snapshot reload attempts."}, []string{"backend", "success"})
	snapshotAge        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pithos_authorization_snapshot_age_seconds", Help: "Age of the active authorization snapshot."}, []string{"backend"})
)

func init() { prometheus.MustRegister(decisionCounter, evaluationDuration, reloadCounter, snapshotAge) }
func ObserveDecision(backend string, effect Effect, started time.Time) {
	decisionCounter.WithLabelValues(backend, string(effect)).Inc()
	evaluationDuration.WithLabelValues(backend).Observe(time.Since(started).Seconds())
}
func ObserveReload(backend string, success bool) {
	reloadCounter.WithLabelValues(backend, strconv.FormatBool(success)).Inc()
}
func SetSnapshotAge(backend string, age time.Duration) {
	snapshotAge.WithLabelValues(backend).Set(age.Seconds())
}
