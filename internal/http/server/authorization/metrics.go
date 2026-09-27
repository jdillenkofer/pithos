package authorization

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	decisionCounter    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_authorization_decisions_total", Help: "Authorization decisions by backend and effect."}, []string{"backend", "effect"})
	evaluationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "pithos_authorization_evaluation_duration_seconds", Help: "Time spent evaluating authorization requests."}, []string{"backend"})
	reloadCounter      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_authorization_reload_total", Help: "Authorization snapshot reload attempts."}, []string{"backend", "success"})
	snapshotLoadedAt   sync.Map
	snapshotAgeMetric  = &snapshotAgeCollector{desc: prometheus.NewDesc("pithos_authorization_snapshot_age_seconds", "Age of the active authorization snapshot.", []string{"backend"}, nil)}
)

type snapshotAgeCollector struct {
	desc *prometheus.Desc
}

func (c *snapshotAgeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *snapshotAgeCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	snapshotLoadedAt.Range(func(key, value any) bool {
		backend, backendOK := key.(string)
		loadedAt, loadedAtOK := value.(time.Time)
		if backendOK && loadedAtOK {
			age := now.Sub(loadedAt).Seconds()
			if age < 0 {
				age = 0
			}
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, age, backend)
		}
		return true
	})
}

func init() {
	prometheus.MustRegister(decisionCounter, evaluationDuration, reloadCounter, snapshotAgeMetric)
}
func ObserveDecision(backend string, effect Effect, started time.Time) {
	decisionCounter.WithLabelValues(backend, string(effect)).Inc()
	evaluationDuration.WithLabelValues(backend).Observe(time.Since(started).Seconds())
}
func ObserveReload(backend string, success bool) {
	reloadCounter.WithLabelValues(backend, strconv.FormatBool(success)).Inc()
}
func SetSnapshotLoadedAt(backend string, loadedAt time.Time) {
	snapshotLoadedAt.Store(backend, loadedAt)
}
