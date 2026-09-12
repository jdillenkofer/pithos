package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var refreshOnce sync.Once
var refreshErrors *prometheus.CounterVec
var refreshLastSuccess *prometheus.GaugeVec
var refreshDuration *prometheus.HistogramVec

func ObserveRefresh(collector string, started time.Time, err error) {
	refreshOnce.Do(func() {
		refreshErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "metrics", Name: "refresh_errors_total", Help: "Number of periodic metric refresh failures"}, []string{"collector"})
		refreshLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "metrics", Name: "last_success_timestamp_seconds", Help: "Unix timestamp of the latest successful periodic metric refresh"}, []string{"collector"})
		refreshDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "pithos", Subsystem: "metrics", Name: "refresh_duration_seconds", Help: "Duration of periodic metric refreshes"}, []string{"collector"})
	})
	Register(refreshErrors, refreshLastSuccess, refreshDuration)
	refreshDuration.WithLabelValues(collector).Observe(time.Since(started).Seconds())
	if err != nil {
		refreshErrors.WithLabelValues(collector).Inc()
		return
	}
	refreshLastSuccess.WithLabelValues(collector).SetToCurrentTime()
}
