package replication

import "github.com/prometheus/client_golang/prometheus"

type replicationMetrics struct {
	pending  prometheus.Gauge
	retries  prometheus.Counter
	failures prometheus.Counter
}

func newMetrics(registerer prometheus.Registerer, id string) (*replicationMetrics, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	pending := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "pithos_replication_pending_operations", Help: "Durable operations awaiting primary or replica confirmation."}, []string{"replication_id"})
	retries := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_replication_retries_total", Help: "Retries of durable replication operations."}, []string{"replication_id"})
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "pithos_replication_failures_total", Help: "Replication attempts that failed before every replica confirmed."}, []string{"replication_id"})
	for _, collector := range []prometheus.Collector{pending, retries, failures} {
		if err := registerer.Register(collector); err != nil {
			if existing, ok := err.(prometheus.AlreadyRegisteredError); ok {
				switch collector {
				case pending:
					pending = existing.ExistingCollector.(*prometheus.GaugeVec)
				case retries:
					retries = existing.ExistingCollector.(*prometheus.CounterVec)
				case failures:
					failures = existing.ExistingCollector.(*prometheus.CounterVec)
				}
			} else {
				return nil, err
			}
		}
	}
	return &replicationMetrics{pending: pending.WithLabelValues(id), retries: retries.WithLabelValues(id), failures: failures.WithLabelValues(id)}, nil
}
