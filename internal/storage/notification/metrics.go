package notification

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// notificationMetrics holds the Prometheus collectors for the notification
// dispatcher. Labels are deliberately low-cardinality (outbox id, destination
// type, payload format); per-bucket, per-key, and per-ARN labels are avoided.
type notificationMetrics struct {
	pendingEntries      *prometheus.GaugeVec
	deadLetteredEntries *prometheus.GaugeVec
	claimedEntries      *prometheus.CounterVec
	publishedEntries    *prometheus.CounterVec
	failedPublishes     *prometheus.CounterVec
	retryAttempts       *prometheus.CounterVec
	publishLatency      *prometheus.HistogramVec
}

const (
	metricsNamespace = "pithos"
	metricsSubsystem = "notification"
)

var (
	gaugeLabels   = []string{"outbox_id"}
	counterLabels = []string{"outbox_id", "destination_type", "payload_format"}
)

func newNotificationMetrics(registerer prometheus.Registerer) *notificationMetrics {
	m := &notificationMetrics{
		pendingEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "pending_entries", Help: "Number of notification outbox entries awaiting delivery",
		}, gaugeLabels),
		deadLetteredEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "dead_lettered_entries", Help: "Number of dead-lettered notification outbox entries",
		}, gaugeLabels),
		claimedEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "claimed_entries_total", Help: "Total number of notification outbox entries claimed for delivery",
		}, counterLabels),
		publishedEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "published_entries_total", Help: "Total number of successfully published notifications",
		}, counterLabels),
		failedPublishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_publishes_total", Help: "Total number of failed notification publish attempts",
		}, counterLabels),
		retryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "retry_attempts_total", Help: "Total number of notification delivery retries scheduled",
		}, counterLabels),
		publishLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "publish_latency_seconds", Help: "Latency of notification publish attempts in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, counterLabels),
	}
	if registerer != nil {
		for name, collector := range map[string]prometheus.Collector{
			"pendingEntries":      m.pendingEntries,
			"deadLetteredEntries": m.deadLetteredEntries,
			"claimedEntries":      m.claimedEntries,
			"publishedEntries":    m.publishedEntries,
			"failedPublishes":     m.failedPublishes,
			"retryAttempts":       m.retryAttempts,
			"publishLatency":      m.publishLatency,
		} {
			if err := registerer.Register(collector); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
					slog.Error("Failed to register notification metric", "metric", name, "error", err)
				}
			}
		}
	}
	return m
}

func payloadFormatLabel(format PayloadFormat) string {
	if format == "" {
		return string(PayloadFormatS3Records)
	}
	return string(format)
}

func (m *notificationMetrics) entryLabels(outboxID string, entry *OutboxEntry) prometheus.Labels {
	return prometheus.Labels{
		"outbox_id":        outboxID,
		"destination_type": destinationTypeLabel(entry.DestinationARN),
		"payload_format":   payloadFormatLabel(entry.PayloadFormat),
	}
}

func (m *notificationMetrics) recordClaimed(outboxID string, entry *OutboxEntry) {
	m.claimedEntries.With(m.entryLabels(outboxID, entry)).Inc()
}

func (m *notificationMetrics) recordPublished(outboxID string, entry *OutboxEntry, latency time.Duration) {
	labels := m.entryLabels(outboxID, entry)
	m.publishedEntries.With(labels).Inc()
	m.publishLatency.With(labels).Observe(latency.Seconds())
}

func (m *notificationMetrics) recordFailed(outboxID string, entry *OutboxEntry, latency time.Duration) {
	labels := m.entryLabels(outboxID, entry)
	m.failedPublishes.With(labels).Inc()
	m.publishLatency.With(labels).Observe(latency.Seconds())
}

func (m *notificationMetrics) recordRetry(outboxID string, entry *OutboxEntry) {
	m.retryAttempts.With(m.entryLabels(outboxID, entry)).Inc()
}

func (m *notificationMetrics) setPending(outboxID string, pending int, deadLettered int) {
	m.pendingEntries.With(prometheus.Labels{"outbox_id": outboxID}).Set(float64(pending))
	m.deadLetteredEntries.With(prometheus.Labels{"outbox_id": outboxID}).Set(float64(deadLettered))
}
