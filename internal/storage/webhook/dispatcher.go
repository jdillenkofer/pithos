package webhook

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	webhookOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/webhookoutboxentry"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type webhookMetrics struct {
	pendingEntries     prometheus.Gauge
	processedEntries   prometheus.Counter
	processingDuration prometheus.Histogram
	errorsCounter      prometheus.Counter
}

func newWebhookMetrics(registerer prometheus.Registerer) *webhookMetrics {
	m := &webhookMetrics{
		pendingEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: "webhook",
			Name:      "pending_entries",
			Help:      "Number of pending webhook outbox entries",
		}),
		processedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "webhook",
			Name:      "processed_entries_total",
			Help:      "Total number of processed webhook outbox entries",
		}),
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pithos",
			Subsystem: "webhook",
			Name:      "processing_duration_seconds",
			Help:      "Duration of webhook processing in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		errorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "webhook",
			Name:      "errors_total",
			Help:      "Total number of webhook processing errors",
		}),
	}

	if err := registerer.Register(m.pendingEntries); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register webhook pendingEntries metric", "error", err)
		}
	}
	if err := registerer.Register(m.processedEntries); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register webhook processedEntries metric", "error", err)
		}
	}
	if err := registerer.Register(m.processingDuration); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register webhook processingDuration metric", "error", err)
		}
	}
	if err := registerer.Register(m.errorsCounter); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register webhook errorsCounter metric", "error", err)
		}
	}

	return m
}

// Dispatcher persists webhook outbox entries transactionally and delivers them
// over HTTP after the enqueueing transaction commits. Delivery mirrors the
// storage outbox engine: ULID-ordered claim, lease + heartbeat, delete on HTTP
// 2xx, release-and-retry on failure.
type Dispatcher struct {
	*lifecycle.ValidatedLifecycle
	db                     database.Database
	triggerChannel         chan struct{}
	triggerChannelClosed   bool
	outboxId               string
	claimOwner             string
	claimLeaseDuration     time.Duration
	processingTaskHandle   *task.TaskHandle
	webhookOutboxEntryRepo webhookOutboxEntry.Repository
	httpClient             *http.Client
	tracer                 trace.Tracer
	metrics                *webhookMetrics
}

const (
	defaultClaimLeaseDuration = 30 * time.Second
	defaultHttpTimeout        = 30 * time.Second
)

func NewDispatcher(db database.Database, outboxId string, repo webhookOutboxEntry.Repository, registerer prometheus.Registerer, claimLeaseDuration time.Duration, httpTimeout time.Duration) (*Dispatcher, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("WebhookDispatcher")
	if err != nil {
		return nil, err
	}
	leaseDuration := defaultClaimLeaseDuration
	if claimLeaseDuration > 0 {
		leaseDuration = claimLeaseDuration
	}
	timeout := defaultHttpTimeout
	if httpTimeout > 0 {
		timeout = httpTimeout
	}
	d := &Dispatcher{
		ValidatedLifecycle:     lifecycle,
		db:                     db,
		triggerChannel:         make(chan struct{}, 16),
		triggerChannelClosed:   false,
		outboxId:               outboxId,
		claimOwner:             outboxId + ":" + ulid.Make().String(),
		claimLeaseDuration:     leaseDuration,
		webhookOutboxEntryRepo: repo,
		httpClient:             &http.Client{Timeout: timeout},
		tracer:                 otel.Tracer("internal/storage/webhook"),
		metrics:                newWebhookMetrics(registerer),
	}
	return d, nil
}

// Enqueue persists a webhook outbox entry within the given transaction and
// schedules delivery once the transaction commits.
func (d *Dispatcher) Enqueue(ctx context.Context, tx database.Tx, entry *webhookOutboxEntry.Entity) error {
	err := d.webhookOutboxEntryRepo.SaveWebhookOutboxEntry(ctx, tx.SqlTx(), d.outboxId, entry)
	if err != nil {
		return err
	}

	tx.OnAfterCommit(func(context.Context) error {
		// Put struct{} in the channel unless it is full.
		select {
		case d.triggerChannel <- struct{}{}:
		default:
		}
		return nil
	})

	return nil
}

func (d *Dispatcher) claimNextOutboxEntry(ctx context.Context) (*webhookOutboxEntry.Entity, bool, error) {
	var entry *webhookOutboxEntry.Entity
	var claimed bool
	err := database.WithTx(ctx, d.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		now := time.Now().UTC()
		var err error
		entry, claimed, err = d.webhookOutboxEntryRepo.ClaimFirstWebhookOutboxEntry(ctx, tx.SqlTx(), d.outboxId, d.claimOwner, now, now.Add(d.claimLeaseDuration))
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return entry, claimed, nil
}

func (d *Dispatcher) finalizeOutboxEntry(ctx context.Context, entry *webhookOutboxEntry.Entity) (bool, error) {
	var deleted bool
	err := database.WithTx(ctx, d.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		deleted, err = d.webhookOutboxEntryRepo.DeleteWebhookOutboxEntryByClaimOwner(ctx, tx.SqlTx(), d.outboxId, *entry.Id, d.claimOwner)
		return err
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func (d *Dispatcher) releaseOutboxEntry(ctx context.Context, entry *webhookOutboxEntry.Entity) (bool, error) {
	var released bool
	err := database.WithTx(ctx, d.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		released, err = d.webhookOutboxEntryRepo.ReleaseWebhookOutboxEntryClaim(ctx, tx.SqlTx(), d.outboxId, *entry.Id, d.claimOwner, time.Now().UTC())
		return err
	})
	if err != nil {
		return false, err
	}
	return released, nil
}

func (d *Dispatcher) startOutboxHeartbeat(ctx context.Context, entry *webhookOutboxEntry.Entity) func() {
	interval := d.claimLeaseDuration / 3
	if interval <= 0 {
		interval = time.Second
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				now := time.Now().UTC()
				var extended bool
				err := database.WithTx(ctx, d.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
					var err error
					extended, err = d.webhookOutboxEntryRepo.ExtendWebhookOutboxEntryClaim(ctx, tx.SqlTx(), d.outboxId, *entry.Id, d.claimOwner, now, now.Add(d.claimLeaseDuration))
					return err
				})
				if err != nil {
					slog.WarnContext(ctx, "Failed to commit webhook outbox heartbeat", "error", err)
					continue
				}
				if !extended {
					slog.WarnContext(ctx, "Webhook outbox heartbeat lost claim", "entryId", entry.Id.String())
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func (d *Dispatcher) sendWebhook(ctx context.Context, entry *webhookOutboxEntry.Entity) error {
	req, err := http.NewRequestWithContext(ctx, entry.Method, entry.Url, bytes.NewReader(entry.Body))
	if err != nil {
		return err
	}
	if entry.Headers != nil && *entry.Headers != "" {
		headers := map[string]string{}
		if err := json.Unmarshal([]byte(*entry.Headers), &headers); err != nil {
			return err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
	}
	return nil
}

func (d *Dispatcher) maybeProcessOutboxEntries(ctx context.Context) {
	startTime := time.Now()
	processedOutboxEntryCount := 0
	defer func() {
		d.metrics.processingDuration.Observe(time.Since(startTime).Seconds())
		if processedOutboxEntryCount > 0 {
			d.metrics.processedEntries.Add(float64(processedOutboxEntryCount))
		}
	}()

	var pendingCount int
	if err := database.WithTx(ctx, d.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		pendingCount, err = d.webhookOutboxEntryRepo.Count(ctx, tx.SqlTx(), d.outboxId)
		return err
	}); err != nil {
		return
	}
	d.metrics.pendingEntries.Set(float64(pendingCount))

	for {
		entry, claimed, err := d.claimNextOutboxEntry(ctx)
		if err != nil {
			d.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if entry == nil || !claimed {
			break
		}

		stopHeartbeat := d.startOutboxHeartbeat(ctx, entry)
		err = d.sendWebhook(ctx, entry)
		stopHeartbeat()
		if err != nil {
			slog.WarnContext(ctx, "Failed to deliver webhook", "entryId", entry.Id.String(), "error", err)
			_, _ = d.releaseOutboxEntry(ctx, entry)
			d.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}

		deleted, err := d.finalizeOutboxEntry(ctx, entry)
		if err != nil {
			d.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if !deleted {
			d.metrics.errorsCounter.Inc()
			slog.Warn("Webhook outbox finalize skipped because claim owner no longer matched", "entryId", entry.Id.String())
			return
		}
		processedOutboxEntryCount += 1
	}
	if processedOutboxEntryCount > 0 {
		slog.Info(fmt.Sprintf("Delivered %d webhooks", processedOutboxEntryCount))
	}
}

func (d *Dispatcher) processOutboxLoop() {
	ctx := context.Background()
out:
	for {
		select {
		case _, ok := <-d.triggerChannel:
			if !ok {
				slog.Debug("Stopping webhook dispatcher processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		d.maybeProcessOutboxEntries(ctx)
	}
}

func (d *Dispatcher) Start(ctx context.Context) error {
	if err := d.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	d.processingTaskHandle = task.Start(func(_ *atomic.Bool) {
		d.processOutboxLoop()
	})
	return nil
}

func (d *Dispatcher) Stop(ctx context.Context) error {
	if err := d.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	if !d.triggerChannelClosed {
		close(d.triggerChannel)
		if d.processingTaskHandle != nil {
			joinedWithTimeout := d.processingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				slog.Debug("WebhookDispatcher.processingTaskHandle joined with timeout of 30s")
			} else {
				slog.Debug("WebhookDispatcher.processingTaskHandle joined without timeout")
			}
		}
		d.triggerChannelClosed = true
	}
	return nil
}
