package outboxruntime

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	PendingEntries     prometheus.Gauge
	ProcessedEntries   prometheus.Counter
	ProcessingDuration prometheus.Histogram
	ErrorsCounter      prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer, subsystem string, pendingHelp string, processedHelp string, processingHelp string, errorsHelp string) *Metrics {
	m := &Metrics{
		PendingEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: subsystem,
			Name:      "pending_entries",
			Help:      pendingHelp,
		}),
		ProcessedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: subsystem,
			Name:      "processed_entries_total",
			Help:      processedHelp,
		}),
		ProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pithos",
			Subsystem: subsystem,
			Name:      "processing_duration_seconds",
			Help:      processingHelp,
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		ErrorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      errorsHelp,
		}),
	}

	registerMetric(registerer, m.PendingEntries, "pendingEntries")
	registerMetric(registerer, m.ProcessedEntries, "processedEntries")
	registerMetric(registerer, m.ProcessingDuration, "processingDuration")
	registerMetric(registerer, m.ErrorsCounter, "errorsCounter")

	return m
}

func registerMetric(registerer prometheus.Registerer, collector prometheus.Collector, name string) {
	if err := registerer.Register(collector); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error(fmt.Sprintf("Failed to register %s metric", name), "error", err)
		}
	}
}

type Processor[T any] interface {
	CountPending(ctx context.Context, tx *sql.Tx) (int, error)
	FindFirstForUpdate(ctx context.Context, tx *sql.Tx) (T, error)
	ProcessEntry(ctx context.Context, tx *sql.Tx, entry T) error
	DeleteEntry(ctx context.Context, tx *sql.Tx, entry T) error
	Name() string
}

type Runtime[T any] struct {
	db                 database.Database
	processor          Processor[T]
	metrics            *Metrics
	triggerChannel     chan struct{}
	triggerClosed      bool
	processingTask     *task.TaskHandle
	pollInterval       time.Duration
	errorBackoff       time.Duration
	joinTimeout        time.Duration
	processedLogFormat string
}

func New[T any](db database.Database, processor Processor[T], metrics *Metrics) *Runtime[T] {
	return &Runtime[T]{
		db:                 db,
		processor:          processor,
		metrics:            metrics,
		triggerChannel:     make(chan struct{}, 16),
		triggerClosed:      false,
		pollInterval:       1 * time.Second,
		errorBackoff:       5 * time.Second,
		joinTimeout:        30 * time.Second,
		processedLogFormat: "Processed %d outbox entries",
	}
}

func (r *Runtime[T]) Trigger() {
	select {
	case r.triggerChannel <- struct{}{}:
	default:
	}
}

func (r *Runtime[T]) Start() {
	r.processingTask = task.Start(func(_ *atomic.Bool) {
		r.processLoop()
	})
}

func (r *Runtime[T]) Stop() {
	if r.triggerClosed {
		return
	}
	close(r.triggerChannel)
	if r.processingTask != nil {
		joinedWithTimeout := r.processingTask.JoinWithTimeout(r.joinTimeout)
		if joinedWithTimeout {
			slog.Debug(fmt.Sprintf("%s processing task joined with timeout of %s", r.processor.Name(), r.joinTimeout))
		} else {
			slog.Debug(fmt.Sprintf("%s processing task joined without timeout", r.processor.Name()))
		}
	}
	r.triggerClosed = true
}

func (r *Runtime[T]) processLoop() {
	ctx := context.Background()
out:
	for {
		select {
		case _, ok := <-r.triggerChannel:
			if !ok {
				slog.Debug(fmt.Sprintf("Stopping %s processing", r.processor.Name()))
				break out
			}
		case <-time.After(r.pollInterval):
		}
		r.maybeProcessEntries(ctx)
	}
}

func (r *Runtime[T]) maybeProcessEntries(ctx context.Context) {
	startTime := time.Now()
	processedEntriesCount := 0
	defer func() {
		r.metrics.ProcessingDuration.Observe(time.Since(startTime).Seconds())
		if processedEntriesCount > 0 {
			r.metrics.ProcessedEntries.Add(float64(processedEntriesCount))
		}
	}()

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return
	}
	pendingCount, err := r.processor.CountPending(ctx, tx)
	if err != nil {
		tx.Rollback()
		return
	}
	r.metrics.PendingEntries.Set(float64(pendingCount))
	tx.Commit()

	for {
		tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			r.metrics.ErrorsCounter.Inc()
			return
		}
		entry, err := r.processor.FindFirstForUpdate(ctx, tx)
		if err != nil {
			tx.Rollback()
			r.metrics.ErrorsCounter.Inc()
			time.Sleep(r.errorBackoff)
			return
		}
		if any(entry) == nil {
			tx.Commit()
			break
		}

		err = r.processor.ProcessEntry(ctx, tx, entry)
		if err != nil {
			tx.Rollback()
			r.metrics.ErrorsCounter.Inc()
			time.Sleep(r.errorBackoff)
			return
		}

		err = r.processor.DeleteEntry(ctx, tx, entry)
		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
		if err != nil {
			return
		}
		processedEntriesCount += 1
	}

	if processedEntriesCount > 0 {
		slog.Info(fmt.Sprintf(r.processedLogFormat, processedEntriesCount))
	}
}
