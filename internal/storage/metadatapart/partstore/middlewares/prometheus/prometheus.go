package prometheus

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/task"
	client "github.com/prometheus/client_golang/prometheus"
)

var metricsOnce sync.Once
var metrics struct {
	ops              *client.CounterVec
	duration         *client.HistogramVec
	bytesRead        *client.CounterVec
	bytesWritten     *client.CounterVec
	parts, partBytes *client.GaugeVec
}

func registerMetrics() {
	metricsOnce.Do(func() {
		metrics.ops = client.NewCounterVec(client.CounterOpts{Namespace: "pithos", Subsystem: "partstore", Name: "ops_total", Help: "Number of part store operations by store, operation, and outcome"}, []string{"store", "operation", "outcome"})
		metrics.duration = client.NewHistogramVec(client.HistogramOpts{Namespace: "pithos", Subsystem: "partstore", Name: "op_duration_seconds", Help: "Duration of part store operations by store and operation"}, []string{"store", "operation"})
		metrics.bytesRead = client.NewCounterVec(client.CounterOpts{Namespace: "pithos", Subsystem: "partstore", Name: "bytes_read_total", Help: "Bytes read from part stores"}, []string{"store"})
		metrics.bytesWritten = client.NewCounterVec(client.CounterOpts{Namespace: "pithos", Subsystem: "partstore", Name: "bytes_written_total", Help: "Bytes written to part stores"}, []string{"store"})
		metrics.parts = client.NewGaugeVec(client.GaugeOpts{Namespace: "pithos", Subsystem: "partstore", Name: "parts", Help: "Number of physical parts in each part store"}, []string{"store"})
		metrics.partBytes = client.NewGaugeVec(client.GaugeOpts{Namespace: "pithos", Subsystem: "partstore", Name: "part_bytes", Help: "Bytes occupied by physical parts in each part store"}, []string{"store"})
	})
	pithosmetrics.Register(metrics.ops, metrics.duration, metrics.bytesRead, metrics.bytesWritten, metrics.parts, metrics.partBytes)
}

type PartStoreMiddleware struct {
	inner    partstore.PartStore
	db       database.Database
	name     string
	interval time.Duration
	task     *task.TaskHandle
}

var _ partstore.PartStore = (*PartStoreMiddleware)(nil)
var _ partstore.CapabilityProvider = (*PartStoreMiddleware)(nil)

func New(inner partstore.PartStore, db database.Database, name string, interval time.Duration) partstore.PartStore {
	registerMetrics()
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &PartStoreMiddleware{inner: inner, db: db, name: name, interval: interval}
}

func (m *PartStoreMiddleware) Capabilities() partstore.Capabilities {
	return partstore.CapabilitiesOf(m.inner)
}

func (m *PartStoreMiddleware) observe(operation string, started time.Time, err error) {
	outcome := "success"
	if errors.Is(err, partstore.ErrPartNotFound) {
		outcome = "not_found"
	} else if err != nil {
		outcome = "error"
	}
	metrics.ops.WithLabelValues(m.name, operation, outcome).Inc()
	metrics.duration.WithLabelValues(m.name, operation).Observe(time.Since(started).Seconds())
}

func (m *PartStoreMiddleware) PutPart(ctx context.Context, tx database.Tx, id partstore.PartId, reader io.Reader) error {
	started := time.Now()
	var count int64
	err := m.inner.PutPart(ctx, tx, id, ioutils.NewCountingReader(reader, &count))
	m.observe("put", started, err)
	metrics.bytesWritten.WithLabelValues(m.name).Add(float64(count))
	return err
}

func (m *PartStoreMiddleware) GetPart(ctx context.Context, tx database.Tx, id partstore.PartId) (io.ReadCloser, error) {
	started := time.Now()
	reader, err := m.inner.GetPart(ctx, tx, id)
	m.observe("get", started, err)
	if err != nil {
		return nil, err
	}
	return ioutils.NewStatsReadCloser(reader, func(n int) { metrics.bytesRead.WithLabelValues(m.name).Add(float64(n)) }), nil
}

func (m *PartStoreMiddleware) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	started := time.Now()
	ids, err := m.inner.GetPartIds(ctx, tx)
	m.observe("list", started, err)
	return ids, err
}

func (m *PartStoreMiddleware) DeletePart(ctx context.Context, tx database.Tx, id partstore.PartId) error {
	started := time.Now()
	err := m.inner.DeletePart(ctx, tx, id)
	m.observe("delete", started, err)
	return err
}

func (m *PartStoreMiddleware) refresh(ctx context.Context) {
	_ = database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		ids, err := m.inner.GetPartIds(ctx, tx)
		if err != nil {
			return err
		}
		var bytes int64
		for _, id := range ids {
			reader, err := m.inner.GetPart(ctx, tx, id)
			if err != nil {
				return err
			}
			n, readErr := io.Copy(io.Discard, reader)
			closeErr := reader.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			bytes += n
		}
		metrics.parts.WithLabelValues(m.name).Set(float64(len(ids)))
		metrics.partBytes.WithLabelValues(m.name).Set(float64(bytes))
		return nil
	})
}

func (m *PartStoreMiddleware) Start(ctx context.Context) error {
	if err := m.inner.Start(ctx); err != nil {
		return err
	}
	m.task = task.Start(func(cancel *atomic.Bool) {
		for !cancel.Load() {
			m.refresh(context.Background())
			deadline := time.Now().Add(m.interval)
			for !cancel.Load() && time.Now().Before(deadline) {
				time.Sleep(min(250*time.Millisecond, time.Until(deadline)))
			}
		}
	})
	return nil
}

func (m *PartStoreMiddleware) Stop(ctx context.Context) error {
	if m.task != nil {
		m.task.Cancel()
		m.task.JoinWithTimeout(30 * time.Second)
	}
	return m.inner.Stop(ctx)
}
