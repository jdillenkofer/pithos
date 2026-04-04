package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	partOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type partOutboxMetrics struct {
	pendingEntries     prometheus.Gauge
	processedEntries   prometheus.Counter
	processingDuration prometheus.Histogram
	errorsCounter      prometheus.Counter
}

func newPartOutboxMetrics(registerer prometheus.Registerer) *partOutboxMetrics {
	m := &partOutboxMetrics{
		pendingEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: "part_outbox",
			Name:      "pending_entries",
			Help:      "Number of pending part outbox entries",
		}),
		processedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "part_outbox",
			Name:      "processed_entries_total",
			Help:      "Total number of processed part outbox entries",
		}),
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pithos",
			Subsystem: "part_outbox",
			Name:      "processing_duration_seconds",
			Help:      "Duration of part outbox processing in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		errorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "part_outbox",
			Name:      "errors_total",
			Help:      "Total number of part outbox processing errors",
		}),
	}

	if err := registerer.Register(m.pendingEntries); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register pendingEntries metric", "error", err)
		}
	}
	if err := registerer.Register(m.processedEntries); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register processedEntries metric", "error", err)
		}
	}
	if err := registerer.Register(m.processingDuration); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register processingDuration metric", "error", err)
		}
	}
	if err := registerer.Register(m.errorsCounter); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			slog.Error("Failed to register errorsCounter metric", "error", err)
		}
	}

	return m
}

type outboxPartStore struct {
	db                         database.Database
	triggerChannel             chan struct{}
	triggerChannelClosed       bool
	outboxProcessingTaskHandle *task.TaskHandle
	innerPartStore             partstore.PartStore
	partOutboxEntryRepository  partOutboxEntry.Repository
	tracer                     trace.Tracer
	metrics                    *partOutboxMetrics
}

// Compile-time check to ensure outboxPartStore implements partstore.PartStore
var _ partstore.PartStore = (*outboxPartStore)(nil)

func New(db database.Database, innerPartStore partstore.PartStore, partOutboxEntryRepository partOutboxEntry.Repository, registerer prometheus.Registerer) (partstore.PartStore, error) {
	obs := &outboxPartStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 16),
		triggerChannelClosed:      false,
		innerPartStore:            innerPartStore,
		partOutboxEntryRepository: partOutboxEntryRepository,
		tracer:                    otel.Tracer("internal/storage/metadatapart/partstore/outbox"),
		metrics:                   newPartOutboxMetrics(registerer),
	}
	return obs, nil
}

func (obs *outboxPartStore) maybeProcessOutboxEntries(ctx context.Context) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.maybeProcessOutboxEntries")
	defer span.End()

	startTime := time.Now()
	processedOutboxEntryCount := 0
	defer func() {
		obs.metrics.processingDuration.Observe(time.Since(startTime).Seconds())
		if processedOutboxEntryCount > 0 {
			obs.metrics.processedEntries.Add(float64(processedOutboxEntryCount))
		}
	}()

	tx, err := obs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return
	}

	pendingCount, err := obs.partOutboxEntryRepository.Count(ctx, tx)
	if err != nil {
		tx.Rollback()
		return
	}
	obs.metrics.pendingEntries.Set(float64(pendingCount))
	tx.Commit()

	for {
		tx, err := obs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			obs.metrics.errorsCounter.Inc()
			return
		}

		entry, err := obs.partOutboxEntryRepository.FindFirstPartOutboxEntryWithForUpdateLock(ctx, tx)
		if err != nil {
			tx.Rollback()
			obs.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if entry == nil {
			tx.Commit()
			break
		}

		switch entry.Operation {
		case partOutboxEntry.PutPartOperation:
			chunks, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunksById(ctx, tx, *entry.Id)
			if err != nil {
				tx.Rollback()
				obs.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
			readers := make([]io.Reader, len(chunks))
			for i, chunk := range chunks {
				readers[i] = bytes.NewReader(chunk.Content)
			}
			err = obs.innerPartStore.PutPart(ctx, tx, entry.PartId, io.MultiReader(readers...))
			if err != nil {
				tx.Rollback()
				obs.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		case partOutboxEntry.DeletePartOperation:
			err = obs.innerPartStore.DeletePart(ctx, tx, entry.PartId)
			if err != nil {
				tx.Rollback()
				obs.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		default:
			slog.Warn(fmt.Sprint("Invalid operation", entry.Operation, "during outbox processing."))
			tx.Rollback()
			obs.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		err = obs.partOutboxEntryRepository.DeletePartOutboxEntryById(ctx, tx, *entry.Id)
		if err != nil {
			tx.Rollback()
			return
		}
		processedOutboxEntryCount += 1

		err = tx.Commit()
		if err != nil {
			return
		}
	}
	if processedOutboxEntryCount > 0 {
		slog.Info(fmt.Sprintf("Processed %d outbox entries", processedOutboxEntryCount))
	}
}

func (obs *outboxPartStore) processOutboxLoop() {
	ctx := context.Background()
out:
	for {
		select {
		case _, ok := <-obs.triggerChannel:
			if !ok {
				slog.Debug("Stopping OutboxPartStore processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		obs.maybeProcessOutboxEntries(ctx)
	}
}

func (obs *outboxPartStore) Start(ctx context.Context) error {
	obs.outboxProcessingTaskHandle = task.Start(func(_ *atomic.Bool) {
		obs.processOutboxLoop()
	})
	return obs.innerPartStore.Start(ctx)
}

func (obs *outboxPartStore) Stop(ctx context.Context) error {
	if !obs.triggerChannelClosed {
		close(obs.triggerChannel)
		if obs.outboxProcessingTaskHandle != nil {
			joinedWithTimeout := obs.outboxProcessingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				slog.Debug("OutboxPartStore.outboxProcessingTaskHandle joined with timeout of 30s")
			} else {
				slog.Debug("OutboxPartStore.outboxProcessingTaskHandle joined without timeout")
			}
		}
		obs.triggerChannelClosed = true
	}
	return obs.innerPartStore.Stop(ctx)
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (obs *outboxPartStore) storePartOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, partId partstore.PartId) (*ulid.ULID, error) {
	entry := partOutboxEntry.Entity{
		Operation: operation,
		PartId:    partId,
	}
	err := obs.partOutboxEntryRepository.SavePartOutboxEntry(ctx, tx, &entry)
	if err != nil {
		return nil, err
	}

	// Put struct{} in the channel unless it is full
	select {
	case obs.triggerChannel <- struct{}{}:
	default:
	}

	return entry.Id, nil
}

func (obs *outboxPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.PutPart")
	defer span.End()

	entryId, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.PutPartOperation, partId)
	if err != nil {
		return err
	}

	buffer := make([]byte, chunkSize)
	chunkIndex := 0
	for {
		n, err := io.ReadFull(reader, buffer)
		if n > 0 {
			content := make([]byte, n)
			copy(content, buffer[:n])

			chunk := partOutboxEntry.ContentChunk{
				OutboxEntryId: *entryId,
				ChunkIndex:    chunkIndex,
				Content:       content,
			}
			err = obs.partOutboxEntryRepository.SavePartOutboxContentChunk(ctx, tx, &chunk)
			if err != nil {
				return err
			}
			chunkIndex++
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
	}

	return nil
}

func (obs *outboxPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPart")
	defer span.End()

	lastEntry, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryByPartId(ctx, tx, partId)
	if err != nil {
		return nil, err
	}
	if lastEntry != nil {
		if lastEntry.Operation == partOutboxEntry.DeletePartOperation {
			return nil, partstore.ErrPartNotFound
		}

		chunks, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunksById(ctx, tx, *lastEntry.Id)
		if err != nil {
			return nil, err
		}
		if len(chunks) > 0 {
			readers := make([]io.Reader, len(chunks))
			for i, chunk := range chunks {
				readers[i] = bytes.NewReader(chunk.Content)
			}
			return io.NopCloser(io.MultiReader(readers...)), nil
		}
	}
	return obs.innerPartStore.GetPart(ctx, tx, partId)
}

func (obs *outboxPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPartIds")
	defer span.End()

	// We get the lastOutboxEntry for each partId
	lastOutboxEntryGroupedByPartId, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryGroupedByPartId(ctx, tx)
	if err != nil {
		return nil, err
	}
	// These are the partIds already committed to the innerStorage
	// might be more than we actually currently have...
	innerPartIds, err := obs.innerPartStore.GetPartIds(ctx, tx)
	if err != nil {
		return nil, err
	}

	allPartIds := make(map[partstore.PartId]struct{})
	// write them all into the set
	for _, partId := range innerPartIds {
		allPartIds[partId] = struct{}{}
	}

	// then remove the once that were deleted in the outbox
	// and add the once that were added to the outbox
	for _, outboxEntry := range lastOutboxEntryGroupedByPartId {
		switch outboxEntry.Operation {
		case partOutboxEntry.DeletePartOperation:
			delete(allPartIds, outboxEntry.PartId)
		case partOutboxEntry.PutPartOperation:
			allPartIds[outboxEntry.PartId] = struct{}{}
		}
	}

	// convert the set back to a list
	partIds := []partstore.PartId{}
	for partId := range allPartIds {
		partIds = append(partIds, partId)
	}
	return partIds, nil
}

func (obs *outboxPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.DeletePart")
	defer span.End()

	_, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.DeletePartOperation, partId)
	if err != nil {
		return err
	}

	return nil
}
