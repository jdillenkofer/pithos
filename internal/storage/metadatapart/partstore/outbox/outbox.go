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

	"github.com/jdillenkofer/pithos/internal/ioutils"
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
	outboxId                   string
	claimOwner                 string
	claimLeaseDuration         time.Duration
	outboxProcessingTaskHandle *task.TaskHandle
	innerPartStore             partstore.PartStore
	partOutboxEntryRepository  partOutboxEntry.Repository
	tracer                     trace.Tracer
	metrics                    *partOutboxMetrics
}

// Compile-time check to ensure outboxPartStore implements partstore.PartStore
var _ partstore.PartStore = (*outboxPartStore)(nil)

const defaultClaimLeaseDuration = 30 * time.Second

func New(db database.Database, outboxId string, innerPartStore partstore.PartStore, partOutboxEntryRepository partOutboxEntry.Repository, registerer prometheus.Registerer, claimLeaseDuration time.Duration) (partstore.PartStore, error) {
	leaseDuration := defaultClaimLeaseDuration
	if claimLeaseDuration > 0 {
		leaseDuration = claimLeaseDuration
	}
	obs := &outboxPartStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 16),
		triggerChannelClosed:      false,
		outboxId:                  outboxId,
		claimOwner:                outboxId + ":" + ulid.Make().String(),
		claimLeaseDuration:        leaseDuration,
		innerPartStore:            innerPartStore,
		partOutboxEntryRepository: partOutboxEntryRepository,
		tracer:                    otel.Tracer("internal/storage/metadatapart/partstore/outbox"),
		metrics:                   newPartOutboxMetrics(registerer),
	}
	return obs, nil
}

func (obs *outboxPartStore) claimNextOutboxEntry(ctx context.Context) (*partOutboxEntry.Entity, bool, error) {
	var entry *partOutboxEntry.Entity
	var claimed bool
	err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		now := time.Now().UTC()
		var err error
		entry, claimed, err = obs.partOutboxEntryRepository.ClaimFirstPartOutboxEntry(ctx, tx.SqlTx(), obs.outboxId, obs.claimOwner, now, now.Add(obs.claimLeaseDuration))
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return entry, claimed, nil
}

func (obs *outboxPartStore) readPartOutboxContent(ctx context.Context, entry *partOutboxEntry.Entity) (io.Reader, error) {
	if entry.Operation != partOutboxEntry.PutPartOperation {
		return nil, nil
	}
	var chunks []*partOutboxEntry.ContentChunk
	err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		chunks, err = obs.partOutboxEntryRepository.FindPartOutboxEntryChunksById(ctx, tx.SqlTx(), obs.outboxId, *entry.Id)
		return err
	})
	if err != nil {
		return nil, err
	}
	readers := make([]io.Reader, len(chunks))
	for i, chunk := range chunks {
		readers[i] = bytes.NewReader(chunk.Content)
	}
	return io.MultiReader(readers...), nil
}

func (obs *outboxPartStore) finalizePartOutboxEntry(ctx context.Context, entry *partOutboxEntry.Entity) (bool, error) {
	var deleted bool
	err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		deleted, err = obs.partOutboxEntryRepository.DeletePartOutboxEntryByClaimOwner(ctx, tx.SqlTx(), obs.outboxId, *entry.Id, obs.claimOwner)
		return err
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func (obs *outboxPartStore) releasePartOutboxEntry(ctx context.Context, entry *partOutboxEntry.Entity) (bool, error) {
	var released bool
	err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		released, err = obs.partOutboxEntryRepository.ReleasePartOutboxEntryClaim(ctx, tx.SqlTx(), obs.outboxId, *entry.Id, obs.claimOwner, time.Now().UTC())
		return err
	})
	if err != nil {
		return false, err
	}
	return released, nil
}

func (obs *outboxPartStore) startPartOutboxHeartbeat(ctx context.Context, entry *partOutboxEntry.Entity) func() {
	interval := obs.claimLeaseDuration / 3
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
				err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
					var err error
					extended, err = obs.partOutboxEntryRepository.ExtendPartOutboxEntryClaim(ctx, tx.SqlTx(), obs.outboxId, *entry.Id, obs.claimOwner, now, now.Add(obs.claimLeaseDuration))
					return err
				})
				if err != nil {
					slog.WarnContext(ctx, "Failed to commit part outbox heartbeat", "error", err)
					continue
				}
				if !extended {
					slog.WarnContext(ctx, "Part outbox heartbeat lost claim", "entryId", entry.Id.String())
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

	var pendingCount int
	if err := database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		pendingCount, err = obs.partOutboxEntryRepository.Count(ctx, tx.SqlTx(), obs.outboxId)
		return err
	}); err != nil {
		return
	}
	obs.metrics.pendingEntries.Set(float64(pendingCount))

	for {
		var entry *partOutboxEntry.Entity
		var putPartReader io.Reader

		entry, claimed, err := obs.claimNextOutboxEntry(ctx)
		if err != nil {
			obs.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if entry == nil || !claimed {
			break
		}

		putPartReader, err = obs.readPartOutboxContent(ctx, entry)
		if err != nil {
			obs.metrics.errorsCounter.Inc()
			_, _ = obs.releasePartOutboxEntry(ctx, entry)
			time.Sleep(5 * time.Second)
			return
		}

		stopHeartbeat := obs.startPartOutboxHeartbeat(ctx, entry)
		err = database.WithTx(ctx, obs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
			switch entry.Operation {
			case partOutboxEntry.PutPartOperation:
				return obs.innerPartStore.PutPart(ctx, tx, entry.PartId, putPartReader)
			case partOutboxEntry.DeletePartOperation:
				return obs.innerPartStore.DeletePart(ctx, tx, entry.PartId)
			default:
				slog.Warn(fmt.Sprint("Invalid operation", entry.Operation, "during outbox processing."))
				return fmt.Errorf("invalid part outbox operation: %s", entry.Operation)
			}
		})
		if err != nil {
			stopHeartbeat()
			_, _ = obs.releasePartOutboxEntry(ctx, entry)
			obs.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		stopHeartbeat()

		deleted, err := obs.finalizePartOutboxEntry(ctx, entry)
		if err != nil {
			obs.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if !deleted {
			obs.metrics.errorsCounter.Inc()
			slog.Warn("Part outbox finalize skipped because claim owner no longer matched", "entryId", entry.Id.String())
			return
		}
		processedOutboxEntryCount += 1
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

func (obs *outboxPartStore) storePartOutboxEntry(ctx context.Context, tx database.Tx, operation string, partId partstore.PartId) (*ulid.ULID, error) {
	entry := partOutboxEntry.Entity{
		Operation: operation,
		PartId:    partId,
	}
	err := obs.partOutboxEntryRepository.SavePartOutboxEntry(ctx, tx.SqlTx(), obs.outboxId, &entry)
	if err != nil {
		return nil, err
	}

	tx.OnAfterCommit(func(context.Context) error {
		// Put struct{} in the channel unless it is full.
		select {
		case obs.triggerChannel <- struct{}{}:
		default:
		}
		return nil
	})

	return entry.Id, nil
}

func (obs *outboxPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.PutPart")
	defer span.End()

	entryId, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.PutPartOperation, partId)
	if err != nil {
		return err
	}

	chunkIndex := 0
	for {
		content, err := ioutils.ReadChunk(reader, chunkSize)
		if len(content) > 0 {
			chunk := partOutboxEntry.ContentChunk{
				OutboxEntryId: *entryId,
				ChunkIndex:    chunkIndex,
				Content:       content,
			}
			if saveErr := obs.partOutboxEntryRepository.SavePartOutboxContentChunk(ctx, tx.SqlTx(), &chunk); saveErr != nil {
				return saveErr
			}
			chunkIndex++
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return nil
}

// SupportsTxFreeGetPart reports whether GetPart works without an ambient
// transaction. The outbox lookup itself runs in a short internal transaction
// when tx is nil (and, in the rare case that the part is still pending in the
// outbox, that transaction is bound to the returned reader), so support only
// depends on the inner store.
func (obs *outboxPartStore) SupportsTxFreeGetPart() bool {
	return partstore.SupportsTxFreeGetPart(obs.innerPartStore)
}

func (obs *outboxPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPart")
	defer span.End()

	if tx == nil {
		return obs.getPartTxFree(ctx, partId)
	}

	lastEntry, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryByPartId(ctx, tx.SqlTx(), obs.outboxId, partId)
	if err != nil {
		return nil, err
	}
	if lastEntry != nil {
		if lastEntry.Operation == partOutboxEntry.DeletePartOperation {
			return nil, partstore.ErrPartNotFound
		}

		// Fetch the first chunk eagerly so we know whether the outbox entry holds
		// any content; the remaining chunks are loaded lazily so we never hold
		// more than one chunk in memory at a time.
		firstChunk, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunkByIndex(ctx, tx.SqlTx(), obs.outboxId, *lastEntry.Id, 0)
		if err != nil {
			return nil, err
		}
		if firstChunk != nil {
			return &lazyOutboxChunkReadCloser{
				ctx:       ctx,
				tx:        tx,
				repo:      obs.partOutboxEntryRepository,
				outboxId:  obs.outboxId,
				entryId:   *lastEntry.Id,
				nextChunk: 1,
				current:   bytes.NewReader(firstChunk.Content),
			}, nil
		}
	}
	return obs.innerPartStore.GetPart(ctx, tx, partId)
}

// getPartTxFree performs the outbox lookup in its own short read transaction.
// In the common case (part already flushed to the inner store) the
// transaction ends before this function returns and the inner store is read
// without any transaction. Only when the part is still pending in the outbox
// is the transaction kept open, bound to the returned reader's Close.
func (obs *outboxPartStore) getPartTxFree(ctx context.Context, partId partstore.PartId) (io.ReadCloser, error) {
	txController, err := obs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	releaseTx := func() {
		_ = txController.Rollback(ctx)
	}

	lastEntry, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryByPartId(ctx, txController.SqlTx(), obs.outboxId, partId)
	if err != nil {
		releaseTx()
		return nil, err
	}
	if lastEntry == nil {
		releaseTx()
		return obs.innerPartStore.GetPart(ctx, nil, partId)
	}
	if lastEntry.Operation == partOutboxEntry.DeletePartOperation {
		releaseTx()
		return nil, partstore.ErrPartNotFound
	}

	firstChunk, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunkByIndex(ctx, txController.SqlTx(), obs.outboxId, *lastEntry.Id, 0)
	if err != nil {
		releaseTx()
		return nil, err
	}
	if firstChunk == nil {
		releaseTx()
		return obs.innerPartStore.GetPart(ctx, nil, partId)
	}

	reader := &lazyOutboxChunkReadCloser{
		ctx:       ctx,
		tx:        txController,
		repo:      obs.partOutboxEntryRepository,
		outboxId:  obs.outboxId,
		entryId:   *lastEntry.Id,
		nextChunk: 1,
		current:   bytes.NewReader(firstChunk.Content),
	}
	return ioutils.NewReadCloserWithCloseHook(reader, func() error {
		return txController.Rollback(ctx)
	}), nil
}

// lazyOutboxChunkReadCloser streams an outbox entry's chunks one at a time,
// querying the next chunk from the repository only once the current one is
// exhausted. This keeps the read path's memory proportional to a single chunk
// rather than the whole part. It relies on the read transaction outliving the
// reader (GetObject binds the tx lifetime to its returned readers).
type lazyOutboxChunkReadCloser struct {
	ctx       context.Context
	tx        database.Tx
	repo      partOutboxEntry.Repository
	outboxId  string
	entryId   ulid.ULID
	nextChunk int
	current   *bytes.Reader
	done      bool
}

func (l *lazyOutboxChunkReadCloser) Read(p []byte) (int, error) {
	for {
		if l.current != nil {
			n, err := l.current.Read(p)
			if err == io.EOF {
				l.current = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		if l.done {
			return 0, io.EOF
		}
		chunk, err := l.repo.FindPartOutboxEntryChunkByIndex(l.ctx, l.tx.SqlTx(), l.outboxId, l.entryId, l.nextChunk)
		if err != nil {
			return 0, err
		}
		if chunk == nil {
			l.done = true
			return 0, io.EOF
		}
		l.nextChunk++
		l.current = bytes.NewReader(chunk.Content)
	}
}

func (l *lazyOutboxChunkReadCloser) Close() error {
	l.done = true
	l.current = nil
	return nil
}

func (obs *outboxPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPartIds")
	defer span.End()

	// We get the lastOutboxEntry for each partId
	lastOutboxEntryGroupedByPartId, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryGroupedByPartId(ctx, tx.SqlTx(), obs.outboxId)
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

func (obs *outboxPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.DeletePart")
	defer span.End()

	_, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.DeletePartOperation, partId)
	if err != nil {
		return err
	}

	return nil
}
