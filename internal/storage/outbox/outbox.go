package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type outboxMetrics struct {
	pendingEntries     prometheus.Gauge
	processedEntries   prometheus.Counter
	processingDuration prometheus.Histogram
	errorsCounter      prometheus.Counter
}

func newOutboxMetrics(registerer prometheus.Registerer) *outboxMetrics {
	m := &outboxMetrics{
		pendingEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pithos",
			Subsystem: "outbox",
			Name:      "pending_entries",
			Help:      "Number of pending outbox entries",
		}),
		processedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "outbox",
			Name:      "processed_entries_total",
			Help:      "Total number of processed outbox entries",
		}),
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pithos",
			Subsystem: "outbox",
			Name:      "processing_duration_seconds",
			Help:      "Duration of outbox processing in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		errorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pithos",
			Subsystem: "outbox",
			Name:      "errors_total",
			Help:      "Total number of outbox processing errors",
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

type outboxStorage struct {
	*lifecycle.ValidatedLifecycle
	db                           database.Database
	triggerChannel               chan struct{}
	triggerChannelClosed         bool
	outboxId                     string
	claimOwner                   string
	claimLeaseDuration           time.Duration
	outboxProcessingTaskHandle   *task.TaskHandle
	innerStorage                 storage.Storage
	storageOutboxEntryRepository storageOutboxEntry.Repository
	tracer                       trace.Tracer
	metrics                      *outboxMetrics
}

// Compile-time check to ensure outboxStorage implements storage.Storage
var _ storage.Storage = (*outboxStorage)(nil)
var _ storage.TransactionalStorage = (*outboxStorage)(nil)

const defaultClaimLeaseDuration = 30 * time.Second

// maxReplayMemoryCacheSize bounds how much of a replayed PutObject body is held
// in memory before spilling to a temp file when wrapping it in a seekable reader.
const maxReplayMemoryCacheSize = 10 * 1000 * 1000 // 10MB

func NewStorage(db database.Database, outboxId string, innerStorage storage.Storage, storageOutboxEntryRepository storageOutboxEntry.Repository, registerer prometheus.Registerer, claimLeaseDuration time.Duration) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("OutboxStorage")
	if err != nil {
		return nil, err
	}
	leaseDuration := defaultClaimLeaseDuration
	if claimLeaseDuration > 0 {
		leaseDuration = claimLeaseDuration
	}
	os := &outboxStorage{
		ValidatedLifecycle:           lifecycle,
		db:                           db,
		triggerChannel:               make(chan struct{}, 16),
		triggerChannelClosed:         false,
		outboxId:                     outboxId,
		claimOwner:                   outboxId + ":" + ulid.Make().String(),
		claimLeaseDuration:           leaseDuration,
		innerStorage:                 innerStorage,
		storageOutboxEntryRepository: storageOutboxEntryRepository,
		tracer:                       otel.Tracer("internal/storage/outbox"),
		metrics:                      newOutboxMetrics(registerer),
	}
	return os, nil
}

func (os *outboxStorage) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return database.WithTx(ctx, os.db, opts, func(ctx context.Context, tx database.Tx) error {
		return fn(ctx, os)
	})
}

func (os *outboxStorage) claimNextOutboxEntry(ctx context.Context) (*storageOutboxEntry.Entity, bool, error) {
	var entry *storageOutboxEntry.Entity
	var claimed bool
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		now := time.Now().UTC()
		var err error
		entry, claimed, err = os.storageOutboxEntryRepository.ClaimFirstStorageOutboxEntry(ctx, tx.SqlTx(), os.outboxId, os.claimOwner, now, now.Add(os.claimLeaseDuration))
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return entry, claimed, nil
}

func (os *outboxStorage) readStorageOutboxChunks(ctx context.Context, entry *storageOutboxEntry.Entity) ([]io.Reader, error) {
	if entry.Operation != storageOutboxEntry.PutObjectStorageOperation {
		return nil, nil
	}
	var chunks []*storageOutboxEntry.ContentChunk
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		chunks, err = os.storageOutboxEntryRepository.FindStorageOutboxEntryChunksById(ctx, tx.SqlTx(), os.outboxId, *entry.Id)
		return err
	})
	if err != nil {
		return nil, err
	}
	readers := make([]io.Reader, len(chunks))
	for i, chunk := range chunks {
		readers[i] = bytes.NewReader(chunk.Content)
	}
	return readers, nil
}

func (os *outboxStorage) finalizeStorageOutboxEntry(ctx context.Context, entry *storageOutboxEntry.Entity) (bool, error) {
	var deleted bool
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		deleted, err = os.storageOutboxEntryRepository.DeleteStorageOutboxEntryByClaimOwner(ctx, tx.SqlTx(), os.outboxId, *entry.Id, os.claimOwner)
		return err
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func (os *outboxStorage) releaseStorageOutboxEntry(ctx context.Context, entry *storageOutboxEntry.Entity) (bool, error) {
	var released bool
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var err error
		released, err = os.storageOutboxEntryRepository.ReleaseStorageOutboxEntryClaim(ctx, tx.SqlTx(), os.outboxId, *entry.Id, os.claimOwner, time.Now().UTC())
		return err
	})
	if err != nil {
		return false, err
	}
	return released, nil
}

func (os *outboxStorage) startStorageOutboxHeartbeat(ctx context.Context, entry *storageOutboxEntry.Entity) func() {
	interval := os.claimLeaseDuration / 3
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
				err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
					var err error
					extended, err = os.storageOutboxEntryRepository.ExtendStorageOutboxEntryClaim(ctx, tx.SqlTx(), os.outboxId, *entry.Id, os.claimOwner, now, now.Add(os.claimLeaseDuration))
					return err
				})
				if err != nil {
					slog.WarnContext(ctx, "Failed to commit storage outbox heartbeat", "error", err)
					continue
				}
				if !extended {
					slog.WarnContext(ctx, "Storage outbox heartbeat lost claim", "entryId", entry.Id.String())
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

func (os *outboxStorage) maybeProcessOutboxEntries(ctx context.Context) {
	startTime := time.Now()
	processedOutboxEntryCount := 0
	defer func() {
		os.metrics.processingDuration.Observe(time.Since(startTime).Seconds())
		if processedOutboxEntryCount > 0 {
			os.metrics.processedEntries.Add(float64(processedOutboxEntryCount))
		}
	}()

	var pendingCount int
	if err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		pendingCount, err = os.storageOutboxEntryRepository.Count(ctx, tx.SqlTx(), os.outboxId)
		return err
	}); err != nil {
		return
	}
	os.metrics.pendingEntries.Set(float64(pendingCount))

	for {
		var entry *storageOutboxEntry.Entity
		var putObjectReaders []io.Reader

		entry, claimed, err := os.claimNextOutboxEntry(ctx)
		if err != nil {
			os.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if entry == nil || !claimed {
			break
		}

		putObjectReaders, err = os.readStorageOutboxChunks(ctx, entry)
		if err != nil {
			os.metrics.errorsCounter.Inc()
			_, _ = os.releaseStorageOutboxEntry(ctx, entry)
			time.Sleep(5 * time.Second)
			return
		}

		stopHeartbeat := os.startStorageOutboxHeartbeat(ctx, entry)
		switch entry.Operation {
		case storageOutboxEntry.CreateBucketStorageOperation:
			err = os.innerStorage.CreateBucket(ctx, entry.Bucket)
		case storageOutboxEntry.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(ctx, entry.Bucket)
		case storageOutboxEntry.PutObjectStorageOperation:
			// Wrap the concatenated chunks in a seekable reader: an S3 backend (s3client)
			// needs to seek the body to compute the request checksum when the connection
			// is not over TLS, which a plain io.MultiReader cannot satisfy.
			var body io.ReadSeekCloser
			body, err = ioutils.NewSmartCachedReadSeekCloser(io.MultiReader(putObjectReaders...), maxReplayMemoryCacheSize)
			if err == nil {
				_, err = os.innerStorage.PutObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key), entry.ContentType, body, nil, nil)
				_ = body.Close()
			}
		case storageOutboxEntry.DeleteObjectStorageOperation:
			var deleteOpts *storage.DeleteObjectOptions
			if entry.VersionID != nil {
				deleteOpts = &storage.DeleteObjectOptions{VersionID: entry.VersionID}
			}
			_, err = os.innerStorage.DeleteObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key), deleteOpts)
		default:
			slog.Warn(fmt.Sprint("Invalid operation", entry.Operation, "during outbox processing."))
			err = fmt.Errorf("invalid storage outbox operation: %s", entry.Operation)
		}
		stopHeartbeat()
		if err != nil {
			_, _ = os.releaseStorageOutboxEntry(ctx, entry)
			os.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}

		deleted, err := os.finalizeStorageOutboxEntry(ctx, entry)
		if err != nil {
			os.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if !deleted {
			os.metrics.errorsCounter.Inc()
			slog.Warn("Storage outbox finalize skipped because claim owner no longer matched", "entryId", entry.Id.String())
			return
		}
		processedOutboxEntryCount += 1
	}
	if processedOutboxEntryCount > 0 {
		slog.Info(fmt.Sprintf("Processed %d outbox entries", processedOutboxEntryCount))
	}
}

func (os *outboxStorage) processOutboxLoop() {
	ctx := context.Background()
out:
	for {
		select {
		case _, ok := <-os.triggerChannel:
			if !ok {
				slog.Debug("Stopping outboxStorage processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		os.maybeProcessOutboxEntries(ctx)
	}
}

func (os *outboxStorage) Start(ctx context.Context) error {
	if err := os.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	os.outboxProcessingTaskHandle = task.Start(func(_ *atomic.Bool) {
		os.processOutboxLoop()
	})
	return os.innerStorage.Start(ctx)
}

func (os *outboxStorage) Stop(ctx context.Context) error {
	if err := os.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	if !os.triggerChannelClosed {
		close(os.triggerChannel)
		if os.outboxProcessingTaskHandle != nil {
			joinedWithTimeout := os.outboxProcessingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				slog.Debug("OutboxStorage.outboxProcessingTaskHandle joined with timeout of 30s")
			} else {
				slog.Debug("OutboxStorage.outboxProcessingTaskHandle joined without timeout")
			}
		}
		os.triggerChannelClosed = true
	}
	return os.innerStorage.Stop(ctx)
}

func (os *outboxStorage) storeStorageOutboxEntry(ctx context.Context, tx database.Tx, operation string, bucketName storage.BucketName, key string, contentType *string, versionID *string) (*ulid.ULID, error) {
	entry := storageOutboxEntry.Entity{
		Operation:   operation,
		Bucket:      bucketName,
		Key:         key,
		VersionID:   versionID,
		ContentType: contentType,
	}
	err := os.storageOutboxEntryRepository.SaveStorageOutboxEntry(ctx, tx.SqlTx(), os.outboxId, &entry)
	if err != nil {
		return nil, err
	}

	tx.OnAfterCommit(func(context.Context) error {
		// Put struct{} in the channel unless it is full.
		select {
		case os.triggerChannel <- struct{}{}:
		default:
		}
		return nil
	})

	return entry.Id, nil
}

func (os *outboxStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CreateBucket")
	defer span.End()

	return database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.CreateBucketStorageOperation, bucketName, "", nil, nil)
		return err
	})
}

func (os *outboxStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteBucket")
	defer span.End()

	return database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteBucketStorageOperation, bucketName, "", nil, nil)
		return err
	})
}

func (os *outboxStorage) waitForAllOutboxEntriesOfBucket(ctx context.Context, bucketName storage.BucketName) error {
	var lastStorageOutboxEntry *storageOutboxEntry.Entity
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		lastStorageOutboxEntry, err = os.storageOutboxEntryRepository.FindLastStorageOutboxEntryForBucket(ctx, tx.SqlTx(), os.outboxId, bucketName)
		return err
	})
	if err != nil {
		return err
	}
	if lastStorageOutboxEntry == nil {
		return nil
	}

	lastId := lastStorageOutboxEntry.Id

	for {
		var entry *storageOutboxEntry.Entity
		err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			var err error
			entry, err = os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryForBucket(ctx, tx.SqlTx(), os.outboxId, bucketName)
			return err
		})
		if err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		if (*entry.Id).Compare(*lastId) > 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (os *outboxStorage) waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	var lastStorageOutboxEntry *storageOutboxEntry.Entity
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		lastStorageOutboxEntry, err = os.storageOutboxEntryRepository.FindLastStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx, tx.SqlTx(), os.outboxId, bucketName, key.String())
		return err
	})
	if err != nil {
		return err
	}
	if lastStorageOutboxEntry == nil {
		return nil
	}

	lastId := lastStorageOutboxEntry.Id

	for {
		var entry *storageOutboxEntry.Entity
		err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			var err error
			entry, err = os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx, tx.SqlTx(), os.outboxId, bucketName, key.String())
			return err
		})
		if err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		if (*entry.Id).Compare(*lastId) > 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (os *outboxStorage) waitForAllOutboxEntries(ctx context.Context) error {
	var lastStorageOutboxEntry *storageOutboxEntry.Entity
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		lastStorageOutboxEntry, err = os.storageOutboxEntryRepository.FindLastStorageOutboxEntry(ctx, tx.SqlTx(), os.outboxId)
		return err
	})
	if err != nil {
		return err
	}
	if lastStorageOutboxEntry == nil {
		return nil
	}

	lastId := lastStorageOutboxEntry.Id

	for {
		var entry *storageOutboxEntry.Entity
		err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			var err error
			entry, err = os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(ctx, tx.SqlTx(), os.outboxId)
			return err
		})
		if err != nil {
			return err
		}
		if entry == nil {
			return nil
		}
		if (*entry.Id).Compare(*lastId) > 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (os *outboxStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListBuckets")
	defer span.End()

	err := os.waitForAllOutboxEntries(ctx)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListBuckets(ctx)
}

func (os *outboxStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.HeadBucket")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadBucket(ctx, bucketName)
}

func (os *outboxStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListObjects")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjects(ctx, bucketName, opts)
}

func (os *outboxStorage) GetBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetBucketVersioningConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetBucketVersioningConfiguration(ctx, bucketName)
}

func (os *outboxStorage) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutBucketVersioningConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.PutBucketVersioningConfiguration(ctx, bucketName, config)
}

func (os *outboxStorage) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListObjectVersions")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjectVersions(ctx, bucketName, opts)
}

func (os *outboxStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.HeadObject")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadObject(ctx, bucketName, key, opts)
}

func (os *outboxStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetObject")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, nil, err
	}

	return os.innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (os *outboxStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutObject")
	defer span.End()

	// Options that an outbox entry cannot represent must bypass the outbox:
	// conditional-write preconditions have to be evaluated now, and tag sets and
	// object metadata are not persisted with the entry (replay would drop them).
	// Drain the outbox and write through to the inner storage instead.
	putMustBeSynchronous := opts != nil && (opts.IfNoneMatchStar || opts.IfMatchETag != nil || len(opts.Tags) > 0 || opts.Metadata != nil)
	if !putMustBeSynchronous {
		// Versioning-enabled buckets must write through: an outboxed put cannot
		// return the new version id and replay would collapse version ordering.
		versioningConfig, err := os.innerStorage.GetBucketVersioningConfiguration(ctx, bucketName)
		if err != nil && !errors.Is(err, storage.ErrNoSuchBucket) {
			return nil, err
		}
		if err == nil {
			putMustBeSynchronous = versioningConfig.Status != nil && *versioningConfig.Status == storage.BucketVersioningStatusEnabled
		}
	}
	if putMustBeSynchronous {
		err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput, opts)
	}

	var calculatedChecksums *checksumutils.ChecksumValues
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, c, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
			entryId, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.PutObjectStorageOperation, bucketName, key.String(), contentType, nil)
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

					chunk := storageOutboxEntry.ContentChunk{
						OutboxEntryId: *entryId,
						ChunkIndex:    chunkIndex,
						Content:       content,
					}
					err = os.storageOutboxEntryRepository.SaveStorageOutboxContentChunk(ctx, tx.SqlTx(), &chunk)
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
		})
		if err != nil {
			return err
		}
		calculatedChecksums = c
		return metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	})
	if err != nil {
		return nil, err
	}
	return &storage.PutObjectResult{
		ETag:              calculatedChecksums.ETag,
		ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
		ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
		ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
		ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
		ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
	}, nil
}

func (os *outboxStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.AppendObject")
	defer span.End()

	// Append is always conditional (requires the object to be consistent), so
	// Append is key scoped but must respect global bucket operations.
	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.AppendObject(ctx, bucketName, key, reader, checksumInput, opts)
}

func (os *outboxStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CopyObject")
	defer span.End()

	// The copy reads the source and writes the destination synchronously, so any
	// pending outbox writes for either key must be flushed first.
	if err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, srcBucket, srcKey); err != nil {
		return nil, err
	}
	if err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, dstBucket, dstKey); err != nil {
		return nil, err
	}
	return os.innerStorage.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
}

// bucketHasVersioningStatus reports whether the bucket's versioning has ever
// been configured (Enabled or Suspended). A bucket whose creation is still
// queued in the outbox cannot have a versioning configuration yet.
func (os *outboxStorage) bucketHasVersioningStatus(ctx context.Context, bucketName storage.BucketName) (bool, error) {
	versioningConfig, err := os.innerStorage.GetBucketVersioningConfiguration(ctx, bucketName)
	if err != nil {
		if errors.Is(err, storage.ErrNoSuchBucket) {
			return false, nil
		}
		return false, err
	}
	return versioningConfig.Status != nil, nil
}

func (os *outboxStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObject")
	defer span.End()

	// ETag-conditional deletes must execute synchronously so the precondition
	// is evaluated against the current object.
	deleteMustBeSynchronous := opts != nil && opts.IfMatchETag != nil
	if !deleteMustBeSynchronous {
		// Deletes on versioning-enabled or -suspended buckets create a delete
		// marker whose version id must be returned to the caller; an outboxed
		// entry cannot represent that result.
		hasVersioningStatus, err := os.bucketHasVersioningStatus(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		deleteMustBeSynchronous = hasVersioningStatus
	}
	if deleteMustBeSynchronous {
		err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.DeleteObject(ctx, bucketName, key, opts)
	}

	var versionID *string
	if opts != nil {
		versionID = opts.VersionID
	}
	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		_, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, key.String(), nil, versionID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &storage.DeleteObjectResult{VersionID: versionID}, nil
}

func (os *outboxStorage) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.TransitionObjectStorageClass")
	defer span.End()

	// Transitions move part data and evaluate an ETag precondition against the
	// current object, so they must run synchronously against a drained outbox.
	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return err
	}
	return os.innerStorage.TransitionObjectStorageClass(ctx, bucketName, key, targetStorageClass, opts)
}

func (os *outboxStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObjects")
	defer span.End()

	deleteMustBeSynchronous := false
	for _, entry := range entries {
		if entry.IfMatchETag != nil {
			deleteMustBeSynchronous = true
			break
		}
	}
	if !deleteMustBeSynchronous {
		// Deletes on versioning-enabled or -suspended buckets create delete
		// markers whose version ids must be returned to the caller; outboxed
		// entries cannot represent that result.
		hasVersioningStatus, err := os.bucketHasVersioningStatus(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		deleteMustBeSynchronous = hasVersioningStatus
	}
	if deleteMustBeSynchronous {
		err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.DeleteObjects(ctx, bucketName, entries)
	}

	err := database.WithTx(ctx, os.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		for _, entry := range entries {
			_, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, entry.Key.String(), nil, entry.VersionID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := &storage.DeleteObjectsResult{
		Entries: make([]storage.DeleteObjectsEntry, len(entries)),
	}
	for i, entry := range entries {
		result.Entries[i] = storage.DeleteObjectsEntry{Key: entry.Key, VersionID: entry.VersionID, Deleted: true}
	}
	return result, nil
}

func (os *outboxStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CreateMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}
	return os.innerStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
}

func (os *outboxStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.UploadPart")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
}

func (os *outboxStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.UploadPartCopy")
	defer span.End()

	if err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, srcBucket, srcKey); err != nil {
		return nil, err
	}
	if err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, dstBucket, dstKey); err != nil {
		return nil, err
	}
	return os.innerStorage.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
}

func (os *outboxStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CompleteMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
}

func (os *outboxStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.AbortMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return err
	}

	return os.innerStorage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (os *outboxStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListMultipartUploads")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListMultipartUploads(ctx, bucketName, opts)
}

func (os *outboxStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListParts")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucketAndKeyIncludingGlobal(ctx, bucketName, key)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListParts(ctx, bucketName, key, uploadId, opts)
}

func (os *outboxStorage) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetBucketWebsiteConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetBucketWebsiteConfiguration(ctx, bucketName)
}

func (os *outboxStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutBucketWebsiteConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.PutBucketWebsiteConfiguration(ctx, bucketName, config)
}

func (os *outboxStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteBucketWebsiteConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.DeleteBucketWebsiteConfiguration(ctx, bucketName)
}

func (os *outboxStorage) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetBucketCORSConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetBucketCORSConfiguration(ctx, bucketName)
}

func (os *outboxStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutBucketCORSConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.PutBucketCORSConfiguration(ctx, bucketName, config)
}

func (os *outboxStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteBucketCORSConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.DeleteBucketCORSConfiguration(ctx, bucketName)
}

func (os *outboxStorage) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetBucketLifecycleConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetBucketLifecycleConfiguration(ctx, bucketName)
}

func (os *outboxStorage) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutBucketLifecycleConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.PutBucketLifecycleConfiguration(ctx, bucketName, config)
}

func (os *outboxStorage) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteBucketLifecycleConfiguration")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.DeleteBucketLifecycleConfiguration(ctx, bucketName)
}

func (os *outboxStorage) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetObjectTagging")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetObjectTagging(ctx, bucketName, key, opts)
}

func (os *outboxStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutObjectTagging")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.PutObjectTagging(ctx, bucketName, key, tags, opts)
}

func (os *outboxStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObjectTagging")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	return os.innerStorage.DeleteObjectTagging(ctx, bucketName, key, opts)
}
