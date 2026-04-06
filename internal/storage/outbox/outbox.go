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

const maxOutboxReplayMemoryCacheSize = 32 * 1024 * 1024 // 32MB

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
	outboxProcessingTaskHandle   *task.TaskHandle
	innerStorage                 storage.Storage
	storageOutboxEntryRepository storageOutboxEntry.Repository
	tracer                       trace.Tracer
	metrics                      *outboxMetrics
}

// Compile-time check to ensure outboxStorage implements storage.Storage
var _ storage.Storage = (*outboxStorage)(nil)

func NewStorage(db database.Database, innerStorage storage.Storage, storageOutboxEntryRepository storageOutboxEntry.Repository, registerer prometheus.Registerer) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("OutboxStorage")
	if err != nil {
		return nil, err
	}
	os := &outboxStorage{
		ValidatedLifecycle:           lifecycle,
		db:                           db,
		triggerChannel:               make(chan struct{}, 16),
		triggerChannelClosed:         false,
		innerStorage:                 innerStorage,
		storageOutboxEntryRepository: storageOutboxEntryRepository,
		tracer:                       otel.Tracer("internal/storage/outbox"),
		metrics:                      newOutboxMetrics(registerer),
	}
	return os, nil
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

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return
	}
	pendingCount, err := os.storageOutboxEntryRepository.Count(ctx, tx)
	if err != nil {
		tx.Rollback()
		return
	}
	os.metrics.pendingEntries.Set(float64(pendingCount))
	tx.Commit()

	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			os.metrics.errorsCounter.Inc()
			return
		}
		entry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryWithForUpdateLock(ctx, tx)
		if err != nil {
			tx.Rollback()
			os.metrics.errorsCounter.Inc()
			time.Sleep(5 * time.Second)
			return
		}
		if entry == nil {
			tx.Commit()
			break
		}

		switch entry.Operation {
		case storageOutboxEntry.CreateBucketStorageOperation:
			err = os.innerStorage.CreateBucket(ctx, entry.Bucket)
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(ctx, entry.Bucket)
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.PutObjectStorageOperation:
			chunks, err := os.storageOutboxEntryRepository.FindStorageOutboxEntryChunksById(ctx, tx, *entry.Id)
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
			readers := make([]io.Reader, len(chunks))
			for i, chunk := range chunks {
				readers[i] = bytes.NewReader(chunk.Content)
			}
			readSeekCloser, err := ioutils.NewSmartCachedReadSeekCloser(io.MultiReader(readers...), maxOutboxReplayMemoryCacheSize)
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
			_, err = os.innerStorage.PutObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key), entry.ContentType, readSeekCloser, nil, nil)
			readSeekCloser.Close()
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.DeleteObjectStorageOperation:
			deleteOpts := &storage.DeleteObjectOptions{}
			if entry.VersionID != nil {
				deleteOpts.VersionID = entry.VersionID
			} else {
				deleteOpts = nil
			}
			_, err = os.innerStorage.DeleteObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key), deleteOpts)
			if err != nil {
				tx.Rollback()
				os.metrics.errorsCounter.Inc()
				time.Sleep(5 * time.Second)
				return
			}
		default:
			slog.Warn(fmt.Sprint("Invalid operation", entry.Operation, "during outbox processing."))
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		err = os.storageOutboxEntryRepository.DeleteStorageOutboxEntryById(ctx, tx, *entry.Id)
		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
		if err != nil {
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

func (os *outboxStorage) storeStorageOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, bucketName storage.BucketName, key string, versionID *string) (*ulid.ULID, error) {
	entry := storageOutboxEntry.Entity{
		Operation: operation,
		Bucket:    bucketName,
		Key:       key,
		VersionID: versionID,
	}
	err := os.storageOutboxEntryRepository.SaveStorageOutboxEntry(ctx, tx, &entry)
	if err != nil {
		return nil, err
	}

	// Put struct{} in the channel unless it is full
	select {
	case os.triggerChannel <- struct{}{}:
	default:
	}

	return entry.Id, nil
}

func (os *outboxStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CreateBucket")
	defer span.End()

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.CreateBucketStorageOperation, bucketName, "", nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (os *outboxStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteBucket")
	defer span.End()

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteBucketStorageOperation, bucketName, "", nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (os *outboxStorage) waitForAllOutboxEntriesOfBucket(ctx context.Context, bucketName storage.BucketName) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	lastStorageOutboxEntry, err := os.storageOutboxEntryRepository.FindLastStorageOutboxEntryForBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	if lastStorageOutboxEntry == nil {
		return nil
	}

	lastId := lastStorageOutboxEntry.Id

	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			return err
		}
		entry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryForBucket(ctx, tx, bucketName)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
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
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	lastStorageOutboxEntry, err := os.storageOutboxEntryRepository.FindLastStorageOutboxEntry(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	if lastStorageOutboxEntry == nil {
		return nil
	}

	lastId := lastStorageOutboxEntry.Id

	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			return err
		}
		entry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(ctx, tx)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
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

func (os *outboxStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListObjects")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjects(ctx, bucketName, opts)
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

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadObject(ctx, bucketName, key, opts)
}

func (os *outboxStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetObject")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}

	return os.innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (os *outboxStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutObject")
	defer span.End()

	putMustBeSynchronous := opts != nil && (opts.IfNoneMatchStar || opts.IfMatchETag != nil)
	if !putMustBeSynchronous {
		versioningConfig, err := os.innerStorage.GetBucketVersioningConfiguration(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		putMustBeSynchronous = versioningConfig.Status != nil && *versioningConfig.Status == storage.BucketVersioningStatusEnabled
	}

	if putMustBeSynchronous {
		err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput, opts)
	}

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}
	_, calculatedChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
		entryId, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.PutObjectStorageOperation, bucketName, key.String(), nil)
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
				err = os.storageOutboxEntryRepository.SaveStorageOutboxContentChunk(ctx, tx, &chunk)
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
		tx.Rollback()
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
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
	// flush all pending outbox entries for the bucket before delegating.
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.AppendObject(ctx, bucketName, key, reader, checksumInput, opts)
}

func (os *outboxStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObject")
	defer span.End()

	// ETag-conditional deletes must execute synchronously.
	if opts != nil && opts.IfMatchETag != nil {
		err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.DeleteObject(ctx, bucketName, key, opts)
	}

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}
	var versionID *string
	if opts != nil {
		versionID = opts.VersionID
	}
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, key.String(), versionID)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return &storage.DeleteObjectResult{VersionID: versionID}, nil
}

func (os *outboxStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObjects")
	defer span.End()

	hasETagCondition := false
	for _, entry := range entries {
		if entry.IfMatchETag != nil {
			hasETagCondition = true
			break
		}
	}
	if hasETagCondition {
		err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
		if err != nil {
			return nil, err
		}
		return os.innerStorage.DeleteObjects(ctx, bucketName, entries)
	}

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, entry.Key.String(), entry.VersionID)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()
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

func (os *outboxStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CreateMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return os.innerStorage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
}

func (os *outboxStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.UploadPart")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
}

func (os *outboxStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CompleteMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
}

func (os *outboxStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.AbortMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
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

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
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
