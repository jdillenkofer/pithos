package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/outboxruntime"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type outboxStorage struct {
	*lifecycle.ValidatedLifecycle
	db                           database.Database
	runtime                      *outboxruntime.Runtime[*storageOutboxEntry.Entity]
	innerStorage                 storage.Storage
	storageOutboxEntryRepository storageOutboxEntry.Repository
	tracer                       trace.Tracer
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
		innerStorage:                 innerStorage,
		storageOutboxEntryRepository: storageOutboxEntryRepository,
		tracer:                       otel.Tracer("internal/storage/outbox"),
	}
	os.runtime = outboxruntime.New(db, os, outboxruntime.NewMetrics(registerer, "outbox", "Number of pending outbox entries", "Total number of processed outbox entries", "Duration of outbox processing in seconds", "Total number of outbox processing errors"))
	return os, nil
}

func (os *outboxStorage) Start(ctx context.Context) error {
	if err := os.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	os.runtime.Start()
	return os.innerStorage.Start(ctx)
}

func (os *outboxStorage) Stop(ctx context.Context) error {
	if err := os.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	os.runtime.Stop()
	return os.innerStorage.Stop(ctx)
}

func (os *outboxStorage) Name() string {
	return "OutboxStorage"
}

func (os *outboxStorage) CountPending(ctx context.Context, tx *sql.Tx) (int, error) {
	return os.storageOutboxEntryRepository.Count(ctx, tx)
}

func (os *outboxStorage) FindFirstForUpdate(ctx context.Context, tx *sql.Tx) (*storageOutboxEntry.Entity, error) {
	return os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryWithForUpdateLock(ctx, tx)
}

func (os *outboxStorage) ProcessEntry(ctx context.Context, tx *sql.Tx, storageEntry *storageOutboxEntry.Entity) error {
	if storageEntry == nil {
		return fmt.Errorf("invalid storage outbox entry type")
	}

	switch storageEntry.Operation {
	case storageOutboxEntry.CreateBucketStorageOperation:
		return os.innerStorage.CreateBucket(ctx, storageEntry.Bucket)
	case storageOutboxEntry.DeleteBucketStorageOperation:
		return os.innerStorage.DeleteBucket(ctx, storageEntry.Bucket)
	case storageOutboxEntry.PutObjectStorageOperation:
		chunks, err := os.storageOutboxEntryRepository.FindStorageOutboxEntryChunksById(ctx, tx, *storageEntry.Id)
		if err != nil {
			return err
		}
		readers := make([]io.Reader, len(chunks))
		for i, chunk := range chunks {
			readers[i] = bytes.NewReader(chunk.Content)
		}
		_, err = os.innerStorage.PutObject(ctx, storageEntry.Bucket, storage.MustNewObjectKey(storageEntry.Key), storageEntry.ContentType, io.MultiReader(readers...), nil, nil)
		return err
	case storageOutboxEntry.DeleteObjectStorageOperation:
		return os.innerStorage.DeleteObject(ctx, storageEntry.Bucket, storage.MustNewObjectKey(storageEntry.Key), nil)
	default:
		slog.Warn(fmt.Sprint("Invalid operation", storageEntry.Operation, "during outbox processing."))
		return fmt.Errorf("invalid operation: %s", storageEntry.Operation)
	}
}

func (os *outboxStorage) DeleteEntry(ctx context.Context, tx *sql.Tx, storageEntry *storageOutboxEntry.Entity) error {
	if storageEntry == nil {
		return fmt.Errorf("invalid storage outbox entry type")
	}
	return os.storageOutboxEntryRepository.DeleteStorageOutboxEntryById(ctx, tx, *storageEntry.Id)
}

func (os *outboxStorage) storeStorageOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, bucketName storage.BucketName, key string) (*ulid.ULID, error) {
	entry := storageOutboxEntry.Entity{
		Operation: operation,
		Bucket:    bucketName,
		Key:       key,
	}
	err := os.storageOutboxEntryRepository.SaveStorageOutboxEntry(ctx, tx, &entry)
	if err != nil {
		return nil, err
	}

	// Put struct{} in the channel unless it is full
	os.runtime.Trigger()

	return entry.Id, nil
}

func (os *outboxStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CreateBucket")
	defer span.End()

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.CreateBucketStorageOperation, bucketName, "")
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
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteBucketStorageOperation, bucketName, "")
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

func (os *outboxStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.ListObjects")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjects(ctx, bucketName, opts)
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

	if opts != nil && (opts.IfNoneMatchStar || opts.IfMatchETag != nil) {
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
		entryId, err := os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.PutObjectStorageOperation, bucketName, key.String())
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

func (os *outboxStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObject")
	defer span.End()

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, key.String())
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

func (os *outboxStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.DeleteObjects")
	defer span.End()

	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		_, err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntry.DeleteObjectStorageOperation, bucketName, entry.Key.String())
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
		result.Entries[i] = storage.DeleteObjectsEntry{Key: entry.Key, Deleted: true}
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
