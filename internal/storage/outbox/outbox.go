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
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type outboxStorage struct {
	*lifecycle.ValidatedLifecycle
	db                           database.Database
	triggerChannel               chan struct{}
	triggerChannelClosed         bool
	outboxProcessingTaskHandle   *task.TaskHandle
	innerStorage                 storage.Storage
	storageOutboxEntryRepository storageOutboxEntry.Repository
	tracer                       trace.Tracer
}

// Compile-time check to ensure outboxStorage implements storage.Storage
var _ storage.Storage = (*outboxStorage)(nil)

func NewStorage(db database.Database, innerStorage storage.Storage, storageOutboxEntryRepository storageOutboxEntry.Repository) (storage.Storage, error) {
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
	}
	return os, nil
}

func (os *outboxStorage) maybeProcessOutboxEntries(ctx context.Context) {
	processedOutboxEntryCount := 0
	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			return
		}
		entry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryWithForUpdateLock(ctx, tx)
		if err != nil {
			tx.Rollback()
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
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(ctx, entry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.PutObjectStorageOperation:
			chunks, err := os.storageOutboxEntryRepository.FindStorageOutboxEntryChunksById(ctx, tx, *entry.Id)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
			readers := make([]io.Reader, len(chunks))
			for i, chunk := range chunks {
				readers[i] = bytes.NewReader(chunk.Content)
			}
			_, err = os.innerStorage.PutObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key), entry.ContentType, io.MultiReader(readers...), nil)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntry.DeleteObjectStorageOperation:
			err = os.innerStorage.DeleteObject(ctx, entry.Bucket, storage.MustNewObjectKey(entry.Key))
			if err != nil {
				tx.Rollback()
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

func (os *outboxStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.HeadObject")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadObject(ctx, bucketName, key)
}

func (os *outboxStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.GetObject")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}

	return os.innerStorage.GetObject(ctx, bucketName, key, ranges)
}

const chunkSize = 64 * 1024 * 1024 // 64MB

func (os *outboxStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.PutObject")
	defer span.End()

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

func (os *outboxStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
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

func (os *outboxStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := os.tracer.Start(ctx, "OutboxStorage.CompleteMultipartUpload")
	defer span.End()

	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
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
