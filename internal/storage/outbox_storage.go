package storage

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log"
	"runtime/trace"
	"sync/atomic"
	"time"

	storageOutboxEntryRepository "github.com/jdillenkofer/pithos/internal/storage/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
	"github.com/jdillenkofer/pithos/internal/task"
)

type OutboxStorage struct {
	db                           *sql.DB
	triggerChannel               chan struct{}
	triggerChannelClosed         bool
	outboxProcessingTaskHandle   *task.TaskHandle
	innerStorage                 Storage
	storageOutboxEntryRepository *storageOutboxEntryRepository.StorageOutboxEntryRepository
	startStopValidator           *startstopvalidator.StartStopValidator
}

func NewOutboxStorage(db *sql.DB, innerStorage Storage) (*OutboxStorage, error) {
	startStopValidator, err := startstopvalidator.New("OutboxStorage")
	if err != nil {
		return nil, err
	}
	storageOutboxEntryRepository, err := storageOutboxEntryRepository.New(db)
	if err != nil {
		return nil, err
	}
	os := &OutboxStorage{
		db:                           db,
		triggerChannel:               make(chan struct{}, 16),
		triggerChannelClosed:         false,
		innerStorage:                 innerStorage,
		storageOutboxEntryRepository: storageOutboxEntryRepository,
		startStopValidator:           startStopValidator,
	}
	return os, nil
}

func (os *OutboxStorage) maybeProcessOutboxEntries(ctx context.Context) {
	defer trace.StartRegion(ctx, "OutboxStorage.maybeProcessOutboxEntries()").End()
	processedOutboxEntryCount := 0
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return
	}
	for {
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(ctx, tx)
		if err != nil {
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		if storageOutboxEntry == nil {
			break
		}

		switch storageOutboxEntry.Operation {
		case storageOutboxEntryRepository.CreateBucketStorageOperation:
			err = os.innerStorage.CreateBucket(ctx, storageOutboxEntry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntryRepository.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(ctx, storageOutboxEntry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntryRepository.PutObjectStorageOperation:
			err = os.innerStorage.PutObject(ctx, storageOutboxEntry.Bucket, storageOutboxEntry.Key, bytes.NewReader(storageOutboxEntry.Data))
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case storageOutboxEntryRepository.DeleteObjectStorageOperation:
			err = os.innerStorage.DeleteObject(ctx, storageOutboxEntry.Bucket, storageOutboxEntry.Key)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		default:
			log.Println("Invalid operation", storageOutboxEntry.Operation, "during outbox processing.")
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		err = os.storageOutboxEntryRepository.DeleteStorageOutboxEntryById(ctx, tx, *storageOutboxEntry.Id)
		if err != nil {
			tx.Rollback()
			return
		}
		processedOutboxEntryCount += 1
	}
	err = tx.Commit()
	if err != nil {
		return
	}
	if processedOutboxEntryCount > 0 {
		log.Printf("Processed %d outbox entries\n", processedOutboxEntryCount)
	}
}

func (os *OutboxStorage) processOutboxLoop() {
	ctx := context.Background()
	ctx, task := trace.NewTask(ctx, "OutboxStorage.processOutboxLoop()")
	defer task.End()
out:
	for {
		select {
		case _, ok := <-os.triggerChannel:
			if !ok {
				log.Println("Stopping outboxStorage processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		os.maybeProcessOutboxEntries(ctx)
	}
}

func (os *OutboxStorage) Start(ctx context.Context) error {
	err := os.startStopValidator.Start()
	if err != nil {
		return err
	}
	os.outboxProcessingTaskHandle = task.Start(func(_ *atomic.Bool) {
		os.processOutboxLoop()
	})
	err = os.innerStorage.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (os *OutboxStorage) Stop(ctx context.Context) error {
	err := os.startStopValidator.Stop()
	if err != nil {
		return err
	}
	if !os.triggerChannelClosed {
		close(os.triggerChannel)
		if os.outboxProcessingTaskHandle != nil {
			joinedWithTimeout := os.outboxProcessingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				log.Println("OutboxStorage.outboxProcessingTaskHandle joined with timeout of 30s")
			} else {
				log.Println("OutboxStorage.outboxProcessingTaskHandle joined without timeout")
			}
		}
		os.triggerChannelClosed = true
	}
	err = os.innerStorage.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (os *OutboxStorage) storeStorageOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, bucket string, key string, data []byte) error {
	ordinal, err := os.storageOutboxEntryRepository.NextOrdinal(ctx, tx)
	if err != nil {
		return err
	}
	storageOutboxEntry := storageOutboxEntryRepository.StorageOutboxEntryEntity{
		Operation: operation,
		Bucket:    bucket,
		Key:       key,
		Data:      data,
		Ordinal:   *ordinal,
	}
	err = os.storageOutboxEntryRepository.SaveStorageOutboxEntry(ctx, tx, &storageOutboxEntry)
	if err != nil {
		return err
	}

	// Put struct{} in the channel unless it is full
	select {
	case os.triggerChannel <- struct{}{}:
	default:
	}

	return nil
}

func (os *OutboxStorage) CreateBucket(ctx context.Context, bucket string) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntryRepository.CreateBucketStorageOperation, bucket, "", []byte{})
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

func (os *OutboxStorage) DeleteBucket(ctx context.Context, bucket string) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntryRepository.DeleteBucketStorageOperation, bucket, "", []byte{})
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

func (os *OutboxStorage) waitForAllOutboxEntriesOfBucket(ctx context.Context, bucket string) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	lastStorageOutboxEntry, err := os.storageOutboxEntryRepository.FindLastStorageOutboxEntryForBucket(ctx, tx, bucket)
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

	lastOrdinal := lastStorageOutboxEntry.Ordinal

	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryForBucket(ctx, tx, bucket)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		if storageOutboxEntry == nil {
			return nil
		}
		if storageOutboxEntry.Ordinal > lastOrdinal {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (os *OutboxStorage) waitForAllOutboxEntries(ctx context.Context) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
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

	lastOrdinal := lastStorageOutboxEntry.Ordinal

	for {
		tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(ctx, tx)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		if storageOutboxEntry == nil {
			return nil
		}
		if storageOutboxEntry.Ordinal > lastOrdinal {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (os *OutboxStorage) ListBuckets(ctx context.Context) ([]Bucket, error) {
	err := os.waitForAllOutboxEntries(ctx)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListBuckets(ctx)
}

func (os *OutboxStorage) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadBucket(ctx, bucket)
}

func (os *OutboxStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeys)
}

func (os *OutboxStorage) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadObject(ctx, bucket, key)
}

func (os *OutboxStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetObject(ctx, bucket, key, startByte, endByte)
}

func (os *OutboxStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntryRepository.PutObjectStorageOperation, bucket, key, data)
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

func (os *OutboxStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	tx, err := os.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(ctx, tx, storageOutboxEntryRepository.DeleteObjectStorageOperation, bucket, key, []byte{})
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

func (os *OutboxStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return os.innerStorage.CreateMultipartUpload(ctx, bucket, key)
}

func (os *OutboxStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.UploadPart(ctx, bucket, key, uploadId, partNumber, data)
}

func (os *OutboxStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
}

func (os *OutboxStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	err := os.waitForAllOutboxEntriesOfBucket(ctx, bucket)
	if err != nil {
		return err
	}

	return os.innerStorage.AbortMultipartUpload(ctx, bucket, key, uploadId)
}
