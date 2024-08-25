package storage

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type OutboxStorage struct {
	db                           *sql.DB
	triggerChannel               chan struct{}
	triggerChannelClosed         bool
	outboxProcessingStopped      sync.WaitGroup
	innerStorage                 Storage
	storageOutboxEntryRepository repository.StorageOutboxEntryRepository
}

func NewOutboxStorage(db *sql.DB, innerStorage Storage) (*OutboxStorage, error) {
	os := &OutboxStorage{
		db:                           db,
		triggerChannel:               make(chan struct{}, 16),
		triggerChannelClosed:         false,
		innerStorage:                 innerStorage,
		storageOutboxEntryRepository: repository.NewStorageOutboxEntryRepository(),
	}
	return os, nil
}

func (os *OutboxStorage) maybeProcessOutboxEntries() {
	processedOutboxEntryCount := 0
	tx, err := os.db.Begin()
	if err != nil {
		return
	}
	for {
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(tx)
		if err != nil {
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		if storageOutboxEntry == nil {
			break
		}

		switch storageOutboxEntry.Operation {
		case repository.CreateBucketStorageOperation:
			err = os.innerStorage.CreateBucket(storageOutboxEntry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case repository.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(storageOutboxEntry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case repository.PutObjectStorageOperation:
			err = os.innerStorage.PutObject(storageOutboxEntry.Bucket, storageOutboxEntry.Key, bytes.NewReader(storageOutboxEntry.Data))
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case repository.DeleteObjectStorageOperation:
			err = os.innerStorage.DeleteObject(storageOutboxEntry.Bucket, storageOutboxEntry.Key)
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
		err = os.storageOutboxEntryRepository.DeleteStorageOutboxEntryById(tx, *storageOutboxEntry.Id)
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

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}

func (os *OutboxStorage) processOutboxLoop() {
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
		os.maybeProcessOutboxEntries()
	}
	os.outboxProcessingStopped.Done()
}

func (os *OutboxStorage) Start() error {
	os.outboxProcessingStopped.Add(1)
	go os.processOutboxLoop()
	return os.innerStorage.Start()
}

func (os *OutboxStorage) Stop() error {
	if !os.triggerChannelClosed {
		close(os.triggerChannel)
		waitWithTimeout(&os.outboxProcessingStopped, 5*time.Second)
		os.triggerChannelClosed = true
	}
	return os.innerStorage.Stop()
}

func (os *OutboxStorage) storeStorageOutboxEntry(tx *sql.Tx, operation string, bucket string, key string, data []byte) error {
	ordinal, err := os.storageOutboxEntryRepository.NextOrdinal(tx)
	if err != nil {
		return err
	}
	storageOutboxEntry := repository.StorageOutboxEntryEntity{
		Operation: operation,
		Bucket:    bucket,
		Key:       key,
		Data:      data,
		Ordinal:   *ordinal,
	}
	err = os.storageOutboxEntryRepository.SaveStorageOutboxEntry(tx, &storageOutboxEntry)
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

func (os *OutboxStorage) CreateBucket(bucket string) error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(tx, repository.CreateBucketStorageOperation, bucket, "", []byte{})
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

func (os *OutboxStorage) DeleteBucket(bucket string) error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(tx, repository.DeleteBucketStorageOperation, bucket, "", []byte{})
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

func (os *OutboxStorage) waitForAllOutboxEntriesOfBucket(bucket string) error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	lastStorageOutboxEntry, err := os.storageOutboxEntryRepository.FindLastStorageOutboxEntryForBucket(tx, bucket)
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
		tx, err := os.db.Begin()
		if err != nil {
			return err
		}
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntryForBucket(tx, bucket)
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

func (os *OutboxStorage) waitForAllOutboxEntries() error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	lastStorageOutboxEntry, err := os.storageOutboxEntryRepository.FindLastStorageOutboxEntry(tx)
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
		tx, err := os.db.Begin()
		if err != nil {
			return err
		}
		storageOutboxEntry, err := os.storageOutboxEntryRepository.FindFirstStorageOutboxEntry(tx)
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

func (os *OutboxStorage) ListBuckets() ([]Bucket, error) {
	err := os.waitForAllOutboxEntries()
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListBuckets()
}

func (os *OutboxStorage) HeadBucket(bucket string) (*Bucket, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadBucket(bucket)
}

func (os *OutboxStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
}

func (os *OutboxStorage) HeadObject(bucket string, key string) (*Object, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.HeadObject(bucket, key)
}

func (os *OutboxStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.GetObject(bucket, key, startByte, endByte)
}

func (os *OutboxStorage) PutObject(bucket string, key string, reader io.Reader) error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = os.storeStorageOutboxEntry(tx, repository.PutObjectStorageOperation, bucket, key, data)
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

func (os *OutboxStorage) DeleteObject(bucket string, key string) error {
	tx, err := os.db.Begin()
	if err != nil {
		return err
	}
	err = os.storeStorageOutboxEntry(tx, repository.DeleteObjectStorageOperation, bucket, key, []byte{})
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

func (os *OutboxStorage) CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}
	return os.innerStorage.CreateMultipartUpload(bucket, key)
}

func (os *OutboxStorage) UploadPart(bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.UploadPart(bucket, key, uploadId, partNumber, data)
}

func (os *OutboxStorage) CompleteMultipartUpload(bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return nil, err
	}

	return os.innerStorage.CompleteMultipartUpload(bucket, key, uploadId)
}

func (os *OutboxStorage) AbortMultipartUpload(bucket string, key string, uploadId string) error {
	err := os.waitForAllOutboxEntriesOfBucket(bucket)
	if err != nil {
		return err
	}

	return os.innerStorage.AbortMultipartUpload(bucket, key, uploadId)
}
