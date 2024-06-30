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
			time.Sleep(30 * time.Second)
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
				time.Sleep(30 * time.Second)
				return
			}
		case repository.DeleteBucketStorageOperation:
			err = os.innerStorage.DeleteBucket(storageOutboxEntry.Bucket)
			if err != nil {
				tx.Rollback()
				time.Sleep(30 * time.Second)
				return
			}
		case repository.PutObjectStorageOperation:
			err = os.innerStorage.PutObject(storageOutboxEntry.Bucket, storageOutboxEntry.Key, bytes.NewReader(storageOutboxEntry.Data))
			if err != nil {
				tx.Rollback()
				time.Sleep(30 * time.Second)
				return
			}
		case repository.DeleteObjectStorageOperation:
			err = os.innerStorage.DeleteObject(storageOutboxEntry.Bucket, storageOutboxEntry.Key)
			if err != nil {
				tx.Rollback()
				time.Sleep(30 * time.Second)
				return
			}
		default:
			log.Println("Invalid operation", storageOutboxEntry.Operation, "during outbox processing.")
			tx.Rollback()
			time.Sleep(30 * time.Second)
			return
		}
		err = os.storageOutboxEntryRepository.DeleteStorageOutboxEntryById(tx, *storageOutboxEntry.Id)
		if err != nil {
			tx.Rollback()
			return
		}
		processedOutboxEntryCount += 1
	}
	tx.Commit()
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
				log.Println("Stopping outbox processing")
				break out
			}
		case <-time.After(10 * time.Second):
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
		waitWithTimeout(&os.outboxProcessingStopped, 10*time.Second)
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
	tx.Commit()
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
	tx.Commit()
	return nil
}

func (os *OutboxStorage) ListBuckets() ([]Bucket, error) {
	return os.innerStorage.ListBuckets()
}

func (os *OutboxStorage) HeadBucket(bucket string) (*Bucket, error) {
	return os.innerStorage.HeadBucket(bucket)
}

func (os *OutboxStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	return os.innerStorage.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
}

func (os *OutboxStorage) HeadObject(bucket string, key string) (*Object, error) {
	return os.innerStorage.HeadObject(bucket, key)
}

func (os *OutboxStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
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
	tx.Commit()
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
	tx.Commit()
	return nil
}
