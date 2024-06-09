package blob

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type OutboxBlobStore struct {
	db                        *sql.DB
	triggerChannel            chan struct{}
	triggerChannelClosed      bool
	outboxProcessingStopped   sync.WaitGroup
	innerBlobStore            BlobStore
	blobOutboxEntryRepository repository.BlobOutboxEntryRepository
}

func NewOutboxBlobStore(db *sql.DB, innerBlobStore BlobStore) (*OutboxBlobStore, error) {
	obs := &OutboxBlobStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 16),
		triggerChannelClosed:      false,
		innerBlobStore:            innerBlobStore,
		blobOutboxEntryRepository: repository.NewBlobOutboxEntryRepository(),
	}
	return obs, nil
}

func (obs *OutboxBlobStore) maybeProcessOutboxEntries() {
	processedOutboxEntryCount := 0
	tx, err := obs.db.Begin()
	if err != nil {
		return
	}
	for {
		blobOutboxEntry, err := obs.blobOutboxEntryRepository.FindFirstBlobOutboxEntry(tx)
		if err != nil {
			tx.Rollback()
			time.Sleep(30 * time.Second)
			return
		}
		if blobOutboxEntry == nil {
			break
		}

		switch blobOutboxEntry.Operation {
		case repository.PutOperation:
			_, err := obs.innerBlobStore.PutBlob(tx, blobOutboxEntry.BlobId, bytes.NewReader(blobOutboxEntry.Content))
			if err != nil {
				tx.Rollback()
				time.Sleep(30 * time.Second)
				return
			}
		case repository.DeleteOperation:
			err = obs.innerBlobStore.DeleteBlob(tx, blobOutboxEntry.BlobId)
			if err != nil {
				tx.Rollback()
				time.Sleep(30 * time.Second)
				return
			}
		default:
			log.Println("Invalid operation", blobOutboxEntry.Operation, "during outbox processing.")
			tx.Rollback()
			time.Sleep(30 * time.Second)
			return
		}
		err = obs.blobOutboxEntryRepository.DeleteBlobOutboxEntryById(tx, *blobOutboxEntry.Id)
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

func (obs *OutboxBlobStore) processOutboxLoop() {
out:
	for {
		select {
		case _, ok := <-obs.triggerChannel:
			if !ok {
				log.Println("Stopping outbox processing")
				break out
			}
		case <-time.After(10 * time.Second):
		}
		obs.maybeProcessOutboxEntries()
	}
	obs.outboxProcessingStopped.Done()
}

func (obs *OutboxBlobStore) Start() error {
	obs.outboxProcessingStopped.Add(1)
	go obs.processOutboxLoop()
	return obs.innerBlobStore.Start()
}

func (obs *OutboxBlobStore) Stop() error {
	if !obs.triggerChannelClosed {
		close(obs.triggerChannel)
		waitWithTimeout(&obs.outboxProcessingStopped, 10*time.Second)
		obs.triggerChannelClosed = true
	}
	return obs.innerBlobStore.Stop()
}

func (obs *OutboxBlobStore) storeBlobOutboxEntry(tx *sql.Tx, operation string, blobId BlobId, content []byte) error {
	ordinal, err := obs.blobOutboxEntryRepository.NextOrdinal(tx)
	if err != nil {
		return err
	}
	blobOutboxEntry := repository.BlobOutboxEntryEntity{
		Operation: operation,
		BlobId:    blobId,
		Content:   content,
		Ordinal:   *ordinal,
	}
	err = obs.blobOutboxEntryRepository.SaveBlobOutboxEntry(tx, &blobOutboxEntry)
	if err != nil {
		return err
	}

	// Put struct{} in the channel unless it is full
	select {
	case obs.triggerChannel <- struct{}{}:
	default:
	}

	return nil
}

func (obs *OutboxBlobStore) PutBlob(tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
	content, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}

	err = obs.storeBlobOutboxEntry(tx, repository.PutOperation, blobId, content)
	if err != nil {
		return nil, err
	}

	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	return &PutBlobResult{
		BlobId: blobId,
		ETag:   *etag,
		Size:   int64(len(content)),
	}, nil
}

func (obs *OutboxBlobStore) GetBlob(tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	lastBlobOutboxEntry, err := obs.blobOutboxEntryRepository.FindLastBlobOutboxEntryByBlobId(tx, blobId)
	if err != nil {
		return nil, err
	}
	if lastBlobOutboxEntry != nil {
		switch lastBlobOutboxEntry.Operation {
		case repository.PutOperation:
			return ioutils.NewByteReadSeekCloser(lastBlobOutboxEntry.Content), nil
		}
	}
	return obs.innerBlobStore.GetBlob(tx, blobId)
}

func (obs *OutboxBlobStore) DeleteBlob(tx *sql.Tx, blobId BlobId) error {
	err := obs.storeBlobOutboxEntry(tx, repository.DeleteOperation, blobId, []byte{})
	if err != nil {
		return err
	}

	return nil
}
