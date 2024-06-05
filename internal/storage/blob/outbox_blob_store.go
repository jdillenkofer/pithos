package blob

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type OutboxBlobStore struct {
	db                        *sql.DB
	triggerChannel            chan struct{}
	innerBlobStore            BlobStore
	blobOutboxEntryRepository repository.BlobOutboxEntryRepository
}

func NewOutboxBlobStore(db *sql.DB, innerBlobStore BlobStore) (*OutboxBlobStore, error) {
	obs := &OutboxBlobStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 1000),
		innerBlobStore:            innerBlobStore,
		blobOutboxEntryRepository: repository.NewBlobOutboxEntryRepository(),
	}
	return obs, nil
}

func (obs *OutboxBlobStore) maybeProcessOutboxEntries() {
	log.Println("Processing one outbox entries")
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
			log.Println("No more outbox entries found")
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
			log.Println("Invalid operation", blobOutboxEntry.Operation, "during outbox processing. Skipping...")
		}
		err = obs.blobOutboxEntryRepository.DeleteBlobOutboxEntryById(tx, *blobOutboxEntry.Id)
		if err != nil {
			tx.Rollback()
			return
		}
	}
	tx.Commit()
	log.Println("Processed one outbox entry")
}

func (obs *OutboxBlobStore) processOutboxLoop() {
	for {
		select {
		case _, ok := <-obs.triggerChannel:
			if ok {
				obs.maybeProcessOutboxEntries()
			} else {
				log.Println("Stopping outbox processing")
				return
			}
		case <-time.After(10 * time.Second):
			obs.maybeProcessOutboxEntries()
		}
	}
}

func (obs *OutboxBlobStore) Start() error {
	go obs.processOutboxLoop()
	return obs.innerBlobStore.Start()
}

func (obs *OutboxBlobStore) Stop() error {
	close(obs.triggerChannel)
	return obs.innerBlobStore.Stop()
}

func (obs *OutboxBlobStore) PutBlob(tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
	content, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}
	ordinal, err := obs.blobOutboxEntryRepository.NextOrdinal(tx)
	if err != nil {
		return nil, err
	}
	blobOutboxEntry := repository.BlobOutboxEntryEntity{
		Operation: repository.PutOperation,
		BlobId:    blobId,
		Content:   content,
		Ordinal:   *ordinal,
	}
	err = obs.blobOutboxEntryRepository.SaveBlobOutboxEntry(tx, &blobOutboxEntry)
	if err != nil {
		return nil, err
	}
	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}
	obs.triggerChannel <- struct{}{}

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
	ordinal, err := obs.blobOutboxEntryRepository.NextOrdinal(tx)
	if err != nil {
		return err
	}
	blobOutboxEntry := repository.BlobOutboxEntryEntity{
		Operation: repository.DeleteOperation,
		BlobId:    blobId,
		Ordinal:   *ordinal,
		Content:   []byte{},
	}
	err = obs.blobOutboxEntryRepository.SaveBlobOutboxEntry(tx, &blobOutboxEntry)
	if err != nil {
		return err
	}
	obs.triggerChannel <- struct{}{}
	return nil
}
