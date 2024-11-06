package blob

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
	"github.com/jdillenkofer/pithos/internal/task"
)

type OutboxBlobStore struct {
	db                         *sql.DB
	triggerChannel             chan struct{}
	triggerChannelClosed       bool
	outboxProcessingTaskHandle *task.TaskHandle
	innerBlobStore             BlobStore
	blobOutboxEntryRepository  repository.BlobOutboxEntryRepository
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

func (obs *OutboxBlobStore) maybeProcessOutboxEntries(ctx context.Context) {
	defer trace.StartRegion(ctx, "OutboxBlobStore.maybeProcessOutboxEntries()").End()
	processedOutboxEntryCount := 0
	tx, err := obs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return
	}
	for {
		blobOutboxEntry, err := obs.blobOutboxEntryRepository.FindFirstBlobOutboxEntry(ctx, tx)
		if err != nil {
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		if blobOutboxEntry == nil {
			break
		}

		switch blobOutboxEntry.Operation {
		case repository.PutBlobOperation:
			_, err := obs.innerBlobStore.PutBlob(ctx, tx, blobOutboxEntry.BlobId, bytes.NewReader(blobOutboxEntry.Content))
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case repository.DeleteBlobOperation:
			err = obs.innerBlobStore.DeleteBlob(ctx, tx, blobOutboxEntry.BlobId)
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		default:
			log.Println("Invalid operation", blobOutboxEntry.Operation, "during outbox processing.")
			tx.Rollback()
			time.Sleep(5 * time.Second)
			return
		}
		err = obs.blobOutboxEntryRepository.DeleteBlobOutboxEntryById(ctx, tx, *blobOutboxEntry.Id)
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

func (obs *OutboxBlobStore) processOutboxLoop() {
	ctx := context.Background()
	ctx, task := trace.NewTask(ctx, "OutboxBlobStore.processOutboxLoop()")
	defer task.End()
out:
	for {
		select {
		case _, ok := <-obs.triggerChannel:
			if !ok {
				log.Println("Stopping OutboxBlobStore processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		obs.maybeProcessOutboxEntries(ctx)
	}
}

func (obs *OutboxBlobStore) Start(ctx context.Context) error {
	obs.outboxProcessingTaskHandle = task.Start(func(_ *atomic.Bool) {
		obs.processOutboxLoop()
	})
	return obs.innerBlobStore.Start(ctx)
}

func (obs *OutboxBlobStore) Stop(ctx context.Context) error {
	if !obs.triggerChannelClosed {
		close(obs.triggerChannel)
		if obs.outboxProcessingTaskHandle != nil {
			joinedWithTimeout := obs.outboxProcessingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				log.Println("OutboxBlobStore.outboxProcessingTaskHandle joined with timeout of 30s")
			} else {
				log.Println("OutboxBlobStore.outboxProcessingTaskHandle joined without timeout")
			}
		}
		obs.triggerChannelClosed = true
	}
	return obs.innerBlobStore.Stop(ctx)
}

func (obs *OutboxBlobStore) storeBlobOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, blobId BlobId, content []byte) error {
	ordinal, err := obs.blobOutboxEntryRepository.NextOrdinal(ctx, tx)
	if err != nil {
		return err
	}
	blobOutboxEntry := repository.BlobOutboxEntryEntity{
		Operation: operation,
		BlobId:    blobId,
		Content:   content,
		Ordinal:   *ordinal,
	}
	err = obs.blobOutboxEntryRepository.SaveBlobOutboxEntry(ctx, tx, &blobOutboxEntry)
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

func (obs *OutboxBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
	content, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}

	err = obs.storeBlobOutboxEntry(ctx, tx, repository.PutBlobOperation, blobId, content)
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

func (obs *OutboxBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	lastBlobOutboxEntry, err := obs.blobOutboxEntryRepository.FindLastBlobOutboxEntryByBlobId(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}
	if lastBlobOutboxEntry != nil {
		switch lastBlobOutboxEntry.Operation {
		case repository.PutBlobOperation:
			return ioutils.NewByteReadSeekCloser(lastBlobOutboxEntry.Content), nil
		}
	}
	return obs.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (obs *OutboxBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error) {
	return obs.innerBlobStore.GetBlobIds(ctx, tx)
}

func (obs *OutboxBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	err := obs.storeBlobOutboxEntry(ctx, tx, repository.DeleteBlobOperation, blobId, []byte{})
	if err != nil {
		return err
	}

	return nil
}
