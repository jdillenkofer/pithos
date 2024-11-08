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
	"github.com/jdillenkofer/pithos/internal/storage/repository/bloboutboxentry"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/oklog/ulid/v2"
)

type OutboxBlobStore struct {
	db                         *sql.DB
	triggerChannel             chan struct{}
	triggerChannelClosed       bool
	outboxProcessingTaskHandle *task.TaskHandle
	innerBlobStore             BlobStore
	blobOutboxEntryRepository  *bloboutboxentry.BlobOutboxEntryRepository
}

func NewOutboxBlobStore(db *sql.DB, innerBlobStore BlobStore) (*OutboxBlobStore, error) {
	blobOutboxEntryRepository, err := bloboutboxentry.New(db)
	if err != nil {
		return nil, err
	}
	obs := &OutboxBlobStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 16),
		triggerChannelClosed:      false,
		innerBlobStore:            innerBlobStore,
		blobOutboxEntryRepository: blobOutboxEntryRepository,
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
		case bloboutboxentry.PutBlobOperation:
			_, err := obs.innerBlobStore.PutBlob(ctx, tx, blobOutboxEntry.BlobId, bytes.NewReader(blobOutboxEntry.Content))
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case bloboutboxentry.DeleteBlobOperation:
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
	blobOutboxEntry := bloboutboxentry.BlobOutboxEntryEntity{
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

	err = obs.storeBlobOutboxEntry(ctx, tx, bloboutboxentry.PutBlobOperation, blobId, content)
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
		case bloboutboxentry.PutBlobOperation:
			return ioutils.NewByteReadSeekCloser(lastBlobOutboxEntry.Content), nil
		case bloboutboxentry.DeleteBlobOperation:
			return nil, ErrBlobNotFound
		}
	}
	return obs.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (obs *OutboxBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error) {
	// We get the lastOutboxEntry for each blobId
	lastOutboxEntryGroupedByBlobId, err := obs.blobOutboxEntryRepository.FindLastBlobOutboxEntryGroupedByBlobId(ctx, tx)
	if err != nil {
		return nil, err
	}
	// These are the blobIds already committed to the innerStorage
	// might be more than we actually currently have...
	innerBlobIds, err := obs.innerBlobStore.GetBlobIds(ctx, tx)
	if err != nil {
		return nil, err
	}

	allBlobIds := make(map[ulid.ULID]struct{})
	// write them all into the set
	for _, blobId := range innerBlobIds {
		allBlobIds[blobId] = struct{}{}
	}

	// then remove the once that were deleted in the outbox
	// and add the once that were added to the outbox
	for _, outboxEntry := range lastOutboxEntryGroupedByBlobId {
		switch outboxEntry.Operation {
		case bloboutboxentry.DeleteBlobOperation:
			delete(allBlobIds, outboxEntry.BlobId)
		case bloboutboxentry.PutBlobOperation:
			allBlobIds[outboxEntry.BlobId] = struct{}{}
		}
	}

	// convert the set back to a list
	blobIds := []BlobId{}
	for blobId := range allBlobIds {
		blobIds = append(blobIds, blobId)
	}
	return blobIds, nil
}

func (obs *OutboxBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	err := obs.storeBlobOutboxEntry(ctx, tx, bloboutboxentry.DeleteBlobOperation, blobId, []byte{})
	if err != nil {
		return err
	}

	return nil
}
