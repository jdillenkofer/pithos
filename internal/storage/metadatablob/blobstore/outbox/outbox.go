package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	blobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/task"
)

type outboxBlobStore struct {
	db                         database.Database
	triggerChannel             chan struct{}
	triggerChannelClosed       bool
	outboxProcessingTaskHandle *task.TaskHandle
	innerBlobStore             blobstore.BlobStore
	blobOutboxEntryRepository  blobOutboxEntry.Repository
}

var _ blobstore.BlobStore = (*outboxBlobStore)(nil)

func New(db database.Database, innerBlobStore blobstore.BlobStore, blobOutboxEntryRepository blobOutboxEntry.Repository) (blobstore.BlobStore, error) {
	return &outboxBlobStore{
		db:                        db,
		triggerChannel:            make(chan struct{}, 16),
		triggerChannelClosed:      false,
		innerBlobStore:            innerBlobStore,
		blobOutboxEntryRepository: blobOutboxEntryRepository,
	}, nil
}

func (obs *outboxBlobStore) maybeProcessOutboxEntries(ctx context.Context) {
	defer trace.StartRegion(ctx, "OutboxBlobStore.maybeProcessOutboxEntries()").End()
	processedOutboxEntryCount := 0
	for {
		tx, err := obs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		if err != nil {
			return
		}

		entry, err := obs.blobOutboxEntryRepository.FindFirstBlobOutboxEntryWithForUpdateLock(ctx, tx)
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
		case blobOutboxEntry.PutBlobOperation:
			err = obs.innerBlobStore.PutBlob(ctx, tx, entry.BlobId, bytes.NewReader(entry.Content))
			if err != nil {
				tx.Rollback()
				time.Sleep(5 * time.Second)
				return
			}
		case blobOutboxEntry.DeleteBlobOperation:
			err = obs.innerBlobStore.DeleteBlob(ctx, tx, entry.BlobId)
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
		err = obs.blobOutboxEntryRepository.DeleteBlobOutboxEntryById(ctx, tx, *entry.Id)
		if err != nil {
			tx.Rollback()
			return
		}
		processedOutboxEntryCount += 1

		err = tx.Commit()
		if err != nil {
			return
		}
	}
	if processedOutboxEntryCount > 0 {
		slog.Debug(fmt.Sprintf("Processed %d outbox entries", processedOutboxEntryCount))
	}
}

func (obs *outboxBlobStore) processOutboxLoop() {
	ctx := context.Background()
	ctx, task := trace.NewTask(ctx, "OutboxBlobStore.processOutboxLoop()")
	defer task.End()
out:
	for {
		select {
		case _, ok := <-obs.triggerChannel:
			if !ok {
				slog.Debug("Stopping OutboxBlobStore processing")
				break out
			}
		case <-time.After(1 * time.Second):
		}
		obs.maybeProcessOutboxEntries(ctx)
	}
}

func (obs *outboxBlobStore) Start(ctx context.Context) error {
	obs.outboxProcessingTaskHandle = task.Start(func(_ *atomic.Bool) {
		obs.processOutboxLoop()
	})
	return obs.innerBlobStore.Start(ctx)
}

func (obs *outboxBlobStore) Stop(ctx context.Context) error {
	if !obs.triggerChannelClosed {
		close(obs.triggerChannel)
		if obs.outboxProcessingTaskHandle != nil {
			joinedWithTimeout := obs.outboxProcessingTaskHandle.JoinWithTimeout(30 * time.Second)
			if joinedWithTimeout {
				slog.Debug("OutboxBlobStore.outboxProcessingTaskHandle joined with timeout of 30s")
			} else {
				slog.Debug("OutboxBlobStore.outboxProcessingTaskHandle joined without timeout")
			}
		}
		obs.triggerChannelClosed = true
	}
	return obs.innerBlobStore.Stop(ctx)
}

func (obs *outboxBlobStore) storeBlobOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, blobId blobstore.BlobId, content []byte) error {
	entry := blobOutboxEntry.Entity{
		Operation: operation,
		BlobId:    blobId,
		Content:   content,
	}
	err := obs.blobOutboxEntryRepository.SaveBlobOutboxEntry(ctx, tx, &entry)
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

func (obs *outboxBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	err = obs.storeBlobOutboxEntry(ctx, tx, blobOutboxEntry.PutBlobOperation, blobId, content)
	if err != nil {
		return err
	}

	return nil
}

func (obs *outboxBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	lastBlobOutboxEntry, err := obs.blobOutboxEntryRepository.FindLastBlobOutboxEntryByBlobId(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}
	if lastBlobOutboxEntry != nil {
		switch lastBlobOutboxEntry.Operation {
		case blobOutboxEntry.PutBlobOperation:
			return ioutils.NewByteReadSeekCloser(lastBlobOutboxEntry.Content), nil
		case blobOutboxEntry.DeleteBlobOperation:
			return nil, blobstore.ErrBlobNotFound
		}
	}
	return obs.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (obs *outboxBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
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

	allBlobIds := make(map[blobstore.BlobId]struct{})
	// write them all into the set
	for _, blobId := range innerBlobIds {
		allBlobIds[blobId] = struct{}{}
	}

	// then remove the once that were deleted in the outbox
	// and add the once that were added to the outbox
	for _, outboxEntry := range lastOutboxEntryGroupedByBlobId {
		switch outboxEntry.Operation {
		case blobOutboxEntry.DeleteBlobOperation:
			delete(allBlobIds, outboxEntry.BlobId)
		case blobOutboxEntry.PutBlobOperation:
			allBlobIds[outboxEntry.BlobId] = struct{}{}
		}
	}

	// convert the set back to a list
	blobIds := []blobstore.BlobId{}
	for blobId := range allBlobIds {
		blobIds = append(blobIds, blobId)
	}
	return blobIds, nil
}

func (obs *outboxBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	err := obs.storeBlobOutboxEntry(ctx, tx, blobOutboxEntry.DeleteBlobOperation, blobId, []byte{})
	if err != nil {
		return err
	}

	return nil
}
