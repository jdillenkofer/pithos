package gc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type BlobGarbageCollector interface {
	PreventGCFromRunning() (unblockGC func())
	RunGCLoop(stopRunning *atomic.Bool)
}

type blobGC struct {
	db              database.Database
	collectionMutex sync.RWMutex
	metadataStore   metadatastore.MetadataStore
	blobStore       blobstore.BlobStore
	writeOperations atomic.Int64
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (BlobGarbageCollector, error) {
	return &blobGC{
		db:              db,
		collectionMutex: sync.RWMutex{},
		writeOperations: atomic.Int64{},
		metadataStore:   metadataStore,
		blobStore:       blobStore,
	}, nil
}

func (blobGC *blobGC) PreventGCFromRunning() (unblockGC func()) {
	blobGC.writeOperations.Add(1)
	blobGC.collectionMutex.RLock()
	unblockGC = blobGC.collectionMutex.RUnlock
	return
}

func (blobGC *blobGC) RunGCLoop(stopRunning *atomic.Bool) {
	var lastWriteOperationCount int64 = 0
	for !stopRunning.Load() {
		newWriteOperationCount := blobGC.writeOperations.Load()
		if newWriteOperationCount > lastWriteOperationCount {
			slog.Debug("Running blob garbage collector")
			err := blobGC.runGC()
			if err != nil {
				slog.Error(fmt.Sprintf("Failure while running garbage collector: %s", err))
			} else {
				slog.Debug("Ran blob garbage collector successfully")
			}
		}
		lastWriteOperationCount = newWriteOperationCount
		for range 30 * 4 {
			time.Sleep(250 * time.Millisecond)
			if stopRunning.Load() {
				return
			}
		}
	}
}

func (blobGC *blobGC) runGC() error {
	blobGC.collectionMutex.Lock()
	defer blobGC.collectionMutex.Unlock()

	ctx := context.Background()

	tx, err := blobGC.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	existingBlobIds, err := blobGC.blobStore.GetBlobIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	inUseBlobIdMap := make(map[blobstore.BlobId]struct{})
	inUseBlobIds, err := blobGC.metadataStore.GetInUseBlobIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, inUseBlobId := range inUseBlobIds {
		inUseBlobIdMap[inUseBlobId] = struct{}{}
	}

	numDeletedBlobs := 0

	for _, existingBlobId := range existingBlobIds {
		if _, hasKey := inUseBlobIdMap[existingBlobId]; !hasKey {
			err = blobGC.blobStore.DeleteBlob(ctx, tx, existingBlobId)
			if err != nil {
				tx.Rollback()
				return err
			}

			numDeletedBlobs += 1
		}
	}

	slog.Debug(fmt.Sprintf("Garbage Collection deleted %d blobs", numDeletedBlobs))

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
