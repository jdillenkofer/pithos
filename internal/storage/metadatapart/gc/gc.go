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
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type PartGarbageCollector interface {
	PreventGCFromRunning(ctx context.Context) (unblockGC func())
	RunGCLoop(stopRunning *atomic.Bool)
}

type partGC struct {
	db              database.Database
	collectionMutex sync.RWMutex
	metadataStore   metadatastore.MetadataStore
	partStores      *partstore.NamedPartStores
	writeOperations atomic.Int64
	tracer          trace.Tracer
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, partStores *partstore.NamedPartStores) (PartGarbageCollector, error) {
	return &partGC{
		db:              db,
		collectionMutex: sync.RWMutex{},
		writeOperations: atomic.Int64{},
		metadataStore:   metadataStore,
		partStores:      partStores,
		tracer:          otel.Tracer("internal/storage/metadatapart/gc"),
	}, nil
}

func (partGC *partGC) PreventGCFromRunning(ctx context.Context) (unblockGC func()) {
	_, span := partGC.tracer.Start(ctx, "PartGarbageCollector.PreventGCFromRunning")
	defer span.End()
	partGC.writeOperations.Add(1)
	span.AddEvent("Acquiring lock")
	partGC.collectionMutex.RLock()
	span.AddEvent("Acquired lock")
	unblockGC = func() {
		partGC.collectionMutex.RUnlock()
		span.AddEvent("Released lock")
	}
	return
}

func (partGC *partGC) RunGCLoop(stopRunning *atomic.Bool) {
	var lastWriteOperationCount int64 = 0
	for !stopRunning.Load() {
		newWriteOperationCount := partGC.writeOperations.Load()
		if newWriteOperationCount > lastWriteOperationCount {
			slog.Debug("Running part garbage collector")
			err := partGC.runGC()
			if err != nil {
				slog.Error(fmt.Sprintf("Failure while running garbage collector: %s", err))
			} else {
				slog.Debug("Ran part garbage collector successfully")
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

func (partGC *partGC) runGC() error {
	ctx := context.Background()
	ctx, span := partGC.tracer.Start(ctx, "PartGarbageCollector.runGC")
	defer span.End()

	span.AddEvent("Acquiring lock")
	partGC.collectionMutex.Lock()
	span.AddEvent("Acquired lock")
	defer func() {
		partGC.collectionMutex.Unlock()
		span.AddEvent("Released lock")
	}()

	numDeletedParts := 0
	err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		// Part ids are ULIDs and therefore globally unique across stores, so
		// each store can be swept against the single global in-use set.
		inUsePartIdMap := make(map[partstore.PartId]struct{})
		inUsePartIds, err := partGC.metadataStore.GetInUsePartIds(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		for _, inUsePartId := range inUsePartIds {
			inUsePartIdMap[inUsePartId] = struct{}{}
		}

		for _, partStore := range partGC.partStores.All() {
			existingPartIds, err := partStore.GetPartIds(ctx, tx)
			if err != nil {
				return err
			}

			for _, existingPartId := range existingPartIds {
				if _, hasKey := inUsePartIdMap[existingPartId]; !hasKey {
					err = partStore.DeletePart(ctx, tx, existingPartId)
					if err != nil {
						return err
					}

					numDeletedParts += 1
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	slog.Debug(fmt.Sprintf("Garbage Collection deleted %d parts", numDeletedParts))
	return nil
}
