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
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
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
	partStore       partstore.PartStore
	writeOperations atomic.Int64
	tracer          trace.Tracer
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, partStore partstore.PartStore) (PartGarbageCollector, error) {
	return &partGC{
		db:              db,
		collectionMutex: sync.RWMutex{},
		writeOperations: atomic.Int64{},
		metadataStore:   metadataStore,
		partStore:       partStore,
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

	tx, err := partGC.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	existingPartIds, err := partGC.partStore.GetPartIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	inUsePartIdMap := make(map[partstore.PartId]struct{})
	inUsePartIds, err := partGC.metadataStore.GetInUsePartIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, inUsePartId := range inUsePartIds {
		inUsePartIdMap[inUsePartId] = struct{}{}
	}

	numDeletedParts := 0

	for _, existingPartId := range existingPartIds {
		if _, hasKey := inUsePartIdMap[existingPartId]; !hasKey {
			err = partGC.partStore.DeletePart(ctx, tx, existingPartId)
			if err != nil {
				tx.Rollback()
				return err
			}

			numDeletedParts += 1
		}
	}

	slog.Debug(fmt.Sprintf("Garbage Collection deleted %d parts", numDeletedParts))

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
