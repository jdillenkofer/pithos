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
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
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
	db                     database.Database
	collectionMutex        sync.RWMutex
	metadataStore          metadatastore.MetadataStore
	partRegistryRepository partregistry.Repository
	partStores             *partstore.NamedPartStores
	writeOperations        atomic.Int64
	tracer                 trace.Tracer
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, partStores *partstore.NamedPartStores, partRegistryRepository partregistry.Repository) (PartGarbageCollector, error) {
	return &partGC{
		db:                     db,
		collectionMutex:        sync.RWMutex{},
		writeOperations:        atomic.Int64{},
		metadataStore:          metadataStore,
		partRegistryRepository: partRegistryRepository,
		partStores:             partStores,
		tracer:                 otel.Tracer("internal/storage/metadatapart/gc"),
	}, nil
}

func (partGC *partGC) PreventGCFromRunning(ctx context.Context) (unblockGC func()) {
	ctx, span := partGC.tracer.Start(ctx, "PartGarbageCollector.PreventGCFromRunning")
	partGC.writeOperations.Add(1)
	span.AddEvent("Acquiring lock")
	partGC.collectionMutex.RLock()
	span.AddEvent("Acquired lock")
	unblockGC = func() {
		partGC.collectionMutex.RUnlock()
		span.AddEvent("Released lock")
		span.End()
	}
	return
}

type protectedDatabase struct {
	inner  database.Database
	partGC PartGarbageCollector
}

var _ database.Database = (*protectedDatabase)(nil)

func NewProtectedDatabase(inner database.Database, partGC PartGarbageCollector) database.Database {
	return &protectedDatabase{inner: inner, partGC: partGC}
}

func (db *protectedDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*database.TxController, error) {
	readOnly := opts != nil && opts.ReadOnly
	if tx, ok := database.TxControllerFromContext(ctx); ok {
		if tx.DBHandle() == db || tx.DBHandle() == db.inner {
			if !readOnly && tx.ReadOnly() {
				return nil, database.ErrWriteInReadOnlyTransaction
			}
			return tx.Child(), nil
		}
	}

	var unblockGC func()
	if !readOnly {
		unblockGC = db.partGC.PreventGCFromRunning(ctx)
	}

	tx, err := db.inner.BeginTx(ctx, opts)
	if err != nil {
		if unblockGC != nil {
			unblockGC()
		}
		return nil, err
	}

	protectedTx := database.NewTxController(tx.SqlTx(), db, readOnly)
	if unblockGC != nil {
		var releaseOnce sync.Once
		release := func(context.Context) error {
			releaseOnce.Do(unblockGC)
			return nil
		}
		protectedTx.OnAfterCommit(release)
		protectedTx.OnRollback(release)
	}
	return protectedTx, nil
}

func (db *protectedDatabase) PingContext(ctx context.Context) error {
	return db.inner.PingContext(ctx)
}

func (db *protectedDatabase) Close() error {
	return db.inner.Close()
}

func (db *protectedDatabase) GetDatabaseType() database.DatabaseType {
	return db.inner.GetDatabaseType()
}

func (db *protectedDatabase) UnwrapDatabase() database.Database {
	return db.inner
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
		// The parts table is the sole liveness source. Reconcile the registry
		// before sweeping so drift can never cause live bytes to be deleted.
		counts, err := partGC.metadataStore.GetInUsePartIdCounts(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		registryEntities, err := partGC.partRegistryRepository.FindAllEntities(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		seen := make(map[partstore.PartId]struct{}, len(registryEntities))
		for _, entity := range registryEntities {
			count, ok := counts[entity.PartId]
			if !ok {
				slog.Warn("Removing orphaned part registry row", "part_id", entity.PartId.String(), "ref_count", entity.RefCount)
				if err := partGC.partRegistryRepository.DeleteByPartId(ctx, tx.SqlTx(), entity.PartId); err != nil {
					return err
				}
				continue
			}
			seen[entity.PartId] = struct{}{}
			if entity.RefCount != count {
				slog.Warn("Correcting part registry refcount", "part_id", entity.PartId.String(), "old_ref_count", entity.RefCount, "new_ref_count", count)
				if err := partGC.partRegistryRepository.UpdateRefCount(ctx, tx.SqlTx(), entity.PartId, count); err != nil {
					return err
				}
			}
		}
		missingRefs := make([]partregistry.Ref, 0)
		for id, count := range counts {
			if _, ok := seen[id]; !ok {
				slog.Warn("Restoring missing part registry row", "part_id", id.String(), "ref_count", count)
				missingRefs = append(missingRefs, partregistry.Ref{PartId: id, Delta: count})
			}
		}
		if err := partGC.partRegistryRepository.RegisterParts(ctx, tx.SqlTx(), missingRefs); err != nil {
			return err
		}

		// Part ids are ULIDs and therefore globally unique across stores, so
		// each store can be swept against the single global in-use set.
		for _, partStore := range partGC.partStores.All() {
			existingPartIds, err := partStore.GetPartIds(ctx, tx)
			if err != nil {
				return err
			}

			for _, existingPartId := range existingPartIds {
				if _, hasKey := counts[existingPartId]; !hasKey {
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
