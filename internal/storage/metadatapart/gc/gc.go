package gc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partdedupindex"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type PartGarbageCollector interface {
	RunGCLoop(stopRunning *atomic.Bool)
}

type partGC struct {
	db                       database.Database
	metadataStore            metadatastore.MetadataStore
	partRegistryRepository   partregistry.Repository
	partDedupIndexRepository partdedupindex.Repository
	partStores               *partstore.NamedPartStores
	writeOperations          atomic.Int64
	tracer                   trace.Tracer
	graceWindow              time.Duration
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, partStores *partstore.NamedPartStores, partRegistryRepository partregistry.Repository, partDedupIndexRepository partdedupindex.Repository, graceWindows ...time.Duration) (PartGarbageCollector, error) {
	graceWindow := 30 * time.Minute
	if len(graceWindows) > 0 {
		graceWindow = graceWindows[0]
	}
	if graceWindow <= 0 {
		return nil, fmt.Errorf("GC grace window must be positive")
	}
	return &partGC{
		db:                       db,
		writeOperations:          atomic.Int64{},
		metadataStore:            metadataStore,
		partRegistryRepository:   partRegistryRepository,
		partDedupIndexRepository: partDedupIndexRepository,
		partStores:               partStores,
		tracer:                   otel.Tracer("internal/storage/metadatapart/gc"),
		graceWindow:              graceWindow,
	}, nil
}

type protectedDatabase struct {
	inner  database.Database
	partGC *partGC
}

var _ database.Database = (*protectedDatabase)(nil)

func NewProtectedDatabase(inner database.Database, collector PartGarbageCollector) database.Database {
	return &protectedDatabase{inner: inner, partGC: collector.(*partGC)}
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

	if !readOnly {
		db.partGC.writeOperations.Add(1)
	}

	tx, err := db.inner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	protectedTx := database.NewTxController(tx.SqlTx(), db, readOnly)
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
	cutoff := time.Now().UTC().Add(-partGC.graceWindow)
	var observations []partregistry.Reconciliation
	if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		observations, err = partGC.partRegistryRepository.FindReconciliation(ctx, tx.SqlTx())
		return err
	}); err != nil {
		return err
	}
	for start := 0; start < len(observations); start += 256 {
		end := min(start+256, len(observations))
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
			for _, o := range observations[start:end] {
				if o.Version == nil {
					_, err := partGC.partRegistryRepository.RestoreMissing(ctx, tx.SqlTx(), partregistry.Ref{PartId: o.PartId, Delta: o.ActualCount})
					if err != nil {
						return err
					}
				} else if o.ActualCount == 0 {
					_, err := partGC.partRegistryRepository.DeleteByPartId(ctx, tx.SqlTx(), o.PartId, *o.Version)
					if err != nil {
						return err
					}
				} else if o.RefCount == nil || *o.RefCount != o.ActualCount {
					_, err := partGC.partRegistryRepository.UpdateRefCount(ctx, tx.SqlTx(), o.PartId, o.ActualCount, *o.Version)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// Pruning and backfill are idempotent and deliberately isolated from scans.
	if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		indexed, err := partGC.partDedupIndexRepository.FindAllPartIds(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		live, err := partGC.metadataStore.GetInUsePartIdCounts(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		var dead []partstore.PartId
		for _, id := range indexed {
			if _, ok := live[id]; !ok {
				dead = append(dead, id)
			}
		}
		if err = partGC.partDedupIndexRepository.DeleteByPartIds(ctx, tx.SqlTx(), dead); err != nil {
			return err
		}
		_, err = partGC.partDedupIndexRepository.BackfillFromParts(ctx, tx.SqlTx())
		return err
	}); err != nil {
		return err
	}
	numDeletedParts := 0
	for _, store := range partGC.partStores.All() {
		var candidates []partstore.PartId
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			ids, err := store.GetPartIds(ctx, tx)
			if err != nil {
				return err
			}
			for _, id := range ids {
				if id.CreatedAt().Before(cutoff) {
					candidates = append(candidates, id)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for start := 0; start < len(candidates); start += 256 {
			end := min(start+256, len(candidates))
			var external []partstore.PartId
			if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
				for _, id := range candidates[start:end] {
					condemned, err := partGC.partRegistryRepository.Condemn(ctx, tx.SqlTx(), id)
					if err != nil {
						return err
					}
					if !condemned {
						continue
					}
					if err = partGC.partDedupIndexRepository.DeleteByPartIds(ctx, tx.SqlTx(), []partstore.PartId{id}); err != nil {
						return err
					}
					if partstore.SupportsTxFreeDeletePart(store) {
						external = append(external, id)
					} else if err = store.DeletePart(ctx, tx, id); err != nil {
						return err
					}
					numDeletedParts++
				}
				return nil
			}); err != nil {
				return err
			}
			for _, id := range external {
				if err := store.DeletePart(ctx, nil, id); err != nil {
					slog.Warn("post-commit part deletion failed; leaving orphan for next GC", "part_id", id.String(), "error", err)
				}
			}
		}
	}
	slog.Debug(fmt.Sprintf("Garbage Collection deleted %d parts", numDeletedParts))
	return nil
}
