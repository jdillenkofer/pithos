package gc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partdedupindex"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var gcMetricsOnce sync.Once
var gcMetrics struct {
	runs, errors     *prometheus.CounterVec
	deleted, orphans prometheus.Counter
	duration         *prometheus.HistogramVec
	drift            prometheus.Gauge
}

const (
	passSweep = "sweep"
	passFull  = "full"
)

const gcBatchSize = 256

func registerGCMetrics() {
	gcMetricsOnce.Do(func() {
		gcMetrics.runs = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "runs_total", Help: "Number of garbage collection runs by pass"}, []string{"pass"})
		gcMetrics.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "duration_seconds", Help: "Duration of garbage collection runs by pass"}, []string{"pass"})
		gcMetrics.deleted = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "deleted_parts_total", Help: "Number of parts deleted by garbage collection"})
		gcMetrics.orphans = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "tracked_orphans_total", Help: "Number of untracked physical parts registered as orphans by full passes"})
		gcMetrics.errors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "errors_total", Help: "Number of garbage collection errors by pass"}, []string{"pass"})
		gcMetrics.drift = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "storage_gc", Name: "part_registry_drift", Help: "Number of part registry rows requiring reconciliation"})
	})
	pithosmetrics.Register(gcMetrics.runs, gcMetrics.duration, gcMetrics.deleted, gcMetrics.orphans, gcMetrics.errors, gcMetrics.drift)
}

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
	collectionInterval       time.Duration
	fullPassInterval         time.Duration
}

func New(db database.Database, metadataStore metadatastore.MetadataStore, partStores *partstore.NamedPartStores, partRegistryRepository partregistry.Repository, partDedupIndexRepository partdedupindex.Repository, durations ...time.Duration) (PartGarbageCollector, error) {
	registerGCMetrics()
	graceWindow := 30 * time.Minute
	if len(durations) > 0 {
		graceWindow = durations[0]
	}
	if graceWindow <= 0 {
		return nil, fmt.Errorf("GC grace window must be positive")
	}
	collectionInterval := 30 * time.Minute
	if len(durations) > 1 {
		collectionInterval = durations[1]
	}
	if collectionInterval <= 0 {
		return nil, fmt.Errorf("GC interval must be positive")
	}
	fullPassInterval := 24 * time.Hour
	if len(durations) > 2 {
		fullPassInterval = durations[2]
	}
	if fullPassInterval <= 0 {
		return nil, fmt.Errorf("GC full pass interval must be positive")
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
		collectionInterval:       collectionInterval,
		fullPassInterval:         fullPassInterval,
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
	ctx, cancel := context.WithCancel(context.Background())
	stopWatcherDone := make(chan struct{})
	go func() {
		defer close(stopWatcherDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if stopRunning.Load() {
				cancel()
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		cancel()
		<-stopWatcherDone
	}()

	// The full pass runs at startup, so restarts never postpone it
	// indefinitely, and then every fullPassInterval. Sweeps run after writes
	// and at least every collectionInterval.
	var lastWriteOperationCount int64 = 0
	var lastFullPass time.Time
	lastSweep := time.Now()
	for !stopRunning.Load() {
		fullPassRan := false
		if !time.Now().Before(lastFullPass.Add(partGC.fullPassInterval)) {
			slog.Debug("Running part garbage collector full pass")
			lastFullPass = time.Now()
			if err := partGC.runFullPassWithContext(ctx); err != nil {
				slog.Error(fmt.Sprintf("Failure while running garbage collector full pass: %s", err))
				// Retry with the sweep cadence instead of a full interval later.
				lastFullPass = time.Now().Add(partGC.collectionInterval - partGC.fullPassInterval)
			}
			fullPassRan = true
		}
		newWriteOperationCount := partGC.writeOperations.Load()
		// A sweep right after the full pass starts retention for new orphans.
		sweepDue := fullPassRan || !time.Now().Before(lastSweep.Add(partGC.collectionInterval))
		if newWriteOperationCount > lastWriteOperationCount || sweepDue {
			slog.Debug("Running part garbage collector sweep")
			err := partGC.runSweepWithContext(ctx)
			lastSweep = time.Now()
			if err != nil {
				slog.Error(fmt.Sprintf("Failure while running garbage collector sweep: %s", err))
			} else {
				slog.Debug("Ran part garbage collector sweep successfully")
			}
		}
		lastWriteOperationCount = newWriteOperationCount

		wait := 30 * time.Second
		for _, next := range []time.Time{lastSweep.Add(partGC.collectionInterval), lastFullPass.Add(partGC.fullPassInterval)} {
			if until := time.Until(next); until < wait {
				wait = until
			}
		}
		if wait <= 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (partGC *partGC) runGC() error {
	return partGC.runGCWithContext(context.Background())
}

func (partGC *partGC) runGCWithContext(ctx context.Context) error {
	if err := partGC.runFullPassWithContext(ctx); err != nil {
		return err
	}
	return partGC.runSweepWithContext(ctx)
}

// runFullPassWithContext repairs drift that the sweep cannot see: registry
// counts that disagree with the parts table, stale dedup index entries and
// physical parts without registry row (interrupted writes, rolled back
// external writes). It never deletes parts itself; untracked parts become
// orphaned registry rows that the sweep reclaims after retention.
func (partGC *partGC) runFullPassWithContext(ctx context.Context) (resultErr error) {
	started := time.Now()
	gcMetrics.runs.WithLabelValues(passFull).Inc()
	defer func() {
		gcMetrics.duration.WithLabelValues(passFull).Observe(time.Since(started).Seconds())
		if resultErr != nil {
			gcMetrics.errors.WithLabelValues(passFull).Inc()
		}
	}()
	ctx, span := partGC.tracer.Start(ctx, "PartGarbageCollector.runFullPass")
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
	gcMetrics.drift.Set(float64(len(observations)))
	for start := 0; start < len(observations); start += 256 {
		end := min(start+256, len(observations))
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
			for _, o := range observations[start:end] {
				if o.Version == nil {
					_, err := partGC.partRegistryRepository.RestoreMissing(ctx, tx.SqlTx(), partregistry.Ref{PartId: o.PartId, Delta: o.ActualCount})
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
	registered := make(map[partstore.PartId]struct{})
	if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rows, err := partGC.partRegistryRepository.FindAllEntities(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		for _, row := range rows {
			registered[row.PartId] = struct{}{}
		}
		return nil
	}); err != nil {
		return err
	}
	numTrackedOrphans := 0
	for name, store := range partGC.partStores.All() {
		var untracked []partstore.PartId
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			ids, err := store.GetPartIds(ctx, tx)
			if err != nil {
				return err
			}
			for _, id := range ids {
				// Young parts may belong to a metadata transaction that has
				// not committed yet.
				if _, ok := registered[id]; !ok && id.CreatedAt().Before(cutoff) {
					untracked = append(untracked, id)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		storeName := recordedStoreName(name)
		for start := 0; start < len(untracked); start += gcBatchSize {
			end := min(start+gcBatchSize, len(untracked))
			if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
				for _, id := range untracked[start:end] {
					tracked, err := partGC.partRegistryRepository.TrackOrphan(ctx, tx.SqlTx(), id, storeName)
					if err != nil {
						return err
					}
					if tracked {
						numTrackedOrphans++
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	gcMetrics.orphans.Add(float64(numTrackedOrphans))
	slog.Debug(fmt.Sprintf("Garbage Collection tracked %d orphaned parts", numTrackedOrphans))
	return nil
}

// runSweepWithContext reclaims zero-reference registry rows. Its cost is
// proportional to the number of candidates, not to the size of the stores,
// so it can run after every burst of writes.
func (partGC *partGC) runSweepWithContext(ctx context.Context) (resultErr error) {
	started := time.Now()
	gcMetrics.runs.WithLabelValues(passSweep).Inc()
	defer func() {
		gcMetrics.duration.WithLabelValues(passSweep).Observe(time.Since(started).Seconds())
		if resultErr != nil {
			gcMetrics.errors.WithLabelValues(passSweep).Inc()
		}
	}()
	ctx, span := partGC.tracer.Start(ctx, "PartGarbageCollector.runSweep")
	defer span.End()
	cutoff := time.Now().UTC().Add(-partGC.graceWindow)
	numDeletedParts := 0
	var after *partstore.PartId
	for {
		var candidates []partregistry.Unreferenced
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			var err error
			candidates, err = partGC.partRegistryRepository.FindUnreferenced(ctx, tx.SqlTx(), cutoff, after, gcBatchSize)
			return err
		}); err != nil {
			return err
		}
		if len(candidates) == 0 {
			break
		}
		after = &candidates[len(candidates)-1].PartId
		type externalDelete struct {
			id    partstore.PartId
			store partstore.PartStore
		}
		var external []externalDelete
		if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
			for _, candidate := range candidates {
				store, err := partGC.partStores.ByName(candidate.StoreName)
				if err != nil {
					slog.Warn("skipping unreferenced part of unknown part store", "part_id", candidate.PartId.String(), "error", err)
					continue
				}
				// Condemn rechecks references under the row lock and only
				// condemns after a committed observation older than cutoff.
				condemned, err := partGC.partRegistryRepository.Condemn(ctx, tx.SqlTx(), candidate.PartId, cutoff)
				if err != nil {
					return err
				}
				if !condemned {
					continue
				}
				if err = partGC.partDedupIndexRepository.DeleteByPartIds(ctx, tx.SqlTx(), []partstore.PartId{candidate.PartId}); err != nil {
					return err
				}
				if partstore.CapabilitiesOf(store).Has(partstore.CapabilityTxFreeDeletePart) {
					external = append(external, externalDelete{id: candidate.PartId, store: store})
					continue
				}
				if err = store.DeletePart(ctx, tx, candidate.PartId); err != nil {
					return err
				}
				if _, err = partGC.partRegistryRepository.Forget(ctx, tx.SqlTx(), candidate.PartId); err != nil {
					return err
				}
				numDeletedParts++
			}
			return nil
		}); err != nil {
			return err
		}
		// External deletes run after the condemnation committed. A failed
		// delete keeps the condemned row, so the next sweep retries it.
		var deleted []partstore.PartId
		for _, d := range external {
			if err := d.store.DeletePart(ctx, nil, d.id); err != nil {
				slog.Warn("post-commit part deletion failed; retrying in next GC sweep", "part_id", d.id.String(), "error", err)
				continue
			}
			deleted = append(deleted, d.id)
		}
		if len(deleted) > 0 {
			if err := database.WithTx(ctx, partGC.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
				for _, id := range deleted {
					if _, err := partGC.partRegistryRepository.Forget(ctx, tx.SqlTx(), id); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			numDeletedParts += len(deleted)
		}
		if len(candidates) < gcBatchSize {
			break
		}
	}
	slog.Debug(fmt.Sprintf("Garbage Collection deleted %d parts", numDeletedParts))
	gcMetrics.deleted.Add(float64(numDeletedParts))
	return nil
}

// recordedStoreName maps a store name to the value recorded in registry and
// part rows, where nil means the default store.
func recordedStoreName(name string) *string {
	if name == partstore.DefaultPartStoreName {
		return nil
	}
	return &name
}
