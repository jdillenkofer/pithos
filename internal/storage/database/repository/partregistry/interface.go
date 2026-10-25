package partregistry

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Repository tracks how many metadata part rows reference each part id. The
// registry row is the serialization point for concurrent reference changes:
// adding and removing references both update the same row, so row locking
// serializes the last-reference race on Postgres (SQLite writes are already
// fully serialized). The invariant at every commit point is
// ref_count == COUNT(*) FROM parts WHERE part_id = X for registered content.
// A negative count is a non-referencable orphan retry marker with no metadata.
type Repository interface {
	// RegisterParts inserts registry rows for freshly created part ids.
	// Refs are processed in ascending part-id order.
	RegisterParts(ctx context.Context, tx *sql.Tx, refs []Ref) error
	// TryAddReferences increments ref_count and clears unreferenced_at.
	// A condemned (deleted registry row) part is never resurrected. It returns
	// false as soon as a guard fails; earlier increments are rolled back with
	// the surrounding transaction by the caller. Refs are processed in
	// ascending part-id order.
	TryAddReferences(ctx context.Context, tx *sql.Tx, refs []Ref) (bool, error)
	// RemoveReferences decrements ref_count and clears the GC observation clock.
	// It returns no ids: physical deletion is exclusively performed by GC.
	// A missing row or insufficient count is skipped so the part leaks until
	// GC instead of risking deletion of live data. Refs are
	// processed in ascending part-id order.
	RemoveReferences(ctx context.Context, tx *sql.Tx, refs []Ref) ([]partstore.PartId, error)
	FindAllEntities(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindReconciliation(ctx context.Context, tx *sql.Tx) ([]Reconciliation, error)
	UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount, version int64) (bool, error)
	DeleteByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId, version int64) (bool, error)
	RestoreMissing(ctx context.Context, tx *sql.Tx, ref Ref) (bool, error)
	// Condemn must run in a fresh GC write transaction (not a metadata mutation
	// transaction or an ambient reader snapshot). The first committed-zero
	// observation starts retention; only later observations may condemn.
	// Missing rows become non-referencable negative-count orphan markers so
	// failed deletion retries cannot resurrect a previously condemned id.
	Condemn(ctx context.Context, tx *sql.Tx, partId partstore.PartId, cutoff time.Time) (bool, error)
	// AcquireReaderLease protects partIds from condemnation until expiresAt.
	// It returns false, writing nothing, if any part is already condemned. It
	// must run in its own short write transaction; it serializes with Condemn
	// on the registry rows, so a committed lease is always seen by Condemn.
	AcquireReaderLease(ctx context.Context, tx *sql.Tx, leaseId string, partIds []partstore.PartId, expiresAt time.Time) (bool, error)
	// RenewReaderLease extends a lease that has not expired by now. It returns
	// false once the lease expired, since Condemn may have ignored it since.
	RenewReaderLease(ctx context.Context, tx *sql.Tx, leaseId string, now, expiresAt time.Time) (bool, error)
	ReleaseReaderLease(ctx context.Context, tx *sql.Tx, leaseId string) error
	DeleteExpiredReaderLeases(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error)
}

// Ref is a reference-count change for one part id.
type Ref struct {
	PartId partstore.PartId
	Delta  int64
}

type Entity struct {
	PartId         partstore.PartId
	RefCount       int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
	UnreferencedAt *time.Time
}

type Reconciliation struct {
	PartId      partstore.PartId
	ActualCount int64
	RefCount    *int64
	Version     *int64
}

// RefsFromPartIds aggregates part ids (with repetitions) into one Ref per id
// whose Delta is the number of occurrences.
func RefsFromPartIds(partIds []partstore.PartId) []Ref {
	counts := map[partstore.PartId]int64{}
	for _, id := range partIds {
		counts[id]++
	}
	refs := make([]Ref, 0, len(counts))
	for id, count := range counts {
		refs = append(refs, Ref{PartId: id, Delta: count})
	}
	return refs
}

// SortRefs orders refs ascending by part id so concurrent transactions touch
// registry rows in the same order (deadlock avoidance on Postgres). It sorts
// a copy and leaves the input untouched.
func SortRefs(refs []Ref) []Ref {
	sorted := make([]Ref, len(refs))
	copy(sorted, refs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PartId.String() < sorted[j].PartId.String()
	})
	return sorted
}
