package partregistry

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Repository tracks how many metadata part rows reference each part id. The
// registry row is the serialization point for concurrent reference changes:
// adding and removing references both update the same row, so row locking
// serializes the last-reference race on Postgres (SQLite writes are already
// fully serialized). The invariant at every commit point is
// ref_count == COUNT(*) FROM parts WHERE part_id = X.
// An orphaned row has no metadata references and must never gain any: GC
// condemned its part, or found no registry row while condemning, and the row
// stays until the part is deleted.
type Repository interface {
	// RegisterParts inserts registry rows for freshly created part ids and
	// records the store holding each part, so GC can reclaim it without
	// listing stores. A zero-reference row that GC tracked while the write
	// was still running is adopted; an orphaned row fails with
	// ErrPartCondemned. Refs are processed in ascending part-id order.
	RegisterParts(ctx context.Context, tx *sql.Tx, refs []Ref) error
	// RegisterUnreferenced records a part in storeName that metadata never
	// referenced. No reader can hold such a part, so its retention has
	// already expired and the next sweep may delete it. An existing row is
	// left untouched.
	RegisterUnreferenced(ctx context.Context, tx *sql.Tx, partId partstore.PartId, storeName *string) error
	// TryAddReferences increments ref_count and clears unreferenced_at.
	// An orphaned part (condemned, or missing its registry row) is never
	// resurrected. It returns false as soon as a guard fails; earlier
	// increments are rolled back with the surrounding transaction by the
	// caller. Refs are processed in ascending part-id order.
	TryAddReferences(ctx context.Context, tx *sql.Tx, refs []Ref) (bool, error)
	// RemoveReferences decrements ref_count and clears the GC observation clock.
	// Rows that reach zero are retained: physical deletion is exclusively
	// performed by GC. A missing row or insufficient count is skipped so the
	// part leaks until GC instead of risking deletion of live data. Refs are
	// processed in ascending part-id order.
	RemoveReferences(ctx context.Context, tx *sql.Tx, refs []Ref) error
	FindAllEntities(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindReconciliation(ctx context.Context, tx *sql.Tx) ([]Reconciliation, error)
	UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount, version int64) (bool, error)
	// RestoreMissing inserts a row for parts without one and records the
	// store the parts table names, so the sweep deletes from the right store.
	RestoreMissing(ctx context.Context, tx *sql.Tx, ref Ref) (bool, error)
	// Condemn must run in a fresh GC write transaction (not a metadata mutation
	// transaction or an ambient reader snapshot). The first committed-zero
	// observation starts retention; only later observations may condemn.
	// A condemned row stays orphaned until Forget, so a failed physical
	// deletion is retried by the next sweep and the id is never resurrected.
	// Missing rows become orphaned rows for the same reason.
	Condemn(ctx context.Context, tx *sql.Tx, partId partstore.PartId, cutoff time.Time) (bool, error)
	// IsReferenced reports whether metadata references partId. A referenced
	// part has not started its retention, so GC cannot condemn it before the
	// grace window after a later dereference.
	IsReferenced(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (bool, error)
	// Forget deletes a condemned row once its physical part is gone.
	Forget(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (bool, error)
	// FindUnreferenced returns up to limit zero-reference rows after the given
	// part id, in ascending part-id order, skipping rows still in retention.
	FindUnreferenced(ctx context.Context, tx *sql.Tx, cutoff time.Time, after *partstore.PartId, limit int) ([]Unreferenced, error)
	// TrackOrphan inserts a zero-reference row for a physical part in
	// storeName that has neither a registry row nor metadata references. The
	// sweep reclaims it after retention unless a write still registers it.
	TrackOrphan(ctx context.Context, tx *sql.Tx, partId partstore.PartId, storeName *string) (bool, error)
}

// ErrPartCondemned reports that RegisterParts found a part GC condemned (the
// part may already be gone) or a row that already holds references, so the
// write must fail.
var ErrPartCondemned = errors.New("part registry: part is condemned or already registered")

// Ref is a reference-count change for one part id. StoreName is only used
// by RegisterParts; nil means the default store.
type Ref struct {
	PartId    partstore.PartId
	Delta     int64
	StoreName *string
}

// Unreferenced is a zero-reference registry row and the store holding its
// part; nil means the default store.
type Unreferenced struct {
	PartId    partstore.PartId
	StoreName *string
}

type Entity struct {
	PartId         partstore.PartId
	RefCount       int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
	UnreferencedAt *time.Time
	Orphaned       bool
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
