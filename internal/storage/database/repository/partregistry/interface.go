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
// ref_count == COUNT(*) FROM parts WHERE part_id = X.
type Repository interface {
	// RegisterParts inserts registry rows for freshly created part ids.
	// Refs are processed in ascending part-id order.
	RegisterParts(ctx context.Context, tx *sql.Tx, refs []Ref) error
	// TryAddReferences increments ref_count per ref, guarded by ref_count > 0
	// so a condemned part is never resurrected. It returns false as soon as a
	// guard fails; earlier increments within the same call are rolled back with
	// the surrounding transaction by the caller. Refs are processed in
	// ascending part-id order.
	TryAddReferences(ctx context.Context, tx *sql.Tx, refs []Ref) (bool, error)
	// RemoveReferences decrements ref_count per ref and returns the part ids
	// whose count reached zero (their rows are deleted); only those may be
	// physically deleted. A missing row or insufficient count is skipped so the
	// part leaks until GC instead of risking deletion of live data. Refs are
	// processed in ascending part-id order.
	RemoveReferences(ctx context.Context, tx *sql.Tx, refs []Ref) ([]partstore.PartId, error)
	// FindAllEntities returns every registry row for GC reconciliation.
	FindAllEntities(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	// UpdateRefCount overwrites a row's ref_count (GC self-heal).
	UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount int64) error
	// DeleteByPartId removes a registry row (GC self-heal).
	DeleteByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error
}

// Ref is a reference-count change for one part id.
type Ref struct {
	PartId partstore.PartId
	Delta  int64
}

type Entity struct {
	PartId    partstore.PartId
	RefCount  int64
	CreatedAt time.Time
	UpdatedAt time.Time
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
