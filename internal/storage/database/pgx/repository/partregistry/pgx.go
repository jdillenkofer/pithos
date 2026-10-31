package partregistry

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type postgresRepository struct {
}

const (
	insertPartRegistryStmt          = "INSERT INTO part_registry (part_id, ref_count, part_store_name, version, created_at, updated_at) VALUES ($1, $2, $3, 1, $4, $5)"
	addReferencesStmt               = "UPDATE part_registry SET ref_count = ref_count + $1, version = version + 1, updated_at = $2, unreferenced_at = NULL WHERE part_id = $3 AND NOT orphaned"
	removeReferencesStmt            = "UPDATE part_registry SET ref_count = ref_count - $1, version = version + 1, updated_at = $2, unreferenced_at = NULL WHERE part_id = $3 AND ref_count >= $1"
	findAllPartRegistryEntitiesStmt = "SELECT part_id, ref_count, created_at, updated_at, version, unreferenced_at, orphaned FROM part_registry"
	reconciliationStmt              = "SELECT p.part_id, COUNT(*) AS actual_count, r.ref_count, r.version FROM parts p LEFT JOIN part_registry r ON r.part_id = p.part_id GROUP BY p.part_id, r.ref_count, r.version UNION ALL SELECT r.part_id, 0, r.ref_count, r.version FROM part_registry r WHERE NOT EXISTS (SELECT 1 FROM parts p WHERE p.part_id = r.part_id)"
	updateRefCountByPartIdStmt      = "UPDATE part_registry SET ref_count = $1, orphaned = orphaned AND $1::BIGINT = 0, version = version + 1, updated_at = $2, unreferenced_at = CASE WHEN $1::BIGINT = 0 AND ref_count = 0 THEN unreferenced_at ELSE NULL END WHERE part_id = $3 AND version = $4"
	restoreMissingStmt              = "INSERT INTO part_registry (part_id, ref_count, part_store_name, version, created_at, updated_at) SELECT $1::TEXT, $2::BIGINT, MIN(part_store_name), 1, $3::TIMESTAMP, $3::TIMESTAMP FROM parts WHERE part_id = $1::TEXT ON CONFLICT(part_id) DO NOTHING"
	insertOrphanStmt                = "INSERT INTO part_registry (part_id, ref_count, orphaned, part_store_name, version, created_at, updated_at) VALUES ($1, $2, $3, $4, 1, $5, $5) ON CONFLICT(part_id) DO NOTHING"
	markUnreferencedStmt            = "UPDATE part_registry SET unreferenced_at = $1 WHERE part_id = $2 AND version = $3"
	findRegistryForCondemnStmt      = "SELECT ref_count,version,unreferenced_at FROM part_registry WHERE part_id=$1 FOR UPDATE"
	condemnStmt                     = "UPDATE part_registry SET orphaned = $1, version = version + 1, updated_at = $2 WHERE part_id = $3 AND version = $4"
	forgetStmt                      = "DELETE FROM part_registry WHERE part_id = $1 AND orphaned AND ref_count = $2"
	findUnreferencedStmt            = "SELECT part_id, part_store_name FROM part_registry WHERE ref_count = 0 AND part_id > $1 AND (unreferenced_at IS NULL OR unreferenced_at < $2) ORDER BY part_id LIMIT $3"
	countPartsByPartIdStmt          = "SELECT COUNT(*) FROM parts WHERE part_id=$1"
)

func NewRepository() (partregistry.Repository, error) {
	return &postgresRepository{}, nil
}

func (pr *postgresRepository) RegisterParts(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) error {
	now := time.Now().UTC()
	for _, ref := range partregistry.SortRefs(refs) {
		_, err := tx.ExecContext(ctx, insertPartRegistryStmt, ref.PartId.String(), ref.Delta, ref.StoreName, now, now)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pr *postgresRepository) TryAddReferences(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) (bool, error) {
	now := time.Now().UTC()
	for _, ref := range partregistry.SortRefs(refs) {
		result, err := tx.ExecContext(ctx, addReferencesStmt, ref.Delta, now, ref.PartId.String())
		if err != nil {
			return false, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if rowsAffected == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (pr *postgresRepository) RemoveReferences(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) error {
	now := time.Now().UTC()
	for _, ref := range partregistry.SortRefs(refs) {
		// Zero-reference rows are retained; only GC may condemn and delete parts.
		r, err := tx.ExecContext(ctx, removeReferencesStmt, ref.Delta, now, ref.PartId.String())
		if err != nil {
			return err
		}
		n, err := r.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// Missing row or insufficient ref_count: skip the decrement so the
			// part leaks until GC instead of ever deleting live data.
			slog.Warn(fmt.Sprintf("part registry: skipping decrement of %d for part %s (row missing or ref_count too low)", ref.Delta, ref.PartId.String()))
		}
	}
	return nil
}

func (pr *postgresRepository) FindAllEntities(ctx context.Context, tx *sql.Tx) ([]partregistry.Entity, error) {
	rows, err := tx.QueryContext(ctx, findAllPartRegistryEntitiesStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entities := []partregistry.Entity{}
	for rows.Next() {
		var entity partregistry.Entity
		var partIdStr string
		err := rows.Scan(&partIdStr, &entity.RefCount, &entity.CreatedAt, &entity.UpdatedAt, &entity.Version, &entity.UnreferencedAt, &entity.Orphaned)
		if err != nil {
			return nil, err
		}
		entity.PartId = *partstore.MustNewPartIdFromString(partIdStr)
		entities = append(entities, entity)
	}
	return entities, rows.Err()
}

func (pr *postgresRepository) UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount, version int64) (bool, error) {
	r, err := tx.ExecContext(ctx, updateRefCountByPartIdStmt, refCount, time.Now().UTC(), partId.String(), version)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *postgresRepository) FindReconciliation(ctx context.Context, tx *sql.Tx) ([]partregistry.Reconciliation, error) {
	rows, err := tx.QueryContext(ctx, reconciliationStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []partregistry.Reconciliation
	for rows.Next() {
		var id string
		var actual int64
		var ref, version sql.NullInt64
		if err := rows.Scan(&id, &actual, &ref, &version); err != nil {
			return nil, err
		}
		item := partregistry.Reconciliation{PartId: *partstore.MustNewPartIdFromString(id), ActualCount: actual}
		if ref.Valid {
			item.RefCount = &ref.Int64
		}
		if version.Valid {
			item.Version = &version.Int64
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
func (pr *postgresRepository) RestoreMissing(ctx context.Context, tx *sql.Tx, ref partregistry.Ref) (bool, error) {
	r, err := tx.ExecContext(ctx, restoreMissingStmt, ref.PartId.String(), ref.Delta, time.Now().UTC())
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
func (pr *postgresRepository) Condemn(ctx context.Context, tx *sql.Tx, id partstore.PartId, cutoff time.Time) (bool, error) {
	var ref, version int64
	var unreferenced sql.NullTime
	err := tx.QueryRowContext(ctx, findRegistryForCondemnStmt, id.String()).Scan(&ref, &version, &unreferenced)
	if err == sql.ErrNoRows {
		var live int
		err = tx.QueryRowContext(ctx, countPartsByPartIdStmt, id.String()).Scan(&live)
		if err != nil || live != 0 {
			return false, err
		}
		// Even missing rows and deletion retries need a committed observation.
		_, err = tx.ExecContext(ctx, insertOrphanStmt, id.String(), 0, true, nil, time.Now().UTC())
		return false, err
	}
	if err != nil {
		return false, err
	}
	if ref == 0 && !unreferenced.Valid {
		// Sample after acquiring the row lock and observing committed zero refs.
		_, err = tx.ExecContext(ctx, markUnreferencedStmt, time.Now().UTC(), id.String(), version)
		return false, err
	}
	if ref != 0 || !unreferenced.Valid || !unreferenced.Time.Before(cutoff) {
		return false, nil
	}
	var live int
	if err = tx.QueryRowContext(ctx, countPartsByPartIdStmt, id.String()).Scan(&live); err != nil || live != 0 {
		return false, err
	}
	r, err := tx.ExecContext(ctx, condemnStmt, true, time.Now().UTC(), id.String(), version)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *postgresRepository) Forget(ctx context.Context, tx *sql.Tx, id partstore.PartId) (bool, error) {
	r, err := tx.ExecContext(ctx, forgetStmt, id.String(), 0)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// FindUnreferenced keeps ref_count = 0 as a literal in its statement: only
// then may the planner use the partial index for prepared statements, as
// PostgreSQL's generic plans cannot prove a bound parameter matches it.
func (pr *postgresRepository) FindUnreferenced(ctx context.Context, tx *sql.Tx, cutoff time.Time, after *partstore.PartId, limit int) ([]partregistry.Unreferenced, error) {
	afterId := ""
	if after != nil {
		afterId = after.String()
	}
	rows, err := tx.QueryContext(ctx, findUnreferencedStmt, afterId, cutoff.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []partregistry.Unreferenced
	for rows.Next() {
		var id string
		var storeName sql.NullString
		if err := rows.Scan(&id, &storeName); err != nil {
			return nil, err
		}
		item := partregistry.Unreferenced{PartId: *partstore.MustNewPartIdFromString(id)}
		if storeName.Valid {
			item.StoreName = &storeName.String
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (pr *postgresRepository) TrackOrphan(ctx context.Context, tx *sql.Tx, id partstore.PartId, storeName *string) (bool, error) {
	var live int
	if err := tx.QueryRowContext(ctx, countPartsByPartIdStmt, id.String()).Scan(&live); err != nil || live != 0 {
		return false, err
	}
	r, err := tx.ExecContext(ctx, insertOrphanStmt, id.String(), 0, true, storeName, time.Now().UTC())
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
