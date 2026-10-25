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

type sqliteRepository struct {
}

const (
	insertPartRegistryStmt          = "INSERT INTO part_registry (part_id, ref_count, version, created_at, updated_at) VALUES ($1, $2, 1, $3, $4)"
	addReferencesStmt               = "UPDATE part_registry SET ref_count = ref_count + $1, version = version + 1, updated_at = $2, unreferenced_at = NULL WHERE part_id = $3 AND NOT orphaned"
	removeReferencesStmt            = "UPDATE part_registry SET ref_count = ref_count - $1, version = version + 1, updated_at = $2, unreferenced_at = NULL WHERE part_id = $3 AND ref_count >= $1 RETURNING ref_count, version"
	findAllPartRegistryEntitiesStmt = "SELECT part_id, ref_count, created_at, updated_at, version, unreferenced_at, orphaned FROM part_registry"
	reconciliationStmt              = "SELECT p.part_id, COUNT(*) AS actual_count, r.ref_count, r.version FROM parts p LEFT JOIN part_registry r ON r.part_id = p.part_id GROUP BY p.part_id, r.ref_count, r.version UNION ALL SELECT r.part_id, 0, r.ref_count, r.version FROM part_registry r WHERE NOT EXISTS (SELECT 1 FROM parts p WHERE p.part_id = r.part_id)"
	updateRefCountByPartIdStmt      = "UPDATE part_registry SET ref_count = $1, orphaned = orphaned AND $1 = 0, version = version + 1, updated_at = $2, unreferenced_at = CASE WHEN $1 = 0 AND ref_count = 0 THEN unreferenced_at ELSE NULL END WHERE part_id = $3 AND version = $4"
	deleteByPartIdStmt              = "DELETE FROM part_registry WHERE part_id = $1 AND version = $2"
	restoreMissingStmt              = "INSERT INTO part_registry (part_id, ref_count, version, created_at, updated_at) VALUES ($1,$2,1,$3,$3) ON CONFLICT(part_id) DO NOTHING"
	insertOrphanStmt                = "INSERT INTO part_registry (part_id, ref_count, orphaned, version, created_at, updated_at) VALUES ($1, $2, $3, 1, $4, $4) ON CONFLICT(part_id) DO NOTHING"
	markUnreferencedStmt            = "UPDATE part_registry SET unreferenced_at = $1 WHERE part_id = $2 AND version = $3"
	findRegistryForCondemnStmt      = "SELECT ref_count, version, unreferenced_at FROM part_registry WHERE part_id=$1"
	countPartsByPartIdStmt          = "SELECT COUNT(*) FROM parts WHERE part_id=$1"
	findRegistryForLeaseStmt        = "SELECT orphaned FROM part_registry WHERE part_id=$1"
	insertReaderLeaseStmt           = "INSERT INTO part_reader_lease (id, expires_at) VALUES ($1, $2)"
	insertReaderLeasePartStmt       = "INSERT INTO part_reader_lease_part (lease_id, part_id) VALUES ($1, $2)"
	renewReaderLeaseStmt            = "UPDATE part_reader_lease SET expires_at = $1 WHERE id = $2 AND expires_at > $3"
	deleteReaderLeaseStmt           = "DELETE FROM part_reader_lease WHERE id = $1"
	deleteExpiredReaderLeasesStmt   = "DELETE FROM part_reader_lease WHERE expires_at <= $1"
	countActiveReaderLeasesStmt     = "SELECT COUNT(*) FROM part_reader_lease_part lp JOIN part_reader_lease l ON l.id = lp.lease_id WHERE lp.part_id = $1 AND l.expires_at > $2"
)

func NewRepository() (partregistry.Repository, error) {
	return &sqliteRepository{}, nil
}

func (pr *sqliteRepository) RegisterParts(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) error {
	now := time.Now().UTC()
	for _, ref := range partregistry.SortRefs(refs) {
		_, err := tx.ExecContext(ctx, insertPartRegistryStmt, ref.PartId.String(), ref.Delta, now, now)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pr *sqliteRepository) TryAddReferences(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) (bool, error) {
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

func (pr *sqliteRepository) RemoveReferences(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) ([]partstore.PartId, error) {
	now := time.Now().UTC()
	unreferenced := []partstore.PartId{}
	for _, ref := range partregistry.SortRefs(refs) {
		var refCount, version int64
		err := tx.QueryRowContext(ctx, removeReferencesStmt, ref.Delta, now, ref.PartId.String()).Scan(&refCount, &version)
		if err == sql.ErrNoRows {
			// Missing row or insufficient ref_count: skip the decrement so the
			// part leaks until GC instead of ever deleting live data.
			slog.Warn(fmt.Sprintf("part registry: skipping decrement of %d for part %s (row missing or ref_count too low)", ref.Delta, ref.PartId.String()))
			continue
		}
		if err != nil {
			return nil, err
		}
		// Retain zero-reference rows; only GC may condemn and delete parts.
	}
	return unreferenced, nil
}

func (pr *sqliteRepository) FindAllEntities(ctx context.Context, tx *sql.Tx) ([]partregistry.Entity, error) {
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

func (pr *sqliteRepository) UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount, version int64) (bool, error) {
	r, err := tx.ExecContext(ctx, updateRefCountByPartIdStmt, refCount, time.Now().UTC(), partId.String(), version)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *sqliteRepository) DeleteByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId, version int64) (bool, error) {
	r, err := tx.ExecContext(ctx, deleteByPartIdStmt, partId.String(), version)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *sqliteRepository) FindReconciliation(ctx context.Context, tx *sql.Tx) ([]partregistry.Reconciliation, error) {
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

func (pr *sqliteRepository) RestoreMissing(ctx context.Context, tx *sql.Tx, ref partregistry.Ref) (bool, error) {
	r, err := tx.ExecContext(ctx, restoreMissingStmt, ref.PartId.String(), ref.Delta, time.Now().UTC())
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *sqliteRepository) Condemn(ctx context.Context, tx *sql.Tx, id partstore.PartId, cutoff time.Time) (bool, error) {
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
		_, err = tx.ExecContext(ctx, insertOrphanStmt, id.String(), 0, true, time.Now().UTC())
		return false, err
	}
	if err != nil {
		return false, err
	}
	if ref == 0 && !unreferenced.Valid {
		// Sample after observing committed zero refs, never in the mutation
		// transaction. A delayed observation commit only lengthens retention.
		_, err = tx.ExecContext(ctx, markUnreferencedStmt, time.Now().UTC(), id.String(), version)
		return false, err
	}
	if ref != 0 || !unreferenced.Valid || !unreferenced.Time.Before(cutoff) {
		return false, nil
	}
	var leased int
	if err = tx.QueryRowContext(ctx, countActiveReaderLeasesStmt, id.String(), time.Now().UTC()).Scan(&leased); err != nil || leased != 0 {
		return false, err
	}
	var live int
	if err = tx.QueryRowContext(ctx, countPartsByPartIdStmt, id.String()).Scan(&live); err != nil || live != 0 {
		return false, err
	}
	return pr.DeleteByPartId(ctx, tx, id, version)
}

func (pr *sqliteRepository) AcquireReaderLease(ctx context.Context, tx *sql.Tx, leaseId string, partIds []partstore.PartId, expiresAt time.Time) (bool, error) {
	refs := partregistry.SortRefs(partregistry.RefsFromPartIds(partIds))
	for _, r := range refs {
		// Deleted rows are condemned; orphaned rows never had metadata.
		var orphaned bool
		err := tx.QueryRowContext(ctx, findRegistryForLeaseStmt, r.PartId.String()).Scan(&orphaned)
		if err == sql.ErrNoRows || (err == nil && orphaned) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, insertReaderLeaseStmt, leaseId, expiresAt.UTC()); err != nil {
		return false, err
	}
	for _, r := range refs {
		if _, err := tx.ExecContext(ctx, insertReaderLeasePartStmt, leaseId, r.PartId.String()); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (pr *sqliteRepository) RenewReaderLease(ctx context.Context, tx *sql.Tx, leaseId string, now, expiresAt time.Time) (bool, error) {
	r, err := tx.ExecContext(ctx, renewReaderLeaseStmt, expiresAt.UTC(), leaseId, now.UTC())
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (pr *sqliteRepository) ReleaseReaderLease(ctx context.Context, tx *sql.Tx, leaseId string) error {
	_, err := tx.ExecContext(ctx, deleteReaderLeaseStmt, leaseId)
	return err
}

func (pr *sqliteRepository) DeleteExpiredReaderLeases(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
	r, err := tx.ExecContext(ctx, deleteExpiredReaderLeasesStmt, now.UTC())
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}
