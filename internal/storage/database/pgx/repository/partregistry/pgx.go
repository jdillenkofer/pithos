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
	insertPartRegistryStmt          = "INSERT INTO part_registry (part_id, ref_count, created_at, updated_at) VALUES ($1, $2, $3, $4)"
	addReferencesStmt               = "UPDATE part_registry SET ref_count = ref_count + $1, updated_at = $2 WHERE part_id = $3 AND ref_count > 0"
	removeReferencesStmt            = "UPDATE part_registry SET ref_count = ref_count - $1, updated_at = $2 WHERE part_id = $3 AND ref_count >= $1 RETURNING ref_count"
	findAllPartRegistryEntitiesStmt = "SELECT part_id, ref_count, created_at, updated_at FROM part_registry"
	updateRefCountByPartIdStmt      = "UPDATE part_registry SET ref_count = $1, updated_at = $2 WHERE part_id = $3"
	deleteByPartIdStmt              = "DELETE FROM part_registry WHERE part_id = $1"
)

func NewRepository() (partregistry.Repository, error) {
	return &postgresRepository{}, nil
}

func (pr *postgresRepository) RegisterParts(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) error {
	now := time.Now().UTC()
	for _, ref := range partregistry.SortRefs(refs) {
		_, err := tx.ExecContext(ctx, insertPartRegistryStmt, ref.PartId.String(), ref.Delta, now, now)
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

func (pr *postgresRepository) RemoveReferences(ctx context.Context, tx *sql.Tx, refs []partregistry.Ref) ([]partstore.PartId, error) {
	now := time.Now().UTC()
	unreferenced := []partstore.PartId{}
	for _, ref := range partregistry.SortRefs(refs) {
		var refCount int64
		err := tx.QueryRowContext(ctx, removeReferencesStmt, ref.Delta, now, ref.PartId.String()).Scan(&refCount)
		if err == sql.ErrNoRows {
			// Missing row or insufficient ref_count: skip the decrement so the
			// part leaks until GC instead of ever deleting live data.
			slog.Warn(fmt.Sprintf("part registry: skipping decrement of %d for part %s (row missing or ref_count too low)", ref.Delta, ref.PartId.String()))
			continue
		}
		if err != nil {
			return nil, err
		}
		if refCount == 0 {
			_, err = tx.ExecContext(ctx, deleteByPartIdStmt, ref.PartId.String())
			if err != nil {
				return nil, err
			}
			unreferenced = append(unreferenced, ref.PartId)
		}
	}
	return unreferenced, nil
}

func (pr *postgresRepository) FindAllEntities(ctx context.Context, tx *sql.Tx) ([]partregistry.Entity, error) {
	rows, err := tx.QueryContext(ctx, findAllPartRegistryEntitiesStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entities := []partregistry.Entity{}
	for rows.Next() {
		var partIdStr string
		var refCount int64
		var createdAt time.Time
		var updatedAt time.Time
		err := rows.Scan(&partIdStr, &refCount, &createdAt, &updatedAt)
		if err != nil {
			return nil, err
		}
		partId := partstore.MustNewPartIdFromString(partIdStr)
		entities = append(entities, partregistry.Entity{
			PartId:    *partId,
			RefCount:  refCount,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		})
	}
	return entities, rows.Err()
}

func (pr *postgresRepository) UpdateRefCount(ctx context.Context, tx *sql.Tx, partId partstore.PartId, refCount int64) error {
	_, err := tx.ExecContext(ctx, updateRefCountByPartIdStmt, refCount, time.Now().UTC(), partId.String())
	return err
}

func (pr *postgresRepository) DeleteByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	_, err := tx.ExecContext(ctx, deleteByPartIdStmt, partId.String())
	return err
}
