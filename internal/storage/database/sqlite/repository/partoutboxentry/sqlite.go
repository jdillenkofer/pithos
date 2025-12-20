package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	findLastPartOutboxEntryByPartIdStmt        = "SELECT id, operation, part_id, content, created_at, updated_at FROM part_outbox_entries WHERE part_id = $1 ORDER BY id DESC LIMIT 1"
	findLastPartOutboxEntryGroupedByPartIdStmt = "SELECT e.id, e.operation, e.part_id, e.content, e.created_at, e.updated_at FROM part_outbox_entries e INNER JOIN ( SELECT part_id, MAX(id) as max_id FROM part_outbox_entries GROUP BY part_id) m ON e.part_id = m.part_id AND e.id = m.max_id"
	findFirstPartOutboxEntryStmt               = "SELECT id, operation, part_id, content, created_at, updated_at FROM part_outbox_entries ORDER BY id ASC LIMIT 1"
	insertPartOutboxEntryStmt                  = "INSERT INTO part_outbox_entries (id, operation, part_id, content, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	updatePartOutboxEntryByIdStmt              = "UPDATE part_outbox_entries SET operation = $1, part_id = $2, content = $3, updated_at = $4 WHERE id = $5"
	deletePartOutboxEntryByIdStmt              = "DELETE FROM part_outbox_entries WHERE id = $1"
)

func NewRepository() (partoutboxentry.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToPartOutboxEntryEntity(partOutboxRow *sql.Row) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := partOutboxRow.Scan(&id, &operation, &partIdStr, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		PartId:    *partId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// @TODO: CodeDuplication See convertRowToPartOutboxEntryEntity
func convertRowsToPartOutboxEntryEntity(partOutboxRows *sql.Rows) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := partOutboxRows.Scan(&id, &operation, &partIdStr, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		PartId:    *partId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bor *sqliteRepository) FindLastPartOutboxEntryByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastPartOutboxEntryByPartIdStmt, partId.String())
	partOutboxEntryEntity, err := convertRowToPartOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return partOutboxEntryEntity, nil
}

func (bor *sqliteRepository) FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx) ([]partoutboxentry.Entity, error) {
	partOutboxEntryRows, err := tx.QueryContext(ctx, findLastPartOutboxEntryGroupedByPartIdStmt)
	if err != nil {
		return nil, err
	}
	defer partOutboxEntryRows.Close()
	partOutboxEntryEntities := []partoutboxentry.Entity{}
	for partOutboxEntryRows.Next() {
		partOutboxEntryEntity, err := convertRowsToPartOutboxEntryEntity(partOutboxEntryRows)
		if err != nil {
			return nil, err
		}
		partOutboxEntryEntities = append(partOutboxEntryEntities, *partOutboxEntryEntity)
	}
	return partOutboxEntryEntities, nil
}

func (bor *sqliteRepository) FindFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstPartOutboxEntryStmt)
	partOutboxEntryEntity, err := convertRowToPartOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return partOutboxEntryEntity, nil
}

func (bor *sqliteRepository) FindFirstPartOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*partoutboxentry.Entity, error) {
	return bor.FindFirstPartOutboxEntry(ctx, tx)
}

func (bor *sqliteRepository) SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, partOutboxEntry *partoutboxentry.Entity) error {
	if partOutboxEntry.Id == nil {
		id := ulid.Make()
		partOutboxEntry.Id = &id
		partOutboxEntry.CreatedAt = time.Now().UTC()
		partOutboxEntry.UpdatedAt = partOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertPartOutboxEntryStmt, partOutboxEntry.Id.String(), partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.Content, partOutboxEntry.CreatedAt, partOutboxEntry.UpdatedAt)
		return err
	}

	partOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartOutboxEntryByIdStmt, partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.Content, partOutboxEntry.UpdatedAt, partOutboxEntry.Id.String())
	return err
}

func (bor *sqliteRepository) DeletePartOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deletePartOutboxEntryByIdStmt, id.String())
	return err
}
