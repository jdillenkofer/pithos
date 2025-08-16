package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	findLastBlobOutboxEntryByBlobIdStmt        = "SELECT id, operation, blob_id, content, created_at, updated_at FROM blob_outbox_entries WHERE blob_id = $1 ORDER BY id DESC LIMIT 1"
	findLastBlobOutboxEntryGroupedByBlobIdStmt = "SELECT MAX(id), operation, blob_id, content, created_at, updated_at FROM blob_outbox_entries GROUP BY blob_id"
	findFirstBlobOutboxEntryStmt               = "SELECT id, operation, blob_id, content, created_at, updated_at FROM blob_outbox_entries ORDER BY id ASC LIMIT 1"
	insertBlobOutboxEntryStmt                  = "INSERT INTO blob_outbox_entries (id, operation, blob_id, content, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	updateBlobOutboxEntryByIdStmt              = "UPDATE blob_outbox_entries SET operation = $1, blob_id = $2, content = $3, updated_at = $4 WHERE id = $5"
	deleteBlobOutboxEntryByIdStmt              = "DELETE FROM blob_outbox_entries WHERE id = $1"
)

func NewRepository() (bloboutboxentry.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToBlobOutboxEntryEntity(blobOutboxRow *sql.Row) (*bloboutboxentry.Entity, error) {
	var id string
	var operation string
	var blobId string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobOutboxRow.Scan(&id, &operation, &blobId, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	ulidBlobId := ulid.MustParse(blobId)
	return &bloboutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		BlobId:    ulidBlobId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// @TODO: CodeDuplication See convertRowToBlobOutboxEntryEntity
func convertRowsToBlobOutboxEntryEntity(blobOutboxRows *sql.Rows) (*bloboutboxentry.Entity, error) {
	var id string
	var operation string
	var blobId string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobOutboxRows.Scan(&id, &operation, &blobId, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	ulidBlobId := ulid.MustParse(blobId)
	return &bloboutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		BlobId:    ulidBlobId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bor *sqliteRepository) FindLastBlobOutboxEntryByBlobId(ctx context.Context, tx *sql.Tx, blobId ulid.ULID) (*bloboutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastBlobOutboxEntryByBlobIdStmt, blobId.String())
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *sqliteRepository) FindLastBlobOutboxEntryGroupedByBlobId(ctx context.Context, tx *sql.Tx) ([]bloboutboxentry.Entity, error) {
	blobOutboxEntryRows, err := tx.QueryContext(ctx, findLastBlobOutboxEntryGroupedByBlobIdStmt)
	if err != nil {
		return nil, err
	}
	defer blobOutboxEntryRows.Close()
	blobOutboxEntryEntities := []bloboutboxentry.Entity{}
	for blobOutboxEntryRows.Next() {
		blobOutboxEntryEntity, err := convertRowsToBlobOutboxEntryEntity(blobOutboxEntryRows)
		if err != nil {
			return nil, err
		}
		blobOutboxEntryEntities = append(blobOutboxEntryEntities, *blobOutboxEntryEntity)
	}
	return blobOutboxEntryEntities, nil
}

func (bor *sqliteRepository) FindFirstBlobOutboxEntry(ctx context.Context, tx *sql.Tx) (*bloboutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstBlobOutboxEntryStmt)
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *sqliteRepository) SaveBlobOutboxEntry(ctx context.Context, tx *sql.Tx, blobOutboxEntry *bloboutboxentry.Entity) error {
	if blobOutboxEntry.Id == nil {
		id := ulid.Make()
		blobOutboxEntry.Id = &id
		blobOutboxEntry.CreatedAt = time.Now()
		blobOutboxEntry.UpdatedAt = blobOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertBlobOutboxEntryStmt, blobOutboxEntry.Id.String(), blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.CreatedAt, blobOutboxEntry.UpdatedAt)
		return err
	}

	blobOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, updateBlobOutboxEntryByIdStmt, blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.UpdatedAt, blobOutboxEntry.Id.String())
	return err
}

func (bor *sqliteRepository) DeleteBlobOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteBlobOutboxEntryByIdStmt, id.String())
	return err
}
