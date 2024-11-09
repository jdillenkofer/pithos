package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteBlobOutboxEntryRepository struct {
	db                                                 *sql.DB
	nextOrdinalBlobOutboxEntryPreparedStmt             *sql.Stmt
	findLastBlobOutboxEntryByBlobIdPreparedStmt        *sql.Stmt
	findLastBlobOutboxEntryGroupedByBlobIdPreparedStmt *sql.Stmt
	findFirstBlobOutboxEntryPreparedStmt               *sql.Stmt
	insertBlobOutboxEntryPreparedStmt                  *sql.Stmt
	updateBlobOutboxEntryByIdPreparedStmt              *sql.Stmt
	deleteBlobOutboxEntryByIdPreparedStmt              *sql.Stmt
}

const (
	nextOrdinalBlobOutboxEntryStmt             = "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM blob_outbox_entries"
	findLastBlobOutboxEntryByBlobIdStmt        = "SELECT id, operation, blob_id, content, ordinal, created_at, updated_at FROM blob_outbox_entries WHERE blob_id = ? ORDER BY ordinal DESC LIMIT 1"
	findLastBlobOutboxEntryGroupedByBlobIdStmt = "SELECT id, operation, blob_id, content, MAX(ordinal), created_at, updated_at FROM blob_outbox_entries GROUP BY blob_id"
	findFirstBlobOutboxEntryStmt               = "SELECT id, operation, blob_id, content, ordinal, created_at, updated_at FROM blob_outbox_entries ORDER BY ordinal ASC LIMIT 1"
	insertBlobOutboxEntryStmt                  = "INSERT INTO blob_outbox_entries (id, operation, blob_id, content, ordinal, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)"
	updateBlobOutboxEntryByIdStmt              = "UPDATE blob_outbox_entries SET operation = ?, blob_id = ?, content = ?, ordinal = ?, updated_at = ? WHERE id = ?"
	deleteBlobOutboxEntryByIdStmt              = "DELETE FROM blob_outbox_entries WHERE id = ?"
)

func New(db *sql.DB) (bloboutboxentry.BlobOutboxEntryRepository, error) {
	nextOrdinalBlobOutboxEntryPreparedStmt, err := db.Prepare(nextOrdinalBlobOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	findLastBlobOutboxEntryByBlobIdPreparedStmt, err := db.Prepare(findLastBlobOutboxEntryByBlobIdStmt)
	if err != nil {
		return nil, err
	}
	findLastBlobOutboxEntryGroupedByBlobIdPreparedStmt, err := db.Prepare(findLastBlobOutboxEntryGroupedByBlobIdStmt)
	if err != nil {
		return nil, err
	}
	findFirstBlobOutboxEntryPreparedStmt, err := db.Prepare(findFirstBlobOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	insertBlobOutboxEntryPreparedStmt, err := db.Prepare(insertBlobOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	updateBlobOutboxEntryByIdPreparedStmt, err := db.Prepare(updateBlobOutboxEntryByIdStmt)
	if err != nil {
		return nil, err
	}
	deleteBlobOutboxEntryByIdPreparedStmt, err := db.Prepare(deleteBlobOutboxEntryByIdStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteBlobOutboxEntryRepository{
		db:                                     db,
		nextOrdinalBlobOutboxEntryPreparedStmt: nextOrdinalBlobOutboxEntryPreparedStmt,
		findLastBlobOutboxEntryByBlobIdPreparedStmt:        findLastBlobOutboxEntryByBlobIdPreparedStmt,
		findLastBlobOutboxEntryGroupedByBlobIdPreparedStmt: findLastBlobOutboxEntryGroupedByBlobIdPreparedStmt,
		findFirstBlobOutboxEntryPreparedStmt:               findFirstBlobOutboxEntryPreparedStmt,
		insertBlobOutboxEntryPreparedStmt:                  insertBlobOutboxEntryPreparedStmt,
		updateBlobOutboxEntryByIdPreparedStmt:              updateBlobOutboxEntryByIdPreparedStmt,
		deleteBlobOutboxEntryByIdPreparedStmt:              deleteBlobOutboxEntryByIdPreparedStmt,
	}, nil
}

func convertRowToBlobOutboxEntryEntity(blobOutboxRow *sql.Row) (*bloboutboxentry.BlobOutboxEntryEntity, error) {
	var id string
	var operation string
	var blobId string
	var content []byte
	var ordinal int
	var createdAt time.Time
	var updatedAt time.Time
	err := blobOutboxRow.Scan(&id, &operation, &blobId, &content, &ordinal, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	ulidBlobId := ulid.MustParse(blobId)
	return &bloboutboxentry.BlobOutboxEntryEntity{
		Id:        &ulidId,
		Operation: operation,
		BlobId:    ulidBlobId,
		Content:   content,
		Ordinal:   ordinal,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// @TODO: CodeDuplication See convertRowToBlobOutboxEntryEntity
func convertRowsToBlobOutboxEntryEntity(blobOutboxRows *sql.Rows) (*bloboutboxentry.BlobOutboxEntryEntity, error) {
	var id string
	var operation string
	var blobId string
	var content []byte
	var ordinal int
	var createdAt time.Time
	var updatedAt time.Time
	err := blobOutboxRows.Scan(&id, &operation, &blobId, &content, &ordinal, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	ulidBlobId := ulid.MustParse(blobId)
	return &bloboutboxentry.BlobOutboxEntryEntity{
		Id:        &ulidId,
		Operation: operation,
		BlobId:    ulidBlobId,
		Content:   content,
		Ordinal:   ordinal,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bor *sqliteBlobOutboxEntryRepository) NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error) {
	row := tx.StmtContext(ctx, bor.nextOrdinalBlobOutboxEntryPreparedStmt).QueryRowContext(ctx)
	var ordinal int
	err := row.Scan(&ordinal)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &ordinal, nil
}

func (bor *sqliteBlobOutboxEntryRepository) FindLastBlobOutboxEntryByBlobId(ctx context.Context, tx *sql.Tx, blobId ulid.ULID) (*bloboutboxentry.BlobOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, bor.findLastBlobOutboxEntryByBlobIdPreparedStmt).QueryRowContext(ctx, blobId.String())
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *sqliteBlobOutboxEntryRepository) FindLastBlobOutboxEntryGroupedByBlobId(ctx context.Context, tx *sql.Tx) ([]bloboutboxentry.BlobOutboxEntryEntity, error) {
	blobOutboxEntryRows, err := tx.StmtContext(ctx, bor.findLastBlobOutboxEntryGroupedByBlobIdPreparedStmt).QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer blobOutboxEntryRows.Close()
	blobOutboxEntryEntities := []bloboutboxentry.BlobOutboxEntryEntity{}
	for blobOutboxEntryRows.Next() {
		blobOutboxEntryEntity, err := convertRowsToBlobOutboxEntryEntity(blobOutboxEntryRows)
		if err != nil {
			return nil, err
		}
		blobOutboxEntryEntities = append(blobOutboxEntryEntities, *blobOutboxEntryEntity)
	}
	return blobOutboxEntryEntities, nil
}

func (bor *sqliteBlobOutboxEntryRepository) FindFirstBlobOutboxEntry(ctx context.Context, tx *sql.Tx) (*bloboutboxentry.BlobOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, bor.findFirstBlobOutboxEntryPreparedStmt).QueryRowContext(ctx)
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *sqliteBlobOutboxEntryRepository) SaveBlobOutboxEntry(ctx context.Context, tx *sql.Tx, blobOutboxEntry *bloboutboxentry.BlobOutboxEntryEntity) error {
	if blobOutboxEntry.Id == nil {
		id := ulid.Make()
		blobOutboxEntry.Id = &id
		blobOutboxEntry.CreatedAt = time.Now()
		blobOutboxEntry.UpdatedAt = blobOutboxEntry.CreatedAt
		_, err := tx.StmtContext(ctx, bor.insertBlobOutboxEntryPreparedStmt).ExecContext(ctx, blobOutboxEntry.Id.String(), blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.Ordinal, blobOutboxEntry.CreatedAt, blobOutboxEntry.UpdatedAt)
		return err
	}

	blobOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, bor.updateBlobOutboxEntryByIdPreparedStmt).ExecContext(ctx, blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.Ordinal, blobOutboxEntry.UpdatedAt, blobOutboxEntry.Id.String())
	return err
}

func (bor *sqliteBlobOutboxEntryRepository) DeleteBlobOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.StmtContext(ctx, bor.deleteBlobOutboxEntryByIdPreparedStmt).ExecContext(ctx, id.String())
	return err
}
