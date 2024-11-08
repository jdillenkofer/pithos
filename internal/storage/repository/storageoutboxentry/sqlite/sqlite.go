package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository/storageoutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteStorageOutboxEntryRepository struct {
	db                                               *sql.DB
	nextOrdinalStorageOutboxEntryPreparedStmt        *sql.Stmt
	findFirstStorageOutboxEntryPreparedStmt          *sql.Stmt
	findLastStorageOutboxEntryPreparedStmt           *sql.Stmt
	findFirstStorageOutboxEntryForBucketPreparedStmt *sql.Stmt
	findLastStorageOutboxEntryForBucketPreparedStmt  *sql.Stmt
	insertStorageOutboxEntryPreparedStmt             *sql.Stmt
	updateStorageOutboxEntryByIdPreparedStmt         *sql.Stmt
	deleteStorageOutboxEntryByIdPreparedStmt         *sql.Stmt
}

const (
	nextOrdinalStorageOutboxEntryStmt        = "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM storage_outbox_entries"
	findFirstStorageOutboxEntryStmt          = "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal ASC LIMIT 1"
	findLastStorageOutboxEntryStmt           = "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketStmt = "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketStmt  = "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal DESC LIMIT 1"
	insertStorageOutboxEntryStmt             = "INSERT INTO storage_outbox_entries (id, operation, bucket, key, data, ordinal, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
	updateStorageOutboxEntryByIdStmt         = "UPDATE storage_outbox_entries SET operation = ?, bucket = ?, key = ?, data = ?, ordinal = ?, updated_at = ? WHERE id = ?"
	deleteStorageOutboxEntryByIdStmt         = "DELETE FROM storage_outbox_entries WHERE id = ?"
)

func New(db *sql.DB) (storageoutboxentry.StorageOutboxEntryRepository, error) {
	nextOrdinalStorageOutboxEntryPreparedStmt, err := db.Prepare(nextOrdinalStorageOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	findFirstStorageOutboxEntryPreparedStmt, err := db.Prepare(findFirstStorageOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	findLastStorageOutboxEntryPreparedStmt, err := db.Prepare(findLastStorageOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	findFirstStorageOutboxEntryForBucketPreparedStmt, err := db.Prepare(findFirstStorageOutboxEntryForBucketStmt)
	if err != nil {
		return nil, err
	}
	findLastStorageOutboxEntryForBucketPreparedStmt, err := db.Prepare(findLastStorageOutboxEntryForBucketStmt)
	if err != nil {
		return nil, err
	}
	insertStorageOutboxEntryPreparedStmt, err := db.Prepare(insertStorageOutboxEntryStmt)
	if err != nil {
		return nil, err
	}
	updateStorageOutboxEntryByIdPreparedStmt, err := db.Prepare(updateStorageOutboxEntryByIdStmt)
	if err != nil {
		return nil, err
	}
	deleteStorageOutboxEntryByIdPreparedStmt, err := db.Prepare(deleteStorageOutboxEntryByIdStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteStorageOutboxEntryRepository{
		db: db,
		nextOrdinalStorageOutboxEntryPreparedStmt:        nextOrdinalStorageOutboxEntryPreparedStmt,
		findFirstStorageOutboxEntryPreparedStmt:          findFirstStorageOutboxEntryPreparedStmt,
		findLastStorageOutboxEntryPreparedStmt:           findLastStorageOutboxEntryPreparedStmt,
		findFirstStorageOutboxEntryForBucketPreparedStmt: findFirstStorageOutboxEntryForBucketPreparedStmt,
		findLastStorageOutboxEntryForBucketPreparedStmt:  findLastStorageOutboxEntryForBucketPreparedStmt,
		insertStorageOutboxEntryPreparedStmt:             insertStorageOutboxEntryPreparedStmt,
		updateStorageOutboxEntryByIdPreparedStmt:         updateStorageOutboxEntryByIdPreparedStmt,
		deleteStorageOutboxEntryByIdPreparedStmt:         deleteStorageOutboxEntryByIdPreparedStmt,
	}, nil
}

func convertRowToStorageOutboxEntryEntity(storageOutboxRow *sql.Row) (*storageoutboxentry.StorageOutboxEntryEntity, error) {
	var id string
	var operation string
	var bucket string
	var key string
	var data []byte
	var ordinal int
	var createdAt time.Time
	var updatedAt time.Time
	err := storageOutboxRow.Scan(&id, &operation, &bucket, &key, &data, &ordinal, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &storageoutboxentry.StorageOutboxEntryEntity{
		Id:        &ulidId,
		Operation: operation,
		Bucket:    bucket,
		Key:       key,
		Data:      data,
		Ordinal:   ordinal,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (sor *sqliteStorageOutboxEntryRepository) NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error) {
	row := tx.StmtContext(ctx, sor.nextOrdinalStorageOutboxEntryPreparedStmt).QueryRowContext(ctx)
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

func (sor *sqliteStorageOutboxEntryRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.StorageOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, sor.findFirstStorageOutboxEntryPreparedStmt).QueryRowContext(ctx)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteStorageOutboxEntryRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.StorageOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, sor.findLastStorageOutboxEntryPreparedStmt).QueryRowContext(ctx)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteStorageOutboxEntryRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.StorageOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, sor.findFirstStorageOutboxEntryForBucketPreparedStmt).QueryRowContext(ctx, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteStorageOutboxEntryRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.StorageOutboxEntryEntity, error) {
	row := tx.StmtContext(ctx, sor.findLastStorageOutboxEntryForBucketPreparedStmt).QueryRowContext(ctx, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteStorageOutboxEntryRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *storageoutboxentry.StorageOutboxEntryEntity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.StmtContext(ctx, sor.insertStorageOutboxEntryPreparedStmt).ExecContext(ctx, storageOutboxEntry.Id.String(), storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, sor.updateStorageOutboxEntryByIdPreparedStmt).ExecContext(ctx, storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String())
	return err
}

func (sor *sqliteStorageOutboxEntryRepository) DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.StmtContext(ctx, sor.deleteStorageOutboxEntryByIdPreparedStmt).ExecContext(ctx, id.String())
	return err
}
