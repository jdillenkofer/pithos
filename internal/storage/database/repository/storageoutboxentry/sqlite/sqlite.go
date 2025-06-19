package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	nextOrdinalStorageOutboxEntryStmt        = "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM storage_outbox_entries"
	findFirstStorageOutboxEntryStmt          = "SELECT id, operation, bucket, key, content_type, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal ASC LIMIT 1"
	findLastStorageOutboxEntryStmt           = "SELECT id, operation, bucket, key, content_type, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketStmt = "SELECT id, operation, bucket, key, content_type, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketStmt  = "SELECT id, operation, bucket, key, content_type, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal DESC LIMIT 1"
	insertStorageOutboxEntryStmt             = "INSERT INTO storage_outbox_entries (id, operation, bucket, key, content_type, data, ordinal, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateStorageOutboxEntryByIdStmt         = "UPDATE storage_outbox_entries SET operation = ?, bucket = ?, key = ?, content_type = ?, data = ?, ordinal = ?, updated_at = ? WHERE id = ?"
	deleteStorageOutboxEntryByIdStmt         = "DELETE FROM storage_outbox_entries WHERE id = ?"
)

func NewRepository() (storageoutboxentry.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToStorageOutboxEntryEntity(storageOutboxRow *sql.Row) (*storageoutboxentry.Entity, error) {
	var id string
	var operation string
	var bucket string
	var key string
	var contentType *string
	var data []byte
	var ordinal int
	var createdAt time.Time
	var updatedAt time.Time
	err := storageOutboxRow.Scan(&id, &operation, &bucket, &key, &contentType, &data, &ordinal, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &storageoutboxentry.Entity{
		Id:          &ulidId,
		Operation:   operation,
		Bucket:      bucket,
		Key:         key,
		Data:        data,
		ContentType: contentType,
		Ordinal:     ordinal,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func (sor *sqliteRepository) NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error) {
	row := tx.QueryRowContext(ctx, nextOrdinalStorageOutboxEntryStmt)
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

func (sor *sqliteRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryStmt)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryStmt)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketStmt, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketStmt, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *storageoutboxentry.Entity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertStorageOutboxEntryStmt, storageOutboxEntry.Id.String(), storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, updateStorageOutboxEntryByIdStmt, storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String())
	return err
}

func (sor *sqliteRepository) DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteStorageOutboxEntryByIdStmt, id.String())
	return err
}
