package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findFirstStorageOutboxEntryStmt          = "SELECT id, operation, bucket, key, content_type, data, created_at, updated_at FROM storage_outbox_entries ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryStmt           = "SELECT id, operation, bucket, key, content_type, data, created_at, updated_at FROM storage_outbox_entries ORDER BY id DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketStmt = "SELECT id, operation, bucket, key, content_type, data, created_at, updated_at FROM storage_outbox_entries WHERE bucket = $1 ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketStmt  = "SELECT id, operation, bucket, key, content_type, data, created_at, updated_at FROM storage_outbox_entries WHERE bucket = $1 ORDER BY id DESC LIMIT 1"
	insertStorageOutboxEntryStmt             = "INSERT INTO storage_outbox_entries (id, operation, bucket, key, content_type, data, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
	updateStorageOutboxEntryByIdStmt         = "UPDATE storage_outbox_entries SET operation = $1, bucket = $2, key = $3, content_type = $4, data = $5, updated_at = $6 WHERE id = $7"
	deleteStorageOutboxEntryByIdStmt         = "DELETE FROM storage_outbox_entries WHERE id = $1"
)

func NewRepository() (storageoutboxentry.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToStorageOutboxEntryEntity(storageOutboxRow *sql.Row) (*storageoutboxentry.Entity, error) {
	var id string
	var operation string
	var bucket string
	var key string
	var contentType *string
	var data []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := storageOutboxRow.Scan(&id, &operation, &bucket, &key, &contentType, &data, &createdAt, &updatedAt)
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
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func (sor *pgxRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryStmt)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryStmt)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketStmt, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketStmt, bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *storageoutboxentry.Entity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now().UTC()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertStorageOutboxEntryStmt, storageOutboxEntry.Id.String(), storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.Data, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateStorageOutboxEntryByIdStmt, storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.Data, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String())
	return err
}

func (sor *pgxRepository) DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteStorageOutboxEntryByIdStmt, id.String())
	return err
}
