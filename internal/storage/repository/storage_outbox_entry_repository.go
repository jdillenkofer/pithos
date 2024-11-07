package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type StorageOutboxEntryRepository struct {
	db *sql.DB
}

func NewStorageOutboxEntryRepository(db *sql.DB) (*StorageOutboxEntryRepository, error) {
	return &StorageOutboxEntryRepository{
		db: db,
	}, nil
}

const (
	CreateBucketStorageOperation = "CreateBucket"
	DeleteBucketStorageOperation = "DeleteBucket"
	PutObjectStorageOperation    = "PutObject"
	DeleteObjectStorageOperation = "DeleteObject"
)

type StorageOutboxEntryEntity struct {
	Id        *ulid.ULID
	Operation string
	Bucket    string
	Key       string
	Data      []byte
	Ordinal   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToStorageOutboxEntryEntity(storageOutboxRow *sql.Row) (*StorageOutboxEntryEntity, error) {
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
	return &StorageOutboxEntryEntity{
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
func (sor *StorageOutboxEntryRepository) NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error) {
	row := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM storage_outbox_entries")
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

func (sor *StorageOutboxEntryRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*StorageOutboxEntryEntity, error) {
	row := tx.QueryRowContext(ctx, "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal ASC LIMIT 1")
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *StorageOutboxEntryRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*StorageOutboxEntryEntity, error) {
	row := tx.QueryRowContext(ctx, "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries ORDER BY ordinal DESC LIMIT 1")
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *StorageOutboxEntryRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*StorageOutboxEntryEntity, error) {
	row := tx.QueryRowContext(ctx, "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal ASC LIMIT 1", bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *StorageOutboxEntryRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*StorageOutboxEntryEntity, error) {
	row := tx.QueryRowContext(ctx, "SELECT id, operation, bucket, key, data, ordinal, created_at, updated_at FROM storage_outbox_entries WHERE bucket = ? ORDER BY ordinal DESC LIMIT 1", bucket)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *StorageOutboxEntryRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *StorageOutboxEntryEntity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, "INSERT INTO storage_outbox_entries (id, operation, bucket, key, data, ordinal, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", storageOutboxEntry.Id.String(), storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, "UPDATE storage_outbox_entries SET operation = ?, bucket = ?, key = ?, data = ?, ordinal = ?, updated_at = ? WHERE id = ?", storageOutboxEntry.Operation, storageOutboxEntry.Bucket, storageOutboxEntry.Key, storageOutboxEntry.Data, storageOutboxEntry.Ordinal, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String())
	return err
}

func (sor *StorageOutboxEntryRepository) DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM storage_outbox_entries WHERE id = ?", id.String())
	return err
}
