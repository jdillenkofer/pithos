package storageoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type StorageOutboxEntryRepository interface {
	NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error)
	FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*StorageOutboxEntryEntity, error)
	FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*StorageOutboxEntryEntity, error)
	FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*StorageOutboxEntryEntity, error)
	FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucket string) (*StorageOutboxEntryEntity, error)
	SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *StorageOutboxEntryEntity) error
	DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

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

const (
	CreateBucketStorageOperation = "CreateBucket"
	DeleteBucketStorageOperation = "DeleteBucket"
	PutObjectStorageOperation    = "PutObject"
	DeleteObjectStorageOperation = "DeleteObject"
)
