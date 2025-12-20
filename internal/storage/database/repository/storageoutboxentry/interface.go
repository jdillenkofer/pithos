package storageoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindFirstStorageOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*Entity, error)
	FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*Entity, error)
	FindStorageOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, id ulid.ULID) ([]*ContentChunk, error)
	SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, storageOutboxEntry *Entity) error
	SaveStorageOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *ContentChunk) error
	DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
	Id          *ulid.ULID
	Operation   string
	Bucket      storage.BucketName
	Key         string
	ContentType *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type ContentChunk struct {
	OutboxEntryId ulid.ULID
	ChunkIndex    int
	Content       []byte
}

const (
	CreateBucketStorageOperation = "CreateBucket"
	DeleteBucketStorageOperation = "DeleteBucket"
	PutObjectStorageOperation    = "PutObject"
	DeleteObjectStorageOperation = "DeleteObject"
)
