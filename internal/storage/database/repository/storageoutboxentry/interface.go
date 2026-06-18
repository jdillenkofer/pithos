package storageoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error)
	FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*Entity, error)
	FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*Entity, error)
	FindFirstStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*Entity, error)
	FindLastStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*Entity, error)
	FindStorageOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*ContentChunk, error)
	SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, storageOutboxEntry *Entity) error
	SaveStorageOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *ContentChunk) error
	DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) error
	ClaimFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, owner string, now time.Time, claimUntil time.Time) (*Entity, bool, error)
	DeleteStorageOutboxEntryByClaimOwner(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string) (bool, error)
	ReleaseStorageOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time) (bool, error)
	ExtendStorageOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error)
}

type Entity struct {
	Id          *ulid.ULID
	Operation   string
	Bucket      storage.BucketName
	Key         string
	ContentType *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ClaimOwner  *string
	ClaimUntil  *time.Time
	Version     int64
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
