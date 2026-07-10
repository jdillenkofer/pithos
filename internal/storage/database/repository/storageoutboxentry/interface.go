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
	// The Global variants match only bucket-lifecycle entries (CreateBucket/
	// DeleteBucket, stored with an empty key), letting callers that depend only
	// on bucket existence or configuration avoid waiting on queued object
	// writes.
	FindFirstGlobalStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	FindLastGlobalStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	FindFirstGlobalStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*Entity, error)
	FindLastGlobalStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*Entity, error)
	FindStorageOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*ContentChunk, error)
	SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, storageOutboxEntry *Entity) error
	SaveStorageOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *ContentChunk) error
	// SaveStorageOutboxEntryPutOptions persists the PutObject options for an
	// already saved entry so a replay can apply them. It must run in the same
	// transaction as SaveStorageOutboxEntry.
	SaveStorageOutboxEntryPutOptions(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, putOptions *PutOptions) error
	// FindStorageOutboxEntryPutOptionsById loads the persisted PutObject
	// options for an entry. Options that were never set come back as their
	// zero values, which replay treats the same as absent.
	FindStorageOutboxEntryPutOptionsById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) (*PutOptions, error)
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
	VersionID   *string
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

// PutOptions holds the PutObject options persisted with a PutObject entry so
// the outbox replay can apply them. A full put replaces tags and metadata
// with exactly the supplied set, so nil and empty are equivalent here.
type PutOptions struct {
	StorageClass *string
	Tags         map[string]string
	Metadata     *storage.ObjectMetadata
}

const (
	CreateBucketStorageOperation = "CreateBucket"
	DeleteBucketStorageOperation = "DeleteBucket"
	PutObjectStorageOperation    = "PutObject"
	DeleteObjectStorageOperation = "DeleteObject"
)
