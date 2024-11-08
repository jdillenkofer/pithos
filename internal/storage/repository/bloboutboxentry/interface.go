package bloboutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobOutboxEntryRepository interface {
	NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error)
	FindLastBlobOutboxEntryByBlobId(ctx context.Context, tx *sql.Tx, blobId ulid.ULID) (*BlobOutboxEntryEntity, error)
	FindLastBlobOutboxEntryGroupedByBlobId(ctx context.Context, tx *sql.Tx) ([]BlobOutboxEntryEntity, error)
	FindFirstBlobOutboxEntry(ctx context.Context, tx *sql.Tx) (*BlobOutboxEntryEntity, error)
	SaveBlobOutboxEntry(ctx context.Context, tx *sql.Tx, blobOutboxEntry *BlobOutboxEntryEntity) error
	DeleteBlobOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type BlobOutboxEntryEntity struct {
	Id        *ulid.ULID
	Operation string
	BlobId    ulid.ULID
	Content   []byte
	Ordinal   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

const (
	PutBlobOperation    = "PutBlob"
	DeleteBlobOperation = "DeleteBlob"
)
