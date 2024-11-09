package bloboutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	NextOrdinal(ctx context.Context, tx *sql.Tx) (*int, error)
	FindLastBlobOutboxEntryByBlobId(ctx context.Context, tx *sql.Tx, blobId ulid.ULID) (*Entity, error)
	FindLastBlobOutboxEntryGroupedByBlobId(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindFirstBlobOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	SaveBlobOutboxEntry(ctx context.Context, tx *sql.Tx, blobOutboxEntry *Entity) error
	DeleteBlobOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
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
