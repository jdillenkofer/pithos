package bloboutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindLastBlobOutboxEntryByBlobId(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (*Entity, error)
	FindLastBlobOutboxEntryGroupedByBlobId(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindFirstBlobOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindFirstBlobOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	SaveBlobOutboxEntry(ctx context.Context, tx *sql.Tx, blobOutboxEntry *Entity) error
	DeleteBlobOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
	Id        *ulid.ULID
	Operation string
	BlobId    blobstore.BlobId
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

const (
	PutBlobOperation    = "PutBlob"
	DeleteBlobOperation = "DeleteBlob"
)
