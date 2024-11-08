package blob

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobRepository interface {
	FindInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error)
	FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]BlobEntity, error)
	SaveBlob(ctx context.Context, tx *sql.Tx, blob *BlobEntity) error
	DeleteBlobsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type BlobEntity struct {
	Id             *ulid.ULID
	BlobId         ulid.ULID
	ObjectId       ulid.ULID
	ETag           string
	Size           int64
	SequenceNumber int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
