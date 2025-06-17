package blob

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error)
	FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]Entity, error)
	SaveBlob(ctx context.Context, tx *sql.Tx, blob *Entity) error
	DeleteBlobsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type Entity struct {
	Id                *ulid.ULID
	BlobId            ulid.ULID
	ObjectId          ulid.ULID
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	Size              int64
	SequenceNumber    int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
