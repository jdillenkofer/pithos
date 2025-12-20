package part

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindInUsePartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error)
	FindPartsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]Entity, error)
	SavePart(ctx context.Context, tx *sql.Tx, part *Entity) error
	DeletePartsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type Entity struct {
	Id                *ulid.ULID
	PartId            partstore.PartId
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
