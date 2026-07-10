package partdedupindex

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type Repository interface {
	FindEntry(ctx context.Context, tx *sql.Tx, partStoreName, checksumSHA256 string, size int64) (*Entity, error)
	TryInsert(ctx context.Context, tx *sql.Tx, entity *Entity) (bool, error)
	DeleteByPartIds(ctx context.Context, tx *sql.Tx, partIds []partstore.PartId) error
	FindAllPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error)
}

type Entity struct {
	PartStoreName     string
	ChecksumSHA256    string
	Size              int64
	ETag              string
	ChecksumCRC32     string
	ChecksumCRC32C    string
	ChecksumCRC64NVME string
	ChecksumSHA1      string
	PartId            partstore.PartId
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
