package partcontent

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Repository persists part content chunks. Every method is scoped by a
// partStoreId discriminator so that multiple SqlPartStore instances can share
// one database/table without seeing each other's parts. Existing rows and
// single-store deployments use the "default" discriminator (DefaultPartStoreId).
type Repository interface {
	FindPartContentChunksById(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId) ([]*Entity, error)
	// FindPartContentChunkByIndex returns the chunk with the given index for the
	// part, or (nil, nil) if no such chunk exists. It lets callers stream a part
	// one chunk at a time instead of loading every chunk into memory at once.
	FindPartContentChunkByIndex(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId, chunkIndex int) (*Entity, error)
	FindPartContentIds(ctx context.Context, tx *sql.Tx, partStoreId string) ([]partstore.PartId, error)
	PutPartContent(ctx context.Context, tx *sql.Tx, partStoreId string, partContent *Entity) error
	SavePartContent(ctx context.Context, tx *sql.Tx, partStoreId string, partContent *Entity) error
	DeletePartContentById(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId) error
}

// DefaultPartStoreId is the discriminator used for rows written before the
// part_store_id column existed and for SqlPartStore instances that do not
// configure an explicit id.
const DefaultPartStoreId = "default"

type Entity struct {
	Id         *partstore.PartId
	ChunkIndex int
	Content    []byte
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
