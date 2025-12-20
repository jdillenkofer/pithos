package partcontent

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type Repository interface {
	FindPartContentChunksById(ctx context.Context, tx *sql.Tx, id partstore.PartId) ([]*Entity, error)
	FindPartContentIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error)
	PutPartContent(ctx context.Context, tx *sql.Tx, partContent *Entity) error
	SavePartContent(ctx context.Context, tx *sql.Tx, partContent *Entity) error
	DeletePartContentById(ctx context.Context, tx *sql.Tx, id partstore.PartId) error
}

type Entity struct {
	Id         *partstore.PartId
	ChunkIndex int
	Content    []byte
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
