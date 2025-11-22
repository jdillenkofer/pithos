package blobcontent

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type Repository interface {
	FindBlobContentById(ctx context.Context, tx *sql.Tx, id blobstore.BlobId) (*Entity, error)
	FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error)
	PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *Entity) error
	SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *Entity) error
	DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id blobstore.BlobId) error
}

type Entity struct {
	Id        *blobstore.BlobId
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}
