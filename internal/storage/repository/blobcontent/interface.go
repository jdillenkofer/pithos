package blobcontent

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobContentRepository interface {
	FindBlobContentById(ctx context.Context, tx *sql.Tx, blobContentId ulid.ULID) (*BlobContentEntity, error)
	FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error)
	PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *BlobContentEntity) error
	SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *BlobContentEntity) error
	DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type BlobContentEntity struct {
	Id        *ulid.ULID
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}
