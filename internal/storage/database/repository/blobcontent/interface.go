package blobcontent

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindBlobContentById(ctx context.Context, tx *sql.Tx, blobContentId ulid.ULID) (*Entity, error)
	FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error)
	PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *Entity) error
	SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *Entity) error
	DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
	Id        *ulid.ULID
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}
