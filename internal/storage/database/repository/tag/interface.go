package tag

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindTagsByObjectIdOrderByKeyAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]Entity, error)
	SaveTag(ctx context.Context, tx *sql.Tx, tag *Entity) error
	DeleteTagsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type Entity struct {
	Id        *ulid.ULID
	ObjectId  ulid.ULID
	Key       string
	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}
