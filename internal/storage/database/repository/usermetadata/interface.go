package usermetadata

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindUserMetadataByObjectIdOrderByKeyAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]Entity, error)
	SaveUserMetadata(ctx context.Context, tx *sql.Tx, userMetadata *Entity) error
	DeleteUserMetadataByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

// Entity is a single user-defined metadata entry (x-amz-meta-*) of an object.
// Keys are stored lowercase without the "x-amz-meta-" prefix.
type Entity struct {
	Id        *ulid.ULID
	ObjectId  ulid.ULID
	Key       string
	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}
