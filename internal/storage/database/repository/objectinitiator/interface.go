package objectinitiator

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

// Repository persists the identity that initiated a multipart upload. The
// object row of a pending upload is the upload, so rows are keyed by object id.
type Repository interface {
	FindObjectInitiatorByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) (*Entity, error)
	// FindObjectInitiatorsByObjectIdsOrderByObjectId returns the initiators of
	// all given objects in a single query.
	FindObjectInitiatorsByObjectIdsOrderByObjectId(ctx context.Context, tx *sql.Tx, objectIds []ulid.ULID) ([]Entity, error)
	SaveObjectInitiator(ctx context.Context, tx *sql.Tx, initiator *Entity) error
	DeleteObjectInitiatorByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

// Entity is the stored initiator of a multipart upload.
type Entity struct {
	ObjectId    ulid.ULID
	AccountId   string
	PrincipalId string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
