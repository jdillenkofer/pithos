package partoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindLastPartOutboxEntryByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (*Entity, error)
	FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindFirstPartOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, partOutboxEntry *Entity) error
	DeletePartOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
	Id        *ulid.ULID
	Operation string
	PartId    partstore.PartId
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

const (
	PutPartOperation    = "PutPart"
	DeletePartOperation = "DeletePart"
)
