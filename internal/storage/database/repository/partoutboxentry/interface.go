package partoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindLastPartOutboxEntryIdByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (*ulid.ULID, error)
	FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindFirstPartOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx) (*Entity, error)
	FindPartOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, id ulid.ULID) ([]*ContentChunk, error)
	SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, partOutboxEntry *Entity) error
	SavePartOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *ContentChunk) error
	DeletePartOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error
}

type Entity struct {
	Id        *ulid.ULID
	Operation string
	PartId    partstore.PartId
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ContentChunk struct {
	OutboxEntryId ulid.ULID
	ChunkIndex    int
	Content       []byte
}

const (
	PutPartOperation    = "PutPart"
	DeletePartOperation = "DeletePart"
)
