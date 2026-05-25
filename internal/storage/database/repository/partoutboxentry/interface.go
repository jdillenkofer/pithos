package partoutboxentry

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error)
	FindLastPartOutboxEntryByPartId(ctx context.Context, tx *sql.Tx, outboxId string, partId partstore.PartId) (*Entity, error)
	FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx, outboxId string) ([]Entity, error)
	FindFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*Entity, error)
	FindPartOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*ContentChunk, error)
	SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, partOutboxEntry *Entity) error
	SavePartOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *ContentChunk) error
	DeletePartOutboxEntryById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) error
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
