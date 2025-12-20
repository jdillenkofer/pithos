package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findLastPartOutboxEntryIdByPartIdStmt         = "SELECT id FROM part_outbox_entries WHERE part_id = $1 ORDER BY id DESC LIMIT 1"
	findLastPartOutboxEntryGroupedByPartIdStmt    = "SELECT DISTINCT ON (part_id) id, operation, part_id, created_at, updated_at FROM part_outbox_entries ORDER BY part_id, id DESC"
	findFirstPartOutboxEntryStmt                  = "SELECT id, operation, part_id, created_at, updated_at FROM part_outbox_entries ORDER BY id ASC LIMIT 1"
	findFirstPartOutboxEntryWithForUpdateLockStmt = "SELECT id, operation, part_id, created_at, updated_at FROM part_outbox_entries ORDER BY id ASC LIMIT 1 FOR UPDATE"
	findPartOutboxEntryChunksByIdStmt             = "SELECT outbox_entry_id, chunk_index, content FROM part_outbox_contents WHERE outbox_entry_id = $1 ORDER BY chunk_index ASC"
	insertPartOutboxEntryStmt                     = "INSERT INTO part_outbox_entries (id, operation, part_id, created_at, updated_at) VALUES($1, $2, $3, $4, $5)"
	updatePartOutboxEntryByIdStmt                 = "UPDATE part_outbox_entries SET operation = $1, part_id = $2, updated_at = $3 WHERE id = $4"
	upsertPartOutboxContentChunkStmt              = "INSERT INTO part_outbox_contents (outbox_entry_id, chunk_index, content) VALUES($1, $2, $3) ON CONFLICT (outbox_entry_id, chunk_index) DO UPDATE SET content = EXCLUDED.content"
	deletePartOutboxEntryByIdStmt                 = "DELETE FROM part_outbox_entries WHERE id = $1"
)

func NewRepository() (partoutboxentry.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToPartOutboxEntryEntity(partOutboxRow *sql.Row) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var createdAt time.Time
	var updatedAt time.Time
	err := partOutboxRow.Scan(&id, &operation, &partIdStr, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		PartId:    *partId,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func convertRowsToPartOutboxEntryEntity(partOutboxRows *sql.Rows) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var createdAt time.Time
	var updatedAt time.Time
	err := partOutboxRows.Scan(&id, &operation, &partIdStr, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:        &ulidId,
		Operation: operation,
		PartId:    *partId,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bor *pgxRepository) FindLastPartOutboxEntryIdByPartId(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (*ulid.ULID, error) {
	row := tx.QueryRowContext(ctx, findLastPartOutboxEntryIdByPartIdStmt, partId.String())
	var id string
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	u := ulid.MustParse(id)
	return &u, nil
}

func (bor *pgxRepository) FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx) ([]partoutboxentry.Entity, error) {
	partOutboxEntryRows, err := tx.QueryContext(ctx, findLastPartOutboxEntryGroupedByPartIdStmt)
	if err != nil {
		return nil, err
	}
	defer partOutboxEntryRows.Close()
	partOutboxEntryEntities := []partoutboxentry.Entity{}
	for partOutboxEntryRows.Next() {
		partOutboxEntryEntity, err := convertRowsToPartOutboxEntryEntity(partOutboxEntryRows)
		if err != nil {
			return nil, err
		}
		partOutboxEntryEntities = append(partOutboxEntryEntities, *partOutboxEntryEntity)
	}
	return partOutboxEntryEntities, nil
}

func (bor *pgxRepository) FindFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstPartOutboxEntryStmt)
	partOutboxEntryEntity, err := convertRowToPartOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return partOutboxEntryEntity, nil
}

func (bor *pgxRepository) FindFirstPartOutboxEntryWithForUpdateLock(ctx context.Context, tx *sql.Tx) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstPartOutboxEntryWithForUpdateLockStmt)
	partOutboxEntryEntity, err := convertRowToPartOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return partOutboxEntryEntity, nil
}

func (bor *pgxRepository) FindPartOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, id ulid.ULID) ([]*partoutboxentry.ContentChunk, error) {
	rows, err := tx.QueryContext(ctx, findPartOutboxEntryChunksByIdStmt, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []*partoutboxentry.ContentChunk
	for rows.Next() {
		var entryIdStr string
		var chunkIndex int
		var content []byte
		err := rows.Scan(&entryIdStr, &chunkIndex, &content)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, &partoutboxentry.ContentChunk{
			OutboxEntryId: ulid.MustParse(entryIdStr),
			ChunkIndex:    chunkIndex,
			Content:       content,
		})
	}
	return chunks, nil
}

func (bor *pgxRepository) SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, partOutboxEntry *partoutboxentry.Entity) error {
	if partOutboxEntry.Id == nil {
		id := ulid.Make()
		partOutboxEntry.Id = &id
		partOutboxEntry.CreatedAt = time.Now().UTC()
		partOutboxEntry.UpdatedAt = partOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertPartOutboxEntryStmt, partOutboxEntry.Id.String(), partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.CreatedAt, partOutboxEntry.UpdatedAt)
		return err
	}

	partOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartOutboxEntryByIdStmt, partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.UpdatedAt, partOutboxEntry.Id.String())
	return err
}

func (bor *pgxRepository) SavePartOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *partoutboxentry.ContentChunk) error {
	_, err := tx.ExecContext(ctx, upsertPartOutboxContentChunkStmt, chunk.OutboxEntryId.String(), chunk.ChunkIndex, chunk.Content)
	return err
}

func (bor *pgxRepository) DeletePartOutboxEntryById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deletePartOutboxEntryByIdStmt, id.String())
	return err
}
