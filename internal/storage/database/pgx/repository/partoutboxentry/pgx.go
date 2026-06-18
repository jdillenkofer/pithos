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
	countPartOutboxEntriesStmt                 = "SELECT COUNT(*) FROM part_outbox_entries WHERE outbox_id = $1"
	findLastPartOutboxEntryByPartIdStmt        = "SELECT id, operation, part_id, created_at, updated_at, claim_owner, claim_until, version FROM part_outbox_entries WHERE outbox_id = $1 AND part_id = $2 ORDER BY id DESC LIMIT 1"
	findLastPartOutboxEntryGroupedByPartIdStmt = "SELECT DISTINCT ON (part_id) id, operation, part_id, created_at, updated_at, claim_owner, claim_until, version FROM part_outbox_entries WHERE outbox_id = $1 ORDER BY part_id, id DESC"
	findFirstPartOutboxEntryStmt               = "SELECT id, operation, part_id, created_at, updated_at, claim_owner, claim_until, version FROM part_outbox_entries WHERE outbox_id = $1 ORDER BY id ASC LIMIT 1"
	findPartOutboxEntryChunksByIdStmt          = "SELECT c.outbox_entry_id, c.chunk_index, c.content FROM part_outbox_contents c INNER JOIN part_outbox_entries e ON e.id = c.outbox_entry_id WHERE c.outbox_entry_id = $1 AND e.outbox_id = $2 ORDER BY c.chunk_index ASC"
	insertPartOutboxEntryStmt                  = "INSERT INTO part_outbox_entries (id, outbox_id, operation, part_id, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	updatePartOutboxEntryByIdStmt              = "UPDATE part_outbox_entries SET operation = $1, part_id = $2, updated_at = $3 WHERE id = $4 AND outbox_id = $5"
	upsertPartOutboxContentChunkStmt           = "INSERT INTO part_outbox_contents (outbox_entry_id, chunk_index, content) VALUES($1, $2, $3) ON CONFLICT (outbox_entry_id, chunk_index) DO UPDATE SET content = EXCLUDED.content"
	claimPartOutboxEntryStmt                   = "UPDATE part_outbox_entries SET claim_owner = $1, claim_until = $2, version = version + 1, updated_at = $3 WHERE id = $4 AND outbox_id = $5 AND version = $6 AND (claim_owner IS NULL OR claim_until <= $7)"
	deletePartOutboxEntryByClaimOwnerStmt      = "DELETE FROM part_outbox_entries WHERE id = $1 AND outbox_id = $2 AND claim_owner = $3"
	releasePartOutboxEntryClaimStmt            = "UPDATE part_outbox_entries SET claim_owner = NULL, claim_until = NULL, version = version + 1, updated_at = $1 WHERE id = $2 AND outbox_id = $3 AND claim_owner = $4"
	extendPartOutboxEntryClaimStmt             = "UPDATE part_outbox_entries SET claim_until = $1, version = version + 1, updated_at = $2 WHERE id = $3 AND outbox_id = $4 AND claim_owner = $5"
)

func NewRepository() (partoutboxentry.Repository, error) {
	return &pgxRepository{}, nil
}

func (bor *pgxRepository) Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countPartOutboxEntriesStmt, outboxId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func convertRowToPartOutboxEntryEntity(partOutboxRow *sql.Row) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var createdAt time.Time
	var updatedAt time.Time
	var claimOwner *string
	var claimUntil *time.Time
	var version int64
	err := partOutboxRow.Scan(&id, &operation, &partIdStr, &createdAt, &updatedAt, &claimOwner, &claimUntil, &version)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:         &ulidId,
		Operation:  operation,
		PartId:     *partId,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
		ClaimOwner: claimOwner,
		ClaimUntil: claimUntil,
		Version:    version,
	}, nil
}

func convertRowsToPartOutboxEntryEntity(partOutboxRows *sql.Rows) (*partoutboxentry.Entity, error) {
	var id string
	var operation string
	var partIdStr string
	var createdAt time.Time
	var updatedAt time.Time
	var claimOwner *string
	var claimUntil *time.Time
	var version int64
	err := partOutboxRows.Scan(&id, &operation, &partIdStr, &createdAt, &updatedAt, &claimOwner, &claimUntil, &version)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	return &partoutboxentry.Entity{
		Id:         &ulidId,
		Operation:  operation,
		PartId:     *partId,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
		ClaimOwner: claimOwner,
		ClaimUntil: claimUntil,
		Version:    version,
	}, nil
}

func (bor *pgxRepository) FindLastPartOutboxEntryByPartId(ctx context.Context, tx *sql.Tx, outboxId string, partId partstore.PartId) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastPartOutboxEntryByPartIdStmt, outboxId, partId.String())
	return convertRowToPartOutboxEntryEntity(row)
}

func (bor *pgxRepository) FindLastPartOutboxEntryGroupedByPartId(ctx context.Context, tx *sql.Tx, outboxId string) ([]partoutboxentry.Entity, error) {
	partOutboxEntryRows, err := tx.QueryContext(ctx, findLastPartOutboxEntryGroupedByPartIdStmt, outboxId)
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

func (bor *pgxRepository) findFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*partoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstPartOutboxEntryStmt, outboxId)
	partOutboxEntryEntity, err := convertRowToPartOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return partOutboxEntryEntity, nil
}

func (bor *pgxRepository) FindPartOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*partoutboxentry.ContentChunk, error) {
	rows, err := tx.QueryContext(ctx, findPartOutboxEntryChunksByIdStmt, id.String(), outboxId)
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

func (bor *pgxRepository) SavePartOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, partOutboxEntry *partoutboxentry.Entity) error {
	if partOutboxEntry.Id == nil {
		id := ulid.Make()
		partOutboxEntry.Id = &id
		partOutboxEntry.CreatedAt = time.Now().UTC()
		partOutboxEntry.UpdatedAt = partOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertPartOutboxEntryStmt, partOutboxEntry.Id.String(), outboxId, partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.CreatedAt, partOutboxEntry.UpdatedAt)
		return err
	}

	partOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartOutboxEntryByIdStmt, partOutboxEntry.Operation, partOutboxEntry.PartId.String(), partOutboxEntry.UpdatedAt, partOutboxEntry.Id.String(), outboxId)
	return err
}

func (bor *pgxRepository) SavePartOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *partoutboxentry.ContentChunk) error {
	_, err := tx.ExecContext(ctx, upsertPartOutboxContentChunkStmt, chunk.OutboxEntryId.String(), chunk.ChunkIndex, chunk.Content)
	return err
}

func (bor *pgxRepository) ClaimFirstPartOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, owner string, now time.Time, claimUntil time.Time) (*partoutboxentry.Entity, bool, error) {
	entry, err := bor.findFirstPartOutboxEntry(ctx, tx, outboxId)
	if err != nil || entry == nil {
		return entry, false, err
	}
	result, err := tx.ExecContext(ctx, claimPartOutboxEntryStmt, owner, claimUntil, now, entry.Id.String(), outboxId, entry.Version, now)
	if err != nil {
		return nil, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if affected != 1 {
		return nil, false, nil
	}
	entry.ClaimOwner = &owner
	entry.ClaimUntil = &claimUntil
	entry.Version += 1
	entry.UpdatedAt = now
	return entry, true, nil
}

func (bor *pgxRepository) DeletePartOutboxEntryByClaimOwner(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string) (bool, error) {
	result, err := tx.ExecContext(ctx, deletePartOutboxEntryByClaimOwnerStmt, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (bor *pgxRepository) ReleasePartOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, releasePartOutboxEntryClaimStmt, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (bor *pgxRepository) ExtendPartOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, extendPartOutboxEntryClaimStmt, claimUntil, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}
