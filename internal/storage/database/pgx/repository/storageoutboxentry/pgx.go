package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	countStorageOutboxEntriesStmt                    = "SELECT COUNT(*) FROM storage_outbox_entries WHERE outbox_id = $1"
	findFirstStorageOutboxEntryStmt                  = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryStmt                   = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 ORDER BY id DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketStmt         = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketStmt          = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 ORDER BY id DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND (key = '' OR key = $3) ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt  = "SELECT id, operation, bucket, key, content_type, created_at, updated_at FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND (key = '' OR key = $3) ORDER BY id DESC LIMIT 1"
	findStorageOutboxEntryChunksByIdStmt             = "SELECT c.outbox_entry_id, c.chunk_index, c.content FROM storage_outbox_contents c INNER JOIN storage_outbox_entries e ON e.id = c.outbox_entry_id WHERE c.outbox_entry_id = $1 AND e.outbox_id = $2 ORDER BY c.chunk_index ASC"
	insertStorageOutboxEntryStmt                     = "INSERT INTO storage_outbox_entries (id, outbox_id, operation, bucket, key, content_type, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
	updateStorageOutboxEntryByIdStmt                 = "UPDATE storage_outbox_entries SET operation = $1, bucket = $2, key = $3, content_type = $4, updated_at = $5 WHERE id = $6 AND outbox_id = $7"
	upsertStorageOutboxContentChunkStmt              = "INSERT INTO storage_outbox_contents (outbox_entry_id, chunk_index, content) VALUES($1, $2, $3) ON CONFLICT (outbox_entry_id, chunk_index) DO UPDATE SET content = EXCLUDED.content"
	deleteStorageOutboxEntryByIdStmt                 = "DELETE FROM storage_outbox_entries WHERE id = $1 AND outbox_id = $2"
)

func NewRepository() (storageoutboxentry.Repository, error) {
	return &pgxRepository{}, nil
}

func (sor *pgxRepository) Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, countStorageOutboxEntriesStmt, outboxId).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func convertRowToStorageOutboxEntryEntity(storageOutboxRow *sql.Row) (*storageoutboxentry.Entity, error) {
	var id string
	var operation string
	var bucket string
	var key string
	var contentType *string
	var createdAt time.Time
	var updatedAt time.Time
	err := storageOutboxRow.Scan(&id, &operation, &bucket, &key, &contentType, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &storageoutboxentry.Entity{
		Id:          &ulidId,
		Operation:   operation,
		Bucket:      storage.MustNewBucketName(bucket),
		Key:         key,
		ContentType: contentType,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func (sor *pgxRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryStmt, outboxId)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryStmt, outboxId)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindFirstStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt, outboxId, bucketName.String(), key)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindLastStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt, outboxId, bucketName.String(), key)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *pgxRepository) FindStorageOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*storageoutboxentry.ContentChunk, error) {
	rows, err := tx.QueryContext(ctx, findStorageOutboxEntryChunksByIdStmt, id.String(), outboxId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []*storageoutboxentry.ContentChunk
	for rows.Next() {
		var entryIdStr string
		var chunkIndex int
		var content []byte
		err := rows.Scan(&entryIdStr, &chunkIndex, &content)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, &storageoutboxentry.ContentChunk{
			OutboxEntryId: ulid.MustParse(entryIdStr),
			ChunkIndex:    chunkIndex,
			Content:       content,
		})
	}
	return chunks, nil
}

func (sor *pgxRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, storageOutboxEntry *storageoutboxentry.Entity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now().UTC()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertStorageOutboxEntryStmt, storageOutboxEntry.Id.String(), outboxId, storageOutboxEntry.Operation, storageOutboxEntry.Bucket.String(), storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateStorageOutboxEntryByIdStmt, storageOutboxEntry.Operation, storageOutboxEntry.Bucket.String(), storageOutboxEntry.Key, storageOutboxEntry.ContentType, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String(), outboxId)
	return err
}

func (sor *pgxRepository) SaveStorageOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *storageoutboxentry.ContentChunk) error {
	_, err := tx.ExecContext(ctx, upsertStorageOutboxContentChunkStmt, chunk.OutboxEntryId.String(), chunk.ChunkIndex, chunk.Content)
	return err
}

func (sor *pgxRepository) DeleteStorageOutboxEntryById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteStorageOutboxEntryByIdStmt, id.String(), outboxId)
	return err
}
