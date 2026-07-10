package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	countStorageOutboxEntriesStmt                                 = "SELECT COUNT(*) FROM storage_outbox_entries WHERE outbox_id = $1"
	findFirstStorageOutboxEntryStmt                               = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryStmt                                = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 ORDER BY id DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketStmt                      = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketStmt                       = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 ORDER BY id DESC LIMIT 1"
	findFirstStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND (key = '' OR key = $3) ORDER BY id ASC LIMIT 1"
	findLastStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt  = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND (key = '' OR key = $3) ORDER BY id DESC LIMIT 1"
	findFirstGlobalStorageOutboxEntryStmt                         = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND key = '' ORDER BY id ASC LIMIT 1"
	findLastGlobalStorageOutboxEntryStmt                          = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND key = '' ORDER BY id DESC LIMIT 1"
	findFirstGlobalStorageOutboxEntryForBucketStmt                = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND key = '' ORDER BY id ASC LIMIT 1"
	findLastGlobalStorageOutboxEntryForBucketStmt                 = "SELECT id, operation, bucket, key, version_id, content_type, created_at, updated_at, claim_owner, claim_until, version FROM storage_outbox_entries WHERE outbox_id = $1 AND bucket = $2 AND key = '' ORDER BY id DESC LIMIT 1"
	findStorageOutboxEntryChunksByIdStmt                          = "SELECT c.outbox_entry_id, c.chunk_index, c.content FROM storage_outbox_contents c INNER JOIN storage_outbox_entries e ON e.id = c.outbox_entry_id WHERE c.outbox_entry_id = $1 AND e.outbox_id = $2 ORDER BY c.chunk_index ASC"
	insertStorageOutboxEntryStmt                                  = "INSERT INTO storage_outbox_entries (id, outbox_id, operation, bucket, key, version_id, content_type, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	updateStorageOutboxEntryByIdStmt                              = "UPDATE storage_outbox_entries SET operation = $1, bucket = $2, key = $3, version_id = $4, content_type = $5, updated_at = $6 WHERE id = $7 AND outbox_id = $8"
	upsertStorageOutboxContentChunkStmt                           = "INSERT OR REPLACE INTO storage_outbox_contents (outbox_entry_id, chunk_index, content) VALUES($1, $2, $3)"
	updateStorageOutboxEntryPutOptionsStmt                        = "UPDATE storage_outbox_entries SET storage_class = $1, cache_control = $2, content_disposition = $3, content_encoding = $4, content_language = $5, expires = $6, website_redirect_location = $7 WHERE id = $8 AND outbox_id = $9"
	findStorageOutboxEntryPutOptionsStmt                          = "SELECT storage_class, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location FROM storage_outbox_entries WHERE id = $1 AND outbox_id = $2"
	upsertStorageOutboxEntryTagStmt                               = "INSERT OR REPLACE INTO storage_outbox_entry_tags (outbox_entry_id, key, value) VALUES($1, $2, $3)"
	findStorageOutboxEntryTagsStmt                                = "SELECT t.key, t.value FROM storage_outbox_entry_tags t INNER JOIN storage_outbox_entries e ON e.id = t.outbox_entry_id WHERE t.outbox_entry_id = $1 AND e.outbox_id = $2"
	upsertStorageOutboxEntryUserMetadataStmt                      = "INSERT OR REPLACE INTO storage_outbox_entry_user_metadata (outbox_entry_id, key, value) VALUES($1, $2, $3)"
	findStorageOutboxEntryUserMetadataStmt                        = "SELECT m.key, m.value FROM storage_outbox_entry_user_metadata m INNER JOIN storage_outbox_entries e ON e.id = m.outbox_entry_id WHERE m.outbox_entry_id = $1 AND e.outbox_id = $2"
	claimStorageOutboxEntryStmt                                   = "UPDATE storage_outbox_entries SET claim_owner = $1, claim_until = $2, version = version + 1, updated_at = $3 WHERE id = $4 AND outbox_id = $5 AND version = $6 AND (claim_owner IS NULL OR claim_until <= $7)"
	deleteStorageOutboxEntryByClaimOwnerStmt                      = "DELETE FROM storage_outbox_entries WHERE id = $1 AND outbox_id = $2 AND claim_owner = $3"
	releaseStorageOutboxEntryClaimStmt                            = "UPDATE storage_outbox_entries SET claim_owner = NULL, claim_until = NULL, version = version + 1, updated_at = $1 WHERE id = $2 AND outbox_id = $3 AND claim_owner = $4"
	extendStorageOutboxEntryClaimStmt                             = "UPDATE storage_outbox_entries SET claim_until = $1, version = version + 1, updated_at = $2 WHERE id = $3 AND outbox_id = $4 AND claim_owner = $5"
)

func NewRepository() (storageoutboxentry.Repository, error) {
	return &sqliteRepository{}, nil
}

func (sor *sqliteRepository) Count(ctx context.Context, tx *sql.Tx, outboxId string) (int, error) {
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
	var versionID *string
	var contentType *string
	var createdAt time.Time
	var updatedAt time.Time
	var claimOwner *string
	var claimUntil *time.Time
	var version int64
	err := storageOutboxRow.Scan(&id, &operation, &bucket, &key, &versionID, &contentType, &createdAt, &updatedAt, &claimOwner, &claimUntil, &version)
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
		VersionID:   versionID,
		ContentType: contentType,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		ClaimOwner:  claimOwner,
		ClaimUntil:  claimUntil,
		Version:     version,
	}, nil
}

func (sor *sqliteRepository) FindFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryStmt, outboxId)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindLastStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryStmt, outboxId)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindFirstStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindLastStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindFirstStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt, outboxId, bucketName.String(), key)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindLastStorageOutboxEntryForBucketAndKeyIncludingGlobal(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName, key string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastStorageOutboxEntryForBucketAndKeyIncludingGlobalStmt, outboxId, bucketName.String(), key)
	storageOutboxEntryEntity, err := convertRowToStorageOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return storageOutboxEntryEntity, nil
}

func (sor *sqliteRepository) FindFirstGlobalStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstGlobalStorageOutboxEntryStmt, outboxId)
	return convertRowToStorageOutboxEntryEntity(row)
}

func (sor *sqliteRepository) FindLastGlobalStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastGlobalStorageOutboxEntryStmt, outboxId)
	return convertRowToStorageOutboxEntryEntity(row)
}

func (sor *sqliteRepository) FindFirstGlobalStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findFirstGlobalStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	return convertRowToStorageOutboxEntryEntity(row)
}

func (sor *sqliteRepository) FindLastGlobalStorageOutboxEntryForBucket(ctx context.Context, tx *sql.Tx, outboxId string, bucketName storage.BucketName) (*storageoutboxentry.Entity, error) {
	row := tx.QueryRowContext(ctx, findLastGlobalStorageOutboxEntryForBucketStmt, outboxId, bucketName.String())
	return convertRowToStorageOutboxEntryEntity(row)
}

func (sor *sqliteRepository) FindStorageOutboxEntryChunksById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) ([]*storageoutboxentry.ContentChunk, error) {
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

func (sor *sqliteRepository) SaveStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, storageOutboxEntry *storageoutboxentry.Entity) error {
	if storageOutboxEntry.Id == nil {
		id := ulid.Make()
		storageOutboxEntry.Id = &id
		storageOutboxEntry.CreatedAt = time.Now().UTC()
		storageOutboxEntry.UpdatedAt = storageOutboxEntry.CreatedAt
		_, err := tx.ExecContext(ctx, insertStorageOutboxEntryStmt, storageOutboxEntry.Id.String(), outboxId, storageOutboxEntry.Operation, storageOutboxEntry.Bucket.String(), storageOutboxEntry.Key, storageOutboxEntry.VersionID, storageOutboxEntry.ContentType, storageOutboxEntry.CreatedAt, storageOutboxEntry.UpdatedAt)
		return err
	}

	storageOutboxEntry.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateStorageOutboxEntryByIdStmt, storageOutboxEntry.Operation, storageOutboxEntry.Bucket.String(), storageOutboxEntry.Key, storageOutboxEntry.VersionID, storageOutboxEntry.ContentType, storageOutboxEntry.UpdatedAt, storageOutboxEntry.Id.String(), outboxId)
	return err
}

func (sor *sqliteRepository) SaveStorageOutboxContentChunk(ctx context.Context, tx *sql.Tx, chunk *storageoutboxentry.ContentChunk) error {
	_, err := tx.ExecContext(ctx, upsertStorageOutboxContentChunkStmt, chunk.OutboxEntryId.String(), chunk.ChunkIndex, chunk.Content)
	return err
}

func (sor *sqliteRepository) SaveStorageOutboxEntryPutOptions(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, putOptions *storageoutboxentry.PutOptions) error {
	var cacheControl, contentDisposition, contentEncoding, contentLanguage, expires, websiteRedirectLocation *string
	var userMetadata map[string]string
	if putOptions.Metadata != nil {
		cacheControl = putOptions.Metadata.CacheControl
		contentDisposition = putOptions.Metadata.ContentDisposition
		contentEncoding = putOptions.Metadata.ContentEncoding
		contentLanguage = putOptions.Metadata.ContentLanguage
		expires = putOptions.Metadata.Expires
		websiteRedirectLocation = putOptions.Metadata.WebsiteRedirectLocation
		userMetadata = putOptions.Metadata.UserMetadata
	}
	_, err := tx.ExecContext(ctx, updateStorageOutboxEntryPutOptionsStmt, putOptions.StorageClass, cacheControl, contentDisposition, contentEncoding, contentLanguage, expires, websiteRedirectLocation, id.String(), outboxId)
	if err != nil {
		return err
	}
	for key, value := range putOptions.Tags {
		if _, err := tx.ExecContext(ctx, upsertStorageOutboxEntryTagStmt, id.String(), key, value); err != nil {
			return err
		}
	}
	for key, value := range userMetadata {
		if _, err := tx.ExecContext(ctx, upsertStorageOutboxEntryUserMetadataStmt, id.String(), key, value); err != nil {
			return err
		}
	}
	return nil
}

func queryStorageOutboxEntryKeyValues(ctx context.Context, tx *sql.Tx, stmt string, id ulid.ULID, outboxId string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, stmt, id.String(), outboxId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, rows.Err()
}

func (sor *sqliteRepository) FindStorageOutboxEntryPutOptionsById(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID) (*storageoutboxentry.PutOptions, error) {
	row := tx.QueryRowContext(ctx, findStorageOutboxEntryPutOptionsStmt, id.String(), outboxId)
	var storageClass, cacheControl, contentDisposition, contentEncoding, contentLanguage, expires, websiteRedirectLocation *string
	if err := row.Scan(&storageClass, &cacheControl, &contentDisposition, &contentEncoding, &contentLanguage, &expires, &websiteRedirectLocation); err != nil {
		if err == sql.ErrNoRows {
			return &storageoutboxentry.PutOptions{}, nil
		}
		return nil, err
	}
	tags, err := queryStorageOutboxEntryKeyValues(ctx, tx, findStorageOutboxEntryTagsStmt, id, outboxId)
	if err != nil {
		return nil, err
	}
	userMetadata, err := queryStorageOutboxEntryKeyValues(ctx, tx, findStorageOutboxEntryUserMetadataStmt, id, outboxId)
	if err != nil {
		return nil, err
	}
	putOptions := &storageoutboxentry.PutOptions{
		StorageClass: storageClass,
		Tags:         tags,
	}
	if cacheControl != nil || contentDisposition != nil || contentEncoding != nil || contentLanguage != nil || expires != nil || websiteRedirectLocation != nil || len(userMetadata) > 0 {
		putOptions.Metadata = &storage.ObjectMetadata{
			CacheControl:            cacheControl,
			ContentDisposition:      contentDisposition,
			ContentEncoding:         contentEncoding,
			ContentLanguage:         contentLanguage,
			Expires:                 expires,
			WebsiteRedirectLocation: websiteRedirectLocation,
			UserMetadata:            userMetadata,
		}
	}
	return putOptions, nil
}

func (sor *sqliteRepository) ClaimFirstStorageOutboxEntry(ctx context.Context, tx *sql.Tx, outboxId string, owner string, now time.Time, claimUntil time.Time) (*storageoutboxentry.Entity, bool, error) {
	entry, err := sor.FindFirstStorageOutboxEntry(ctx, tx, outboxId)
	if err != nil || entry == nil {
		return entry, false, err
	}
	result, err := tx.ExecContext(ctx, claimStorageOutboxEntryStmt, owner, claimUntil, now, entry.Id.String(), outboxId, entry.Version, now)
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

func (sor *sqliteRepository) DeleteStorageOutboxEntryByClaimOwner(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string) (bool, error) {
	result, err := tx.ExecContext(ctx, deleteStorageOutboxEntryByClaimOwnerStmt, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (sor *sqliteRepository) ReleaseStorageOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, releaseStorageOutboxEntryClaimStmt, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (sor *sqliteRepository) ExtendStorageOutboxEntryClaim(ctx context.Context, tx *sql.Tx, outboxId string, id ulid.ULID, owner string, now time.Time, claimUntil time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, extendStorageOutboxEntryClaimStmt, claimUntil, now, id.String(), outboxId, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}
