package partdedupindex

import (
	"context"
	"database/sql"
	"time"

	index "github.com/jdillenkofer/pithos/internal/storage/database/repository/partdedupindex"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type repository struct{}

const (
	findPartDedupIndexEntryStmt      = "SELECT part_store_name, checksum_sha256, size, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, part_id, created_at, updated_at FROM part_dedup_index WHERE part_store_name = $1 AND checksum_sha256 = $2 AND size = $3"
	insertPartDedupIndexStmt         = "INSERT INTO part_dedup_index (part_store_name, checksum_sha256, size, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, part_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING"
	deletePartDedupIndexByPartIdStmt = "DELETE FROM part_dedup_index WHERE part_id = $1"
	findAllPartDedupIndexPartIdsStmt = "SELECT part_id FROM part_dedup_index"
	backfillPartDedupIndexStmt       = "INSERT INTO part_dedup_index (part_store_name, checksum_sha256, size, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, part_id, created_at, updated_at) SELECT COALESCE(part_store_name, ''), checksum_sha256, size, MIN(etag), MIN(checksum_crc32), MIN(checksum_crc32c), MIN(checksum_crc64nvme), MIN(checksum_sha1), MIN(part_id), $1::timestamp, $2::timestamp FROM parts WHERE checksum_sha256 IS NOT NULL AND checksum_crc32 IS NOT NULL AND checksum_crc32c IS NOT NULL AND checksum_crc64nvme IS NOT NULL AND checksum_sha1 IS NOT NULL GROUP BY COALESCE(part_store_name, ''), checksum_sha256, size ON CONFLICT DO NOTHING"
)

func NewRepository() (index.Repository, error) { return &repository{}, nil }

func (r *repository) FindEntry(ctx context.Context, tx *sql.Tx, store, sha256 string, size int64) (*index.Entity, error) {
	row := tx.QueryRowContext(ctx, findPartDedupIndexEntryStmt, store, sha256, size)
	var entity index.Entity
	var id string
	if err := row.Scan(&entity.PartStoreName, &entity.ChecksumSHA256, &entity.Size, &entity.ETag, &entity.ChecksumCRC32, &entity.ChecksumCRC32C, &entity.ChecksumCRC64NVME, &entity.ChecksumSHA1, &id, &entity.CreatedAt, &entity.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	entity.PartId = *partstore.MustNewPartIdFromString(id)
	return &entity, nil
}

func (r *repository) TryInsert(ctx context.Context, tx *sql.Tx, entity *index.Entity) (bool, error) {
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, insertPartDedupIndexStmt, entity.PartStoreName, entity.ChecksumSHA256, entity.Size, entity.ETag, entity.ChecksumCRC32, entity.ChecksumCRC32C, entity.ChecksumCRC64NVME, entity.ChecksumSHA1, entity.PartId.String(), now, now)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

func (r *repository) BackfillFromParts(ctx context.Context, tx *sql.Tx) (int64, error) {
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, backfillPartDedupIndexStmt, now, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *repository) DeleteByPartIds(ctx context.Context, tx *sql.Tx, ids []partstore.PartId) error {
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, deletePartDedupIndexByPartIdStmt, id.String()); err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) FindAllPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	rows, err := tx.QueryContext(ctx, findAllPartDedupIndexPartIdsStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []partstore.PartId{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, *partstore.MustNewPartIdFromString(id))
	}
	return ids, rows.Err()
}
