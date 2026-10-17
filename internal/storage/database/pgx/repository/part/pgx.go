package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findInUsePartIdsStmt                                = "SELECT part_id FROM parts"
	findInUsePartIdCountsStmt                           = "SELECT part_id, COUNT(*) FROM parts GROUP BY part_id"
	groupByStoreStmt                                    = "SELECT COALESCE(NULLIF(part_store_name, ''), 'default'), COUNT(*), COALESCE(SUM(size), 0) FROM parts GROUP BY COALESCE(NULLIF(part_store_name, ''), 'default')"
	findPartsByObjectIdOrderBySequenceNumberAscStmt     = "SELECT id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, part_store_name, created_at, updated_at FROM parts WHERE object_id = $1 ORDER BY sequence_number ASC"
	insertPartStmt                                      = "INSERT INTO parts (id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, part_store_name, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
	updatePartByIdStmt                                  = "UPDATE parts SET part_id = $1, object_id = $2, etag = $3, checksum_crc32 = $4, checksum_crc32c = $5, checksum_crc64nvme = $6, checksum_sha1 = $7, checksum_sha256 = $8, size = $9, sequence_number = $10, part_store_name = $11, updated_at = $12 WHERE id = $13"
	deletePartByObjectIdStmt                            = "DELETE FROM parts WHERE object_id = $1"
	deletePartsByObjectIdReturningStmt                  = "DELETE FROM parts WHERE object_id = $1 RETURNING id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, part_store_name, created_at, updated_at"
	deletePartsByObjectIdAndSequenceNumberReturningStmt = "DELETE FROM parts WHERE object_id = $1 AND sequence_number = $2 RETURNING id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, part_store_name, created_at, updated_at"
)

func (br *pgxRepository) GroupByStore(ctx context.Context, tx *sql.Tx) ([]part.StoreAggregate, error) {
	rows, err := tx.QueryContext(ctx, groupByStoreStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []part.StoreAggregate
	for rows.Next() {
		var a part.StoreAggregate
		if err := rows.Scan(&a.Store, &a.Count, &a.Size); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

func NewRepository() (part.Repository, error) {
	return &pgxRepository{}, nil
}

func (br *pgxRepository) FindInUsePartIdCounts(ctx context.Context, tx *sql.Tx) (map[partstore.PartId]int64, error) {
	rows, err := tx.QueryContext(ctx, findInUsePartIdCountsStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[partstore.PartId]int64{}
	for rows.Next() {
		var id string
		var count int64
		if err := rows.Scan(&id, &count); err != nil {
			return nil, err
		}
		counts[*partstore.MustNewPartIdFromString(id)] = count
	}
	return counts, rows.Err()
}

func convertRowToPartEntity(partRows *sql.Rows) (*part.Entity, error) {
	partEntity := &part.Entity{}
	var id string
	var partIdStr string
	var objectId string
	err := partRows.Scan(&id, &partIdStr, &objectId, &partEntity.ETag, &partEntity.ChecksumCRC32, &partEntity.ChecksumCRC32C, &partEntity.ChecksumCRC64NVME, &partEntity.ChecksumSHA1, &partEntity.ChecksumSHA256, &partEntity.Size, &partEntity.SequenceNumber, &partEntity.PartStoreName, &partEntity.CreatedAt, &partEntity.UpdatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partEntity.Id = &ulidId
	partEntity.PartId = *partstore.MustNewPartIdFromString(partIdStr)
	partEntity.ObjectId = ulid.MustParse(objectId)
	return partEntity, nil
}

func (br *pgxRepository) FindInUsePartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	partIdRows, err := tx.QueryContext(ctx, findInUsePartIdsStmt)
	if err != nil {
		return nil, err
	}
	defer partIdRows.Close()
	partIds := []partstore.PartId{}
	for partIdRows.Next() {
		var partIdStr string
		err := partIdRows.Scan(&partIdStr)
		if err != nil {
			return nil, err
		}
		partId := partstore.MustNewPartIdFromString(partIdStr)
		partIds = append(partIds, *partId)
	}
	return partIds, nil
}

func (br *pgxRepository) FindPartsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]part.Entity, error) {
	partRows, err := tx.QueryContext(ctx, findPartsByObjectIdOrderBySequenceNumberAscStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	defer partRows.Close()
	parts := []part.Entity{}
	for partRows.Next() {
		partEntity, err := convertRowToPartEntity(partRows)
		if err != nil {
			return nil, err
		}
		parts = append(parts, *partEntity)
	}
	return parts, nil
}

func (br *pgxRepository) SavePart(ctx context.Context, tx *sql.Tx, part *part.Entity) error {
	if part.Id == nil {
		id := ulid.Make()
		part.Id = &id
		part.CreatedAt = time.Now().UTC()
		part.UpdatedAt = part.CreatedAt
		_, err := tx.ExecContext(ctx, insertPartStmt, part.Id.String(), part.PartId.String(), part.ObjectId.String(), part.ETag, part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME, part.ChecksumSHA1, part.ChecksumSHA256, part.Size, part.SequenceNumber, part.PartStoreName, part.CreatedAt, part.UpdatedAt)
		return err
	}

	part.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartByIdStmt, part.PartId.String(), part.ObjectId.String(), part.ETag, part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME, part.ChecksumSHA1, part.ChecksumSHA256, part.Size, part.SequenceNumber, part.PartStoreName, part.UpdatedAt, part.Id.String())
	return err
}

func (br *pgxRepository) DeletePartsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deletePartByObjectIdStmt, objectId.String())
	return err
}

func collectDeletedParts(rows *sql.Rows) ([]part.Entity, error) {
	defer rows.Close()
	parts := []part.Entity{}
	for rows.Next() {
		entity, err := convertRowToPartEntity(rows)
		if err != nil {
			return nil, err
		}
		parts = append(parts, *entity)
	}
	return parts, rows.Err()
}

func (br *pgxRepository) DeletePartsByObjectIdReturning(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]part.Entity, error) {
	rows, err := tx.QueryContext(ctx, deletePartsByObjectIdReturningStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	return collectDeletedParts(rows)
}

func (br *pgxRepository) DeletePartsByObjectIdAndSequenceNumberReturning(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, sequenceNumber int) ([]part.Entity, error) {
	rows, err := tx.QueryContext(ctx, deletePartsByObjectIdAndSequenceNumberReturningStmt, objectId.String(), sequenceNumber)
	if err != nil {
		return nil, err
	}
	return collectDeletedParts(rows)
}
