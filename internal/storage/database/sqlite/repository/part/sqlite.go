package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	findInUsePartIdsStmt                            = "SELECT part_id FROM parts"
	findPartsByObjectIdOrderBySequenceNumberAscStmt = "SELECT id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, created_at, updated_at FROM parts WHERE object_id = $1 ORDER BY sequence_number ASC"
	insertPartStmt                                  = "INSERT INTO parts (id, part_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
	updatePartByIdStmt                              = "UPDATE parts SET part_id = $1, object_id = $2, etag = $3, checksum_crc32 = $4, checksum_crc32c = $5, checksum_crc64nvme = $6, checksum_sha1 = $7, checksum_sha256 = $8, size = $9, sequence_number = $10, updated_at = $11 WHERE id = $12"
	deletePartByObjectIdStmt                        = "DELETE FROM parts WHERE object_id = $1"
)

func NewRepository() (part.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToPartEntity(partRows *sql.Rows) (*part.Entity, error) {
	var id string
	var partIdStr string
	var objectId string
	var etag string
	var checksumCRC32 *string
	var checksumCRC32C *string
	var checksumCRC64NVME *string
	var checksumSHA1 *string
	var checksumSHA256 *string
	var size int64
	var sequenceNumber int
	var createdAt time.Time
	var updatedAt time.Time
	err := partRows.Scan(&id, &partIdStr, &objectId, &etag, &checksumCRC32, &checksumCRC32C, &checksumCRC64NVME, &checksumSHA1, &checksumSHA256, &size, &sequenceNumber, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	partId := partstore.MustNewPartIdFromString(partIdStr)
	partEntity := part.Entity{
		Id:                &ulidId,
		PartId:            *partId,
		ObjectId:          ulid.MustParse(objectId),
		ETag:              etag,
		ChecksumCRC32:     checksumCRC32,
		ChecksumCRC32C:    checksumCRC32C,
		ChecksumCRC64NVME: checksumCRC64NVME,
		ChecksumSHA1:      checksumSHA1,
		ChecksumSHA256:    checksumSHA256,
		Size:              size,
		SequenceNumber:    sequenceNumber,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}
	return &partEntity, nil
}

func (br *sqliteRepository) FindInUsePartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
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

func (br *sqliteRepository) FindPartsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]part.Entity, error) {
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

func (br *sqliteRepository) SavePart(ctx context.Context, tx *sql.Tx, part *part.Entity) error {
	if part.Id == nil {
		id := ulid.Make()
		part.Id = &id
		part.CreatedAt = time.Now().UTC()
		part.UpdatedAt = part.CreatedAt
		_, err := tx.ExecContext(ctx, insertPartStmt, part.Id.String(), part.PartId.String(), part.ObjectId.String(), part.ETag, part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME, part.ChecksumSHA1, part.ChecksumSHA256, part.Size, part.SequenceNumber, part.CreatedAt, part.UpdatedAt)
		return err
	}

	part.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartByIdStmt, part.PartId.String(), part.ObjectId.String(), part.ETag, part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME, part.ChecksumSHA1, part.ChecksumSHA256, part.Size, part.SequenceNumber, part.UpdatedAt, part.Id.String())
	return err
}

func (br *sqliteRepository) DeletePartsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deletePartByObjectIdStmt, objectId.String())
	return err
}
