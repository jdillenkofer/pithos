package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	findInUseBlobIdsStmt                            = "SELECT blob_id FROM blobs"
	findBlobsByObjectIdOrderBySequenceNumberAscStmt = "SELECT id, blob_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, created_at, updated_at FROM blobs WHERE object_id = $1 ORDER BY sequence_number ASC"
	insertBlobStmt                                  = "INSERT INTO blobs (id, blob_id, object_id, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, size, sequence_number, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
	updateBlobByIdStmt                              = "UPDATE blobs SET blob_id = $1, object_id = $2, etag = $3, checksum_crc32 = $4, checksum_crc32c = $5, checksum_crc64nvme = $6, checksum_sha1 = $7, checksum_sha256 = $8, size = $9, sequence_number = $10, updated_at = $11 WHERE id = $12"
	deleteBlobByObjectIdStmt                        = "DELETE FROM blobs WHERE object_id = $1"
)

func NewRepository() (blob.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToBlobEntity(blobRows *sql.Rows) (*blob.Entity, error) {
	var id string
	var blobIdStr string
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
	err := blobRows.Scan(&id, &blobIdStr, &objectId, &etag, &checksumCRC32, &checksumCRC32C, &checksumCRC64NVME, &checksumSHA1, &checksumSHA256, &size, &sequenceNumber, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	blobId, err := blobstore.NewBlobIdFromString(blobIdStr)
	if err != nil {
		return nil, err
	}
	blobEntity := blob.Entity{
		Id:                &ulidId,
		BlobId:            *blobId,
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
	return &blobEntity, nil
}

func (br *sqliteRepository) FindInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	blobIdRows, err := tx.QueryContext(ctx, findInUseBlobIdsStmt)
	if err != nil {
		return nil, err
	}
	defer blobIdRows.Close()
	blobIds := []blobstore.BlobId{}
	for blobIdRows.Next() {
		var blobIdStr string
		err := blobIdRows.Scan(&blobIdStr)
		if err != nil {
			return nil, err
		}
		blobId, err := blobstore.NewBlobIdFromString(blobIdStr)
		if err != nil {
			return nil, err
		}
		blobIds = append(blobIds, *blobId)
	}
	return blobIds, nil
}

func (br *sqliteRepository) FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]blob.Entity, error) {
	blobRows, err := tx.QueryContext(ctx, findBlobsByObjectIdOrderBySequenceNumberAscStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	defer blobRows.Close()
	blobs := []blob.Entity{}
	for blobRows.Next() {
		blobEntity, err := convertRowToBlobEntity(blobRows)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, *blobEntity)
	}
	return blobs, nil
}

func (br *sqliteRepository) SaveBlob(ctx context.Context, tx *sql.Tx, blob *blob.Entity) error {
	if blob.Id == nil {
		id := ulid.Make()
		blob.Id = &id
		blob.CreatedAt = time.Now().UTC()
		blob.UpdatedAt = blob.CreatedAt
		_, err := tx.ExecContext(ctx, insertBlobStmt, blob.Id.String(), blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.ChecksumCRC32, blob.ChecksumCRC32C, blob.ChecksumCRC64NVME, blob.ChecksumSHA1, blob.ChecksumSHA256, blob.Size, blob.SequenceNumber, blob.CreatedAt, blob.UpdatedAt)
		return err
	}

	blob.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBlobByIdStmt, blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.ChecksumCRC32, blob.ChecksumCRC32C, blob.ChecksumCRC64NVME, blob.ChecksumSHA1, blob.ChecksumSHA256, blob.Size, blob.SequenceNumber, blob.UpdatedAt, blob.Id.String())
	return err
}

func (br *sqliteRepository) DeleteBlobsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteBlobByObjectIdStmt, objectId.String())
	return err
}
