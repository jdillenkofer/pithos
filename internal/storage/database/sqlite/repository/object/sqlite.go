package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	insertObjectStmt                                                                             = "INSERT INTO objects (id, bucket_name, key, content_type, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, upload_status, upload_id, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)"
	updateObjectByIdStmt                                                                         = "UPDATE objects SET bucket_name = $1, key = $2, content_type = $3, etag = $4, checksum_crc32 = $5, checksum_crc32c = $6, checksum_crc64nvme = $7, checksum_sha1 = $8, checksum_sha256 = $9, checksum_type = $10, size = $11, upload_status = $12, upload_id = $13, updated_at = $14 WHERE id = $15"
	containsBucketObjectsByBucketNameStmt                                                        = "SELECT id FROM objects WHERE bucket_name = $1"
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt                               = "SELECT id, bucket_name, key, content_type, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND key > $3 AND upload_status = $4 ORDER BY key ASC"
	findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscStmt = "SELECT id, bucket_name, key, content_type, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND key > $3 AND upload_id > $4 AND upload_status = $5 ORDER BY key ASC, upload_id ASC"
	findObjectByBucketNameAndKeyAndUploadIdStmt                                                  = "SELECT id, bucket_name, key, content_type, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = $1 AND key = $2 AND upload_id = $3 AND upload_status = $4"
	findObjectByBucketNameAndKeyStmt                                                             = "SELECT id, bucket_name, key, content_type, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = $1 AND key = $2 AND upload_status = $3"
	countObjectsByBucketNameAndPrefixAndStartAfterStmt                                           = "SELECT COUNT(*) FROM objects WHERE bucket_name = $1 and key LIKE $2 || '%' AND key > $3 AND upload_status = $4"
	countObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerStmt                           = "SELECT COUNT(*) FROM objects WHERE bucket_name = $1 and key LIKE $2 || '%' AND key > $3 AND upload_id > $4 AND upload_status = $5"
	deleteObjectByIdStmt                                                                         = "DELETE FROM objects WHERE id = $1"
)

func NewRepository() (object.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToObjectEntity(objectRows *sql.Rows) (*object.Entity, error) {
	var id string
	var bucketName string
	var key string
	var contentType *string
	var etag string
	var checksumCRC32 *string
	var checksumCRC32C *string
	var checksumCRC64NVME *string
	var checksumSHA1 *string
	var checksumSHA256 *string
	var checksumType *string
	var size int64
	var uploadStatus string
	var uploadId *string
	var createdAt time.Time
	var updatedAt time.Time
	err := objectRows.Scan(&id, &bucketName, &key, &contentType, &etag, &checksumCRC32, &checksumCRC32C, &checksumCRC64NVME, &checksumSHA1, &checksumSHA256, &checksumType, &size, &uploadStatus, &uploadId, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	objectEntity := object.Entity{
		Id:                &ulidId,
		BucketName:        bucketName,
		Key:               key,
		ContentType:       contentType,
		ETag:              etag,
		ChecksumCRC32:     checksumCRC32,
		ChecksumCRC32C:    checksumCRC32C,
		ChecksumCRC64NVME: checksumCRC64NVME,
		ChecksumSHA1:      checksumSHA1,
		ChecksumSHA256:    checksumSHA256,
		ChecksumType:      checksumType,
		Size:              size,
		UploadStatus:      uploadStatus,
		UploadId:          uploadId,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}
	return &objectEntity, nil
}

func (or *sqliteRepository) SaveObject(ctx context.Context, tx *sql.Tx, object *object.Entity) error {
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
		object.CreatedAt = time.Now().UTC()
		object.UpdatedAt = object.CreatedAt
		_, err := tx.ExecContext(ctx, insertObjectStmt, object.Id.String(), object.BucketName, object.Key, object.ContentType, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.UploadStatus, object.UploadId, object.CreatedAt, object.UpdatedAt)
		return err
	}
	object.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateObjectByIdStmt, object.BucketName, object.Key, object.ContentType, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.UploadStatus, object.UploadId, object.UpdatedAt, object.Id.String())
	return err
}

func (or *sqliteRepository) ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error) {
	objectRows, err := tx.QueryContext(ctx, containsBucketObjectsByBucketNameStmt, bucketName)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	var containsObjects = objectRows.Next()
	return &containsObjects, nil
}

func (or *sqliteRepository) FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt, bucketName, prefix, startAfter, object.UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	objects := []object.Entity{}
	for objectRows.Next() {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, *objectEntity)
	}
	return objects, nil
}

func (or *sqliteRepository) FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, keyMarker string, uploadIdMarker string) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscStmt, bucketName, prefix, keyMarker, uploadIdMarker, object.UploadStatusPending)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	objects := []object.Entity{}
	for objectRows.Next() {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, *objectEntity)
	}
	return objects, nil
}

func (or *sqliteRepository) FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectByBucketNameAndKeyAndUploadIdStmt, bucketName, key, uploadId, object.UploadStatusPending)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	exists := objectRows.Next()
	if exists {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		return objectEntity, nil
	}
	return nil, nil
}

func (or *sqliteRepository) FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectByBucketNameAndKeyStmt, bucketName, key, object.UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	exists := objectRows.Next()
	if exists {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		return objectEntity, nil
	}
	return nil, nil
}

func (or *sqliteRepository) CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) (*int, error) {
	keyCountRow := tx.QueryRowContext(ctx, countObjectsByBucketNameAndPrefixAndStartAfterStmt, bucketName, prefix, startAfter, object.UploadStatusCompleted)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *sqliteRepository) CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, keyMarker string, uploadIdMarker string) (*int, error) {
	keyCountRow := tx.QueryRowContext(ctx, countObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerStmt, bucketName, prefix, keyMarker, uploadIdMarker, object.UploadStatusPending)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *sqliteRepository) DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteObjectByIdStmt, objectId.String())
	return err
}
