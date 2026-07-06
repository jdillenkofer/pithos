package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	insertObjectStmt                                                                                      = "INSERT INTO objects (id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)"
	insertObjectIfAbsentStmt                                                                              = "INSERT INTO objects (id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27) ON CONFLICT DO NOTHING"
	updateObjectByIdStmt                                                                                  = "UPDATE objects SET bucket_name = $1, key = $2, content_type = $3, cache_control = $4, content_disposition = $5, content_encoding = $6, content_language = $7, expires = $8, website_redirect_location = $9, etag = $10, checksum_crc32 = $11, checksum_crc32c = $12, checksum_crc64nvme = $13, checksum_sha1 = $14, checksum_sha256 = $15, checksum_type = $16, size = $17, version_id = $18, is_delete_marker = $19, is_latest = $20, upload_status = $21, upload_id = $22, storage_class = $23, optimistic_lock_version = optimistic_lock_version + 1, updated_at = $24 WHERE id = $25"
	updateObjectByIdAndOptimisticLockVersionStmt                                                          = "UPDATE objects SET bucket_name = $1, key = $2, content_type = $3, cache_control = $4, content_disposition = $5, content_encoding = $6, content_language = $7, expires = $8, website_redirect_location = $9, etag = $10, checksum_crc32 = $11, checksum_crc32c = $12, checksum_crc64nvme = $13, checksum_sha1 = $14, checksum_sha256 = $15, checksum_type = $16, size = $17, version_id = $18, is_delete_marker = $19, is_latest = $20, upload_status = $21, upload_id = $22, storage_class = $23, optimistic_lock_version = optimistic_lock_version + 1, updated_at = $24 WHERE id = $25 AND optimistic_lock_version = $26"
	containsBucketObjectsByBucketNameStmt                                                                 = "SELECT id FROM objects WHERE bucket_name = $1"
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt                                        = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND key > $3 AND upload_status = $4 AND is_latest = TRUE AND is_delete_marker = FALSE ORDER BY key ASC"
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimitStmt                               = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND key > $3 AND upload_status = $4 AND is_latest = TRUE AND is_delete_marker = FALSE ORDER BY key ASC LIMIT $5"
	findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscStmt          = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND (key > $3 OR ($4 <> '' AND key = $3 AND upload_id > $4)) AND upload_status = $5 ORDER BY key ASC, upload_id ASC"
	findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimitStmt = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND (key > $3 OR ($4 <> '' AND key = $3 AND upload_id > $4)) AND upload_status = $5 ORDER BY key ASC, upload_id ASC LIMIT $6"
	findObjectByBucketNameAndKeyAndUploadIdStmt                                                           = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key = $2 AND upload_id = $3 AND upload_status = $4"
	findObjectByBucketNameAndKeyStmt                                                                      = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key = $2 AND upload_status = $3 AND is_latest = TRUE"
	countObjectsByBucketNameAndPrefixAndStartAfterStmt                                                    = "SELECT COUNT(*) FROM objects WHERE bucket_name = $1 and key LIKE $2 || '%' AND key > $3 AND upload_status = $4 AND is_latest = TRUE AND is_delete_marker = FALSE"
	countObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerStmt                                    = "SELECT COUNT(*) FROM objects WHERE bucket_name = $1 and key LIKE $2 || '%' AND (key > $3 OR ($4 <> '' AND key = $3 AND upload_id > $4)) AND upload_status = $5"
	// Version ids are ULIDs, so their lexicographic order matches creation
	// order; the special 'null' version is mapped to '' so it sorts last
	// within a key instead of comparing greater than every ULID. The marker
	// filter must use the same expression as the ORDER BY for keyset
	// pagination to be consistent.
	findObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescStmt          = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND upload_status = $3 AND (key > $4 OR (key = $4 AND COALESCE(NULLIF(version_id, 'null'), '') < COALESCE(NULLIF($5, 'null'), ''))) ORDER BY key ASC, COALESCE(NULLIF(version_id, 'null'), '') DESC"
	findObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimitStmt = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key LIKE $2 || '%' AND upload_status = $3 AND (key > $4 OR (key = $4 AND COALESCE(NULLIF(version_id, 'null'), '') < COALESCE(NULLIF($5, 'null'), ''))) ORDER BY key ASC, COALESCE(NULLIF(version_id, 'null'), '') DESC LIMIT $6"
	findObjectByBucketNameAndKeyAndVersionIDStmt                                                                    = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key = $2 AND version_id = $3 AND upload_status = $4"
	findNullObjectVersionByBucketNameAndKeyStmt                                                                     = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key = $2 AND version_id = 'null' AND upload_status = $3"
	findLatestObjectByBucketNameAndKeyExcludingIDStmt                                                               = "SELECT id, bucket_name, key, content_type, cache_control, content_disposition, content_encoding, content_language, expires, website_redirect_location, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type, size, version_id, is_delete_marker, is_latest, upload_status, upload_id, optimistic_lock_version, created_at, updated_at, storage_class FROM objects WHERE bucket_name = $1 AND key = $2 AND upload_status = $3 AND id != $4 ORDER BY created_at DESC LIMIT 1"
	clearLatestObjectByBucketNameAndKeyStmt                                                                         = "UPDATE objects SET is_latest = FALSE, updated_at = $3 WHERE bucket_name = $1 AND key = $2 AND upload_status = $4 AND is_latest = TRUE"
	deleteObjectByIdStmt                                                                                            = "DELETE FROM objects WHERE id = $1"
	deleteObjectByIdAndOptimisticLockVersionStmt                                                                    = "DELETE FROM objects WHERE id = $1 AND optimistic_lock_version = $2"
)

func NewRepository() (object.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToObjectEntity(objectRows *sql.Rows) (*object.Entity, error) {
	var id string
	var bucketName string
	var key string
	var contentType *string
	var cacheControl *string
	var contentDisposition *string
	var contentEncoding *string
	var contentLanguage *string
	var expires *string
	var websiteRedirectLocation *string
	var etag string
	var checksumCRC32 *string
	var checksumCRC32C *string
	var checksumCRC64NVME *string
	var checksumSHA1 *string
	var checksumSHA256 *string
	var checksumType *string
	var size int64
	var versionID *string
	var isDeleteMarker bool
	var isLatest bool
	var uploadStatus string
	var uploadId *string
	var optimisticLockVersion int64
	var createdAt time.Time
	var updatedAt time.Time
	var storageClass *string
	err := objectRows.Scan(&id, &bucketName, &key, &contentType, &cacheControl, &contentDisposition, &contentEncoding, &contentLanguage, &expires, &websiteRedirectLocation, &etag, &checksumCRC32, &checksumCRC32C, &checksumCRC64NVME, &checksumSHA1, &checksumSHA256, &checksumType, &size, &versionID, &isDeleteMarker, &isLatest, &uploadStatus, &uploadId, &optimisticLockVersion, &createdAt, &updatedAt, &storageClass)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	objectEntity := object.Entity{
		Id:                      &ulidId,
		BucketName:              storage.MustNewBucketName(bucketName),
		Key:                     storage.MustNewObjectKey(key),
		ContentType:             contentType,
		CacheControl:            cacheControl,
		ContentDisposition:      contentDisposition,
		ContentEncoding:         contentEncoding,
		ContentLanguage:         contentLanguage,
		Expires:                 expires,
		WebsiteRedirectLocation: websiteRedirectLocation,
		ETag:                    etag,
		ChecksumCRC32:           checksumCRC32,
		ChecksumCRC32C:          checksumCRC32C,
		ChecksumCRC64NVME:       checksumCRC64NVME,
		ChecksumSHA1:            checksumSHA1,
		ChecksumSHA256:          checksumSHA256,
		ChecksumType:            checksumType,
		Size:                    size,
		StorageClass:            storageClass,
		VersionID:               versionID,
		IsDeleteMarker:          isDeleteMarker,
		IsLatest:                isLatest,
		UploadStatus:            uploadStatus,
		UploadId:                ptrutils.MapPtr(uploadId, storage.MustNewUploadId),
		OptimisticLockVersion:   optimisticLockVersion,
		CreatedAt:               createdAt,
		UpdatedAt:               updatedAt,
	}
	return &objectEntity, nil
}

func (or *pgxRepository) SaveObject(ctx context.Context, tx *sql.Tx, object *object.Entity) error {
	mapUploadIdToString := func(uploadId storage.UploadId) string {
		return uploadId.String()
	}
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
		object.CreatedAt = time.Now().UTC()
		object.UpdatedAt = object.CreatedAt
		if object.OptimisticLockVersion == 0 {
			object.OptimisticLockVersion = 1
		}
		_, err := tx.ExecContext(ctx, insertObjectStmt, object.Id.String(), object.BucketName.String(), object.Key.String(), object.ContentType, object.CacheControl, object.ContentDisposition, object.ContentEncoding, object.ContentLanguage, object.Expires, object.WebsiteRedirectLocation, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.VersionID, object.IsDeleteMarker, object.IsLatest, object.UploadStatus, ptrutils.MapPtr(object.UploadId, mapUploadIdToString), object.OptimisticLockVersion, object.CreatedAt, object.UpdatedAt, object.StorageClass)
		return err
	}
	object.UpdatedAt = time.Now().UTC()
	res, err := tx.ExecContext(ctx, updateObjectByIdStmt, object.BucketName.String(), object.Key.String(), object.ContentType, object.CacheControl, object.ContentDisposition, object.ContentEncoding, object.ContentLanguage, object.Expires, object.WebsiteRedirectLocation, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.VersionID, object.IsDeleteMarker, object.IsLatest, object.UploadStatus, ptrutils.MapPtr(object.UploadId, mapUploadIdToString), object.StorageClass, object.UpdatedAt, object.Id.String())
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected > 0 {
		object.OptimisticLockVersion++
	}
	return err
}

func (or *pgxRepository) InsertObjectIfAbsent(ctx context.Context, tx *sql.Tx, object *object.Entity) (*bool, error) {
	mapUploadIdToString := func(uploadId storage.UploadId) string {
		return uploadId.String()
	}
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
	}
	if object.CreatedAt.IsZero() {
		object.CreatedAt = time.Now().UTC()
	}
	if object.OptimisticLockVersion == 0 {
		object.OptimisticLockVersion = 1
	}
	object.UpdatedAt = object.CreatedAt

	res, err := tx.ExecContext(ctx, insertObjectIfAbsentStmt, object.Id.String(), object.BucketName.String(), object.Key.String(), object.ContentType, object.CacheControl, object.ContentDisposition, object.ContentEncoding, object.ContentLanguage, object.Expires, object.WebsiteRedirectLocation, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.VersionID, object.IsDeleteMarker, object.IsLatest, object.UploadStatus, ptrutils.MapPtr(object.UploadId, mapUploadIdToString), object.OptimisticLockVersion, object.CreatedAt, object.UpdatedAt, object.StorageClass)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	inserted := rowsAffected > 0
	return &inserted, nil
}

func (or *pgxRepository) UpdateObjectByIdAndOptimisticLockVersion(ctx context.Context, tx *sql.Tx, object *object.Entity, optimisticLockVersion int64) (*bool, error) {
	mapUploadIdToString := func(uploadId storage.UploadId) string {
		return uploadId.String()
	}
	object.UpdatedAt = time.Now().UTC()
	res, err := tx.ExecContext(ctx, updateObjectByIdAndOptimisticLockVersionStmt, object.BucketName.String(), object.Key.String(), object.ContentType, object.CacheControl, object.ContentDisposition, object.ContentEncoding, object.ContentLanguage, object.Expires, object.WebsiteRedirectLocation, object.ETag, object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME, object.ChecksumSHA1, object.ChecksumSHA256, object.ChecksumType, object.Size, object.VersionID, object.IsDeleteMarker, object.IsLatest, object.UploadStatus, ptrutils.MapPtr(object.UploadId, mapUploadIdToString), object.StorageClass, object.UpdatedAt, object.Id.String(), optimisticLockVersion)
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	updated := rowsAffected > 0
	if updated {
		object.OptimisticLockVersion = optimisticLockVersion + 1
	}
	return &updated, nil
}

func (or *pgxRepository) ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bool, error) {
	objectRows, err := tx.QueryContext(ctx, containsBucketObjectsByBucketNameStmt, bucketName.String())
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	var containsObjects = objectRows.Next()
	return &containsObjects, nil
}

func (or *pgxRepository) FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, startAfter string) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt, bucketName.String(), prefix, startAfter, object.UploadStatusCompleted)
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

func (or *pgxRepository) FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimit(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, startAfter string, limit int32) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimitStmt, bucketName.String(), prefix, startAfter, object.UploadStatusCompleted, limit)
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

func (or *pgxRepository) FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, uploadIdMarker string) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscStmt, bucketName.String(), prefix, keyMarker, uploadIdMarker, object.UploadStatusPending)
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

func (or *pgxRepository) FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimit(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, uploadIdMarker string, limit int32) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimitStmt, bucketName.String(), prefix, keyMarker, uploadIdMarker, object.UploadStatusPending, limit)
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

func (or *pgxRepository) FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectByBucketNameAndKeyAndUploadIdStmt, bucketName.String(), key.String(), uploadId.String(), object.UploadStatusPending)
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

func (or *pgxRepository) FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectByBucketNameAndKeyStmt, bucketName.String(), key.String(), object.UploadStatusCompleted)
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

func (or *pgxRepository) CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, startAfter string) (*int, error) {
	keyCountRow := tx.QueryRowContext(ctx, countObjectsByBucketNameAndPrefixAndStartAfterStmt, bucketName.String(), prefix, startAfter, object.UploadStatusCompleted)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *pgxRepository) CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, uploadIdMarker string) (*int, error) {
	keyCountRow := tx.QueryRowContext(ctx, countObjectsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerStmt, bucketName.String(), prefix, keyMarker, uploadIdMarker, object.UploadStatusPending)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *pgxRepository) DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) (*bool, error) {
	res, err := tx.ExecContext(ctx, deleteObjectByIdStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	deleted := rowsAffected > 0
	return &deleted, nil
}

func (or *pgxRepository) DeleteObjectByIdAndOptimisticLockVersion(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, optimisticLockVersion int64) (*bool, error) {
	res, err := tx.ExecContext(ctx, deleteObjectByIdAndOptimisticLockVersionStmt, objectId.String(), optimisticLockVersion)
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	deleted := rowsAffected > 0
	return &deleted, nil
}

func (or *pgxRepository) FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDesc(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, versionIDMarker string) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescStmt, bucketName.String(), prefix, object.UploadStatusCompleted, keyMarker, versionIDMarker)
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

func (or *pgxRepository) FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimit(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, versionIDMarker string, limit int32) ([]object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimitStmt, bucketName.String(), prefix, object.UploadStatusCompleted, keyMarker, versionIDMarker, limit)
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

func (or *pgxRepository) FindObjectByBucketNameAndKeyAndVersionID(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey, versionID string) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findObjectByBucketNameAndKeyAndVersionIDStmt, bucketName.String(), key.String(), versionID, object.UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	if objectRows.Next() {
		return convertRowToObjectEntity(objectRows)
	}
	return nil, nil
}

func (or *pgxRepository) FindNullObjectVersionByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findNullObjectVersionByBucketNameAndKeyStmt, bucketName.String(), key.String(), object.UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	if objectRows.Next() {
		return convertRowToObjectEntity(objectRows)
	}
	return nil, nil
}

func (or *pgxRepository) FindLatestObjectByBucketNameAndKeyExcludingID(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey, excludedObjectID ulid.ULID) (*object.Entity, error) {
	objectRows, err := tx.QueryContext(ctx, findLatestObjectByBucketNameAndKeyExcludingIDStmt, bucketName.String(), key.String(), object.UploadStatusCompleted, excludedObjectID.String())
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	if objectRows.Next() {
		return convertRowToObjectEntity(objectRows)
	}
	return nil, nil
}

func (or *pgxRepository) ClearLatestObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey) error {
	_, err := tx.ExecContext(ctx, clearLatestObjectByBucketNameAndKeyStmt, bucketName.String(), key.String(), time.Now().UTC(), object.UploadStatusCompleted)
	return err
}
