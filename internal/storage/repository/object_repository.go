package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

const UploadStatusPending = "PENDING"
const UploadStatusCompleted = "COMPLETED"

type ObjectRepository struct {
	db *sql.DB
}

func NewObjectRepository(db *sql.DB) ObjectRepository {
	return ObjectRepository{
		db: db,
	}
}

type ObjectEntity struct {
	Id           *ulid.ULID
	BucketName   string
	Key          string
	ETag         string
	Size         int64
	UploadStatus string
	UploadId     string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (or *ObjectRepository) SaveObject(ctx context.Context, tx *sql.Tx, object *ObjectEntity) error {
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
		object.CreatedAt = time.Now()
		object.UpdatedAt = object.CreatedAt
		_, err := tx.ExecContext(ctx, "INSERT INTO objects (id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", object.Id.String(), object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.CreatedAt, object.UpdatedAt)
		return err
	}
	object.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, "UPDATE objects SET bucket_name = ?, key = ?, etag = ?, size = ?, upload_status = ?, upload_id = ?, updated_at = ? WHERE id = ?", object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.UpdatedAt, object.Id.String())
	return err
}

func (or *ObjectRepository) ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error) {
	objectRows, err := tx.QueryContext(ctx, "SELECT id FROM objects WHERE bucket_name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	var containsObjects = objectRows.Next()
	return &containsObjects, nil
}

func convertRowToObjectEntity(objectRows *sql.Rows) (*ObjectEntity, error) {
	var id string
	var bucketName string
	var key string
	var etag string
	var size int64
	var upload_status string
	var upload_id string
	var createdAt time.Time
	var updatedAt time.Time
	err := objectRows.Scan(&id, &bucketName, &key, &etag, &size, &upload_status, &upload_id, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	objectEntity := ObjectEntity{
		Id:           &ulidId,
		BucketName:   bucketName,
		Key:          key,
		ETag:         etag,
		Size:         size,
		UploadStatus: upload_status,
		UploadId:     upload_id,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
	return &objectEntity, nil
}

func (or *ObjectRepository) FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) ([]ObjectEntity, error) {
	objectRows, err := tx.QueryContext(ctx, "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key LIKE ? || '%' AND key > ? AND upload_status = ? ORDER BY key ASC", bucketName, prefix, startAfter, UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	objects := []ObjectEntity{}
	for objectRows.Next() {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, *objectEntity)
	}
	return objects, nil
}

func (or *ObjectRepository) FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*ObjectEntity, error) {
	objectRows, err := tx.QueryContext(ctx, "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key = ? AND upload_id = ? AND upload_status = ?", bucketName, key, uploadId, UploadStatusPending)
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

func (or *ObjectRepository) FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*ObjectEntity, error) {
	objectRows, err := tx.QueryContext(ctx, "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key = ? AND upload_status = ?", bucketName, key, UploadStatusCompleted)
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

func (or *ObjectRepository) CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) (*int, error) {
	keyCountRow := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects WHERE bucket_name = ? and key LIKE ? || '%' AND key > ? AND upload_status = ?", bucketName, prefix, startAfter, UploadStatusCompleted)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *ObjectRepository) DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM objects WHERE id = ?", objectId.String())
	return err
}
