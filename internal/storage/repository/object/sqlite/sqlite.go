package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository/object"
	"github.com/oklog/ulid/v2"
)

type sqliteObjectRepository struct {
	db                                                                     *sql.DB
	insertObjectPreparedStmt                                               *sql.Stmt
	updateObjectByIdPreparedStmt                                           *sql.Stmt
	containsBucketObjectsByBucketNamePreparedStmt                          *sql.Stmt
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscPreparedStmt *sql.Stmt
	findObjectByBucketNameAndKeyAndUploadIdPreparedStmt                    *sql.Stmt
	findObjectByBucketNameAndKeyPreparedStmt                               *sql.Stmt
	countObjectsByBucketNameAndPrefixAndStartAfterPreparedStmt             *sql.Stmt
	deleteObjectByIdPreparedStmt                                           *sql.Stmt
}

const (
	insertObjectStmt                                               = "INSERT INTO objects (id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateObjectByIdStmt                                           = "UPDATE objects SET bucket_name = ?, key = ?, etag = ?, size = ?, upload_status = ?, upload_id = ?, updated_at = ? WHERE id = ?"
	containsBucketObjectsByBucketNameStmt                          = "SELECT id FROM objects WHERE bucket_name = ?"
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt = "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key LIKE ? || '%' AND key > ? AND upload_status = ? ORDER BY key ASC"
	findObjectByBucketNameAndKeyAndUploadIdStmt                    = "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key = ? AND upload_id = ? AND upload_status = ?"
	findObjectByBucketNameAndKeyStmt                               = "SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key = ? AND upload_status = ?"
	countObjectsByBucketNameAndPrefixAndStartAfterStmt             = "SELECT COUNT(*) FROM objects WHERE bucket_name = ? and key LIKE ? || '%' AND key > ? AND upload_status = ?"
	deleteObjectByIdStmt                                           = "DELETE FROM objects WHERE id = ?"
)

func New(db *sql.DB) (object.ObjectRepository, error) {
	insertObjectPreparedStmt, err := db.Prepare(insertObjectStmt)
	if err != nil {
		return nil, err
	}
	updateObjectByIdPreparedStmt, err := db.Prepare(updateObjectByIdStmt)
	if err != nil {
		return nil, err
	}
	containsBucketObjectsByBucketNamePreparedStmt, err := db.Prepare(containsBucketObjectsByBucketNameStmt)
	if err != nil {
		return nil, err
	}
	findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscPreparedStmt, err := db.Prepare(findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscStmt)
	if err != nil {
		return nil, err
	}
	findObjectByBucketNameAndKeyAndUploadIdPreparedStmt, err := db.Prepare(findObjectByBucketNameAndKeyAndUploadIdStmt)
	if err != nil {
		return nil, err
	}
	findObjectByBucketNameAndKeyPreparedStmt, err := db.Prepare(findObjectByBucketNameAndKeyStmt)
	if err != nil {
		return nil, err
	}
	countObjectsByBucketNameAndPrefixAndStartAfterPreparedStmt, err := db.Prepare(countObjectsByBucketNameAndPrefixAndStartAfterStmt)
	if err != nil {
		return nil, err
	}
	deleteObjectByIdPreparedStmt, err := db.Prepare(deleteObjectByIdStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteObjectRepository{
		db:                           db,
		insertObjectPreparedStmt:     insertObjectPreparedStmt,
		updateObjectByIdPreparedStmt: updateObjectByIdPreparedStmt,
		containsBucketObjectsByBucketNamePreparedStmt:                          containsBucketObjectsByBucketNamePreparedStmt,
		findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscPreparedStmt: findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscPreparedStmt,
		findObjectByBucketNameAndKeyAndUploadIdPreparedStmt:                    findObjectByBucketNameAndKeyAndUploadIdPreparedStmt,
		findObjectByBucketNameAndKeyPreparedStmt:                               findObjectByBucketNameAndKeyPreparedStmt,
		countObjectsByBucketNameAndPrefixAndStartAfterPreparedStmt:             countObjectsByBucketNameAndPrefixAndStartAfterPreparedStmt,
		deleteObjectByIdPreparedStmt:                                           deleteObjectByIdPreparedStmt,
	}, nil
}

func convertRowToObjectEntity(objectRows *sql.Rows) (*object.ObjectEntity, error) {
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
	objectEntity := object.ObjectEntity{
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

func (or *sqliteObjectRepository) SaveObject(ctx context.Context, tx *sql.Tx, object *object.ObjectEntity) error {
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
		object.CreatedAt = time.Now()
		object.UpdatedAt = object.CreatedAt
		_, err := tx.StmtContext(ctx, or.insertObjectPreparedStmt).ExecContext(ctx, object.Id.String(), object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.CreatedAt, object.UpdatedAt)
		return err
	}
	object.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, or.updateObjectByIdPreparedStmt).ExecContext(ctx, object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.UpdatedAt, object.Id.String())
	return err
}

func (or *sqliteObjectRepository) ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error) {
	objectRows, err := tx.StmtContext(ctx, or.containsBucketObjectsByBucketNamePreparedStmt).QueryContext(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	var containsObjects = objectRows.Next()
	return &containsObjects, nil
}

func (or *sqliteObjectRepository) FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) ([]object.ObjectEntity, error) {
	objectRows, err := tx.StmtContext(ctx, or.findObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscPreparedStmt).QueryContext(ctx, bucketName, prefix, startAfter, object.UploadStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer objectRows.Close()
	objects := []object.ObjectEntity{}
	for objectRows.Next() {
		objectEntity, err := convertRowToObjectEntity(objectRows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, *objectEntity)
	}
	return objects, nil
}

func (or *sqliteObjectRepository) FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*object.ObjectEntity, error) {
	objectRows, err := tx.StmtContext(ctx, or.findObjectByBucketNameAndKeyAndUploadIdPreparedStmt).QueryContext(ctx, bucketName, key, uploadId, object.UploadStatusPending)
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

func (or *sqliteObjectRepository) FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*object.ObjectEntity, error) {
	objectRows, err := tx.StmtContext(ctx, or.findObjectByBucketNameAndKeyPreparedStmt).QueryContext(ctx, bucketName, key, object.UploadStatusCompleted)
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

func (or *sqliteObjectRepository) CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) (*int, error) {
	keyCountRow := tx.StmtContext(ctx, or.countObjectsByBucketNameAndPrefixAndStartAfterPreparedStmt).QueryRowContext(ctx, bucketName, prefix, startAfter, object.UploadStatusCompleted)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func (or *sqliteObjectRepository) DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.StmtContext(ctx, or.deleteObjectByIdPreparedStmt).ExecContext(ctx, objectId.String())
	return err
}
