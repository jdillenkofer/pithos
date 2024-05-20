package repository

import (
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BucketEntity struct {
	Id        *ulid.ULID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*BucketEntity, error) {
	var id string
	var name string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := BucketEntity{
		Id:        &ulidId,
		Name:      name,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return &bucketEntity, nil
}

func FindAllBuckets(tx *sql.Tx) ([]BucketEntity, error) {
	bucketRows, err := tx.Query("SELECT id, name, created_at, updated_at FROM buckets")
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	buckets := []BucketEntity{}
	for bucketRows.Next() {
		bucketEntity, err := convertRowToBucketEntity(bucketRows)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, *bucketEntity)
	}
	return buckets, nil
}

func FindBucketByName(tx *sql.Tx, bucketName string) (*BucketEntity, error) {
	bucketRows, err := tx.Query("SELECT id, name, created_at, updated_at FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	if !bucketRows.Next() {
		return nil, nil
	}
	bucketEntity, err := convertRowToBucketEntity(bucketRows)
	if err != nil {
		return nil, err
	}
	return bucketEntity, nil
}

func SaveBucket(tx *sql.Tx, bucket *BucketEntity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.Exec("INSERT INTO buckets (id, name, created_at, updated_at) VALUES(?, ?, ?, ?)", bucket.Id.String(), bucket.Name, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE buckets SET name = ?, updated_at = ? WHERE id = ?", bucket.Name, bucket.UpdatedAt, bucket.Id.String())
	return err
}

func ExistsBucketByName(tx *sql.Tx, bucketName string) (*bool, error) {
	bucketRows, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	var exists = bucketRows.Next()
	return &exists, nil
}

func DeleteBucketByName(tx *sql.Tx, bucketName string) error {
	_, err := tx.Exec("DELETE FROM buckets WHERE name = ?", bucketName)
	return err
}

const UploadStatusPending = "PENDING"
const UploadStatusCompleted = "COMPLETED"
const UploadStatusAborted = "ABORTED"

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

func SaveObject(tx *sql.Tx, object *ObjectEntity) error {
	if object.Id == nil {
		id := ulid.Make()
		object.Id = &id
		object.CreatedAt = time.Now()
		object.UpdatedAt = object.CreatedAt
		_, err := tx.Exec("INSERT INTO objects (id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", object.Id.String(), object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.CreatedAt, object.UpdatedAt)
		return err
	}
	object.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE objects SET bucket_name = ?, key = ?, etag = ?, size = ?, upload_status = ?, upload_id = ?, updated_at = ? WHERE id = ?", object.BucketName, object.Key, object.ETag, object.Size, object.UploadStatus, object.UploadId, object.UpdatedAt, object.Id.String())
	return err
}

func ContainsBucketObjectsByBucketName(tx *sql.Tx, bucketName string) (*bool, error) {
	objectRows, err := tx.Query("SELECT id FROM objects WHERE bucket_name = ?", bucketName)
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

func FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(tx *sql.Tx, bucketName string, prefix string, startAfter string) ([]ObjectEntity, error) {
	objectRows, err := tx.Query("SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key LIKE ? || '%' AND key > ? AND upload_status = ? ORDER BY key ASC", bucketName, prefix, startAfter, UploadStatusCompleted)
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

func FindObjectByBucketNameAndKey(tx *sql.Tx, bucketName string, key string) (*ObjectEntity, error) {
	objectRows, err := tx.Query("SELECT id, bucket_name, key, etag, size, upload_status, upload_id, created_at, updated_at FROM objects WHERE bucket_name = ? AND key = ? AND upload_status = ?", bucketName, key, UploadStatusCompleted)
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

func CountObjectsByBucketNameAndPrefixAndStartAfter(tx *sql.Tx, bucketName string, prefix string, startAfter string) (*int, error) {
	keyCountRow := tx.QueryRow("SELECT COUNT(*) FROM objects WHERE bucket_name = ? and key LIKE ? || '%' AND key > ? AND upload_status = ?", bucketName, prefix, startAfter, UploadStatusCompleted)
	var keyCount int
	err := keyCountRow.Scan(&keyCount)
	if err != nil {
		return nil, err
	}
	return &keyCount, nil
}

func DeleteObjectById(tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.Exec("DELETE FROM objects WHERE id = ?", objectId.String())
	return err
}

type BlobEntity struct {
	Id             *ulid.ULID
	BlobId         ulid.ULID
	ObjectId       ulid.ULID
	ETag           string
	Size           int64
	SequenceNumber int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func convertRowToBlobEntity(blobRows *sql.Rows) (*BlobEntity, error) {
	var id string
	var blobId string
	var objectId string
	var etag string
	var size int64
	var sequenceNumber int
	var createdAt time.Time
	var updatedAt time.Time
	err := blobRows.Scan(&id, &blobId, &objectId, &etag, &size, &sequenceNumber, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	blobEntity := BlobEntity{
		Id:             &ulidId,
		BlobId:         ulid.MustParse(blobId),
		ObjectId:       ulid.MustParse(objectId),
		ETag:           etag,
		Size:           size,
		SequenceNumber: sequenceNumber,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}
	return &blobEntity, nil
}

func FindBlobsByObjectIdOrderBySequenceNumberAsc(tx *sql.Tx, objectId ulid.ULID) ([]BlobEntity, error) {
	blobRows, err := tx.Query("SELECT id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at FROM blobs WHERE object_id = ? ORDER BY sequence_number ASC", objectId.String())
	if err != nil {
		return nil, err
	}
	defer blobRows.Close()
	blobs := []BlobEntity{}
	for blobRows.Next() {
		blobEntity, err := convertRowToBlobEntity(blobRows)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, *blobEntity)
	}
	return blobs, nil
}

func SaveBlob(tx *sql.Tx, blob *BlobEntity) error {
	if blob.Id == nil {
		id := ulid.Make()
		blob.Id = &id
		blob.CreatedAt = time.Now()
		blob.UpdatedAt = blob.CreatedAt
		_, err := tx.Exec("INSERT INTO blobs (id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", blob.Id.String(), blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.CreatedAt, blob.UpdatedAt)
		return err
	}

	blob.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE blobs SET blob_id = ?, object_id = ?, etag = ?, size = ?, sequence_number = ?, updated_at = ? WHERE id = ?", blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.UpdatedAt, blob.Id.String())
	return err
}

func DeleteBlobByObjectId(tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.Exec("DELETE FROM blobs WHERE object_id = ?", objectId.String())
	return err
}

type BlobContentEntity struct {
	Id        *ulid.ULID
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToBlobContentEntity(blobContentRow *sql.Row) (*BlobContentEntity, error) {
	var id string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobContentRow.Scan(&id, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &BlobContentEntity{
		Id:        &ulidId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func FindBlobContentById(tx *sql.Tx, blobContentId ulid.ULID) (*BlobContentEntity, error) {
	row := tx.QueryRow("SELECT id, content, created_at, updated_at FROM blob_contents WHERE id = ?", blobContentId.String())
	blobContentEntity, err := convertRowToBlobContentEntity(row)
	if err != nil {
		return nil, err
	}
	return blobContentEntity, nil
}

func SaveBlobContent(tx *sql.Tx, blobContent *BlobContentEntity) error {
	if blobContent.Id == nil {
		id := ulid.Make()
		blobContent.Id = &id
		blobContent.CreatedAt = time.Now()
		blobContent.UpdatedAt = blobContent.CreatedAt
		_, err := tx.Exec("INSERT INTO blob_contents (id, content, created_at, updated_at) VALUES(?, ?, ?, ?)", blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
		return err
	}

	blobContent.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE blob_contents SET content = ?, updated_at = ? WHERE id = ?", blobContent.Content, blobContent.UpdatedAt, blobContent.Id.String())
	return err
}

func DeleteBlobContentById(tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.Exec("DELETE FROM blob_contents WHERE id = ?", id.String())
	return err
}
