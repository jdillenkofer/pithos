package metadata

import (
	"database/sql"
	"slices"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage/repository"
	"github.com/oklog/ulid/v2"
)

type SqlMetadataStore struct {
	db *sql.DB
}

func NewSqlMetadataStore(db *sql.DB) (*SqlMetadataStore, error) {
	return &SqlMetadataStore{
		db: db,
	}, nil
}

func (sms *SqlMetadataStore) CreateBucket(bucketName string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}

	exists, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	if *exists {
		tx.Rollback()
		return ErrBucketAlreadyExists
	}

	err = repository.SaveBucket(tx, &repository.BucketEntity{
		Name: bucketName,
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

func (sms *SqlMetadataStore) DeleteBucket(bucketName string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}

	exists, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	if !*exists {
		tx.Rollback()
		return ErrNoSuchBucket
	}

	containsBucketObjects, err := repository.ContainsBucketObjectsByBucketName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	if *containsBucketObjects {
		tx.Rollback()
		return ErrBucketNotEmpty
	}

	err = repository.DeleteBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	return err
}

func (sms *SqlMetadataStore) ListBuckets() ([]Bucket, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, err
	}
	bucketEntities, err := repository.FindAllBuckets(tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	buckets := []Bucket{}
	for _, bucketEntity := range bucketEntities {
		buckets = append(buckets, Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		})
	}
	tx.Commit()
	return buckets, nil
}

func (sms *SqlMetadataStore) HeadBucket(bucketName string) (*Bucket, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, err
	}
	bucketEntity, err := repository.FindBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if bucketEntity == nil {
		tx.Rollback()
		return nil, ErrNoSuchBucket
	}

	bucket := Bucket{
		Name:         bucketEntity.Name,
		CreationDate: bucketEntity.CreatedAt,
	}
	tx.Commit()
	return &bucket, nil
}

func determineCommonPrefix(prefix, key, delimiter string) *string {
	prefixSegments := strings.Split(prefix, delimiter)
	keySegments := strings.Split(key, delimiter)
	if len(prefixSegments) >= len(keySegments) {
		return nil
	}
	commonPrefix := ""
	for idx := range prefixSegments {
		commonPrefix += keySegments[idx] + delimiter
	}
	return &commonPrefix
}

func (sms *SqlMetadataStore) listObjects(tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	keyCount, err := repository.CountObjectsByBucketNameAndPrefixAndStartAfter(tx, bucketName, prefix, startAfter)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	commonPrefixes := []string{}
	objects := []Object{}
	objectEntities, err := repository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(tx, bucketName, prefix, startAfter)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key, delimiter)
			if commonPrefix != nil && !slices.Contains(commonPrefixes, *commonPrefix) {
				commonPrefixes = append(commonPrefixes, *commonPrefix)
			}
		}
		if len(objects) < maxKeys {
			blobEntities, err := repository.FindBlobsByObjectIdOrderBySequenceNumberAsc(tx, objectEntity.Id)
			if err != nil {
				tx.Rollback()
				return nil, err
			}
			blobs := []Blob{}
			for _, blobEntity := range blobEntities {
				blobStruc := Blob{
					Id:   ulid.MustParse(blobEntity.BlobId),
					ETag: blobEntity.ETag,
					Size: blobEntity.Size,
				}
				blobs = append(blobs, blobStruc)
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key, prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, Object{
					Key:          objectEntity.Key,
					LastModified: objectEntity.UpdatedAt,
					ETag:         objectEntity.ETag,
					Size:         objectEntity.Size,
					Blobs:        blobs,
				})
			}
		}
	}
	tx.Commit()
	listBucketResult := ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    *keyCount > maxKeys,
	}
	return &listBucketResult, nil
}

func (sms *SqlMetadataStore) ListObjects(bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, err
	}

	exists, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if !*exists {
		tx.Rollback()
		return nil, ErrNoSuchBucket
	}

	return sms.listObjects(tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (sms *SqlMetadataStore) HeadObject(bucketName string, key string) (*Object, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, err
	}

	exists, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if !*exists {
		tx.Rollback()
		return nil, ErrNoSuchBucket
	}

	objectEntity, err := repository.FindObjectByBucketNameAndKey(tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if objectEntity == nil {
		tx.Rollback()
		return nil, ErrNoSuchKey
	}
	blobEntities, err := repository.FindBlobsByObjectIdOrderBySequenceNumberAsc(tx, objectEntity.Id)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	blobs := []Blob{}
	for _, blobEntity := range blobEntities {
		blobStruc := Blob{
			Id:   ulid.MustParse(blobEntity.BlobId),
			ETag: blobEntity.ETag,
			Size: blobEntity.Size,
		}
		blobs = append(blobs, blobStruc)
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	object := Object{
		Key:          key,
		LastModified: objectEntity.UpdatedAt,
		ETag:         objectEntity.ETag,
		Size:         objectEntity.Size,
		Blobs:        blobs,
	}
	return &object, nil
}

func (sms *SqlMetadataStore) PutObject(bucketName string, object *Object) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}

	existsBucket, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	if !*existsBucket {
		tx.Rollback()
		return ErrNoSuchBucket
	}

	oldObjectEntity, err := repository.FindObjectByBucketNameAndKey(tx, bucketName, object.Key)
	if err != nil {
		tx.Rollback()
		return err
	}
	if oldObjectEntity != nil {
		// object already exists
		err = repository.DeleteBlobByObjectId(tx, oldObjectEntity.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = repository.DeleteObjectById(tx, oldObjectEntity.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	objectEntity := repository.ObjectEntity{
		BucketName:   bucketName,
		Key:          object.Key,
		ETag:         object.ETag,
		Size:         object.Size,
		UploadStatus: repository.UploadStatusCompleted,
	}
	err = repository.SaveObject(tx, &objectEntity)
	objectId := objectEntity.Id
	if err != nil {
		tx.Rollback()
		return err
	}
	sequenceNumber := 0
	for _, blobStruc := range object.Blobs {
		blobEntity := repository.BlobEntity{
			BlobId:         blobStruc.Id.String(),
			ObjectId:       objectId,
			ETag:           blobStruc.ETag,
			Size:           blobStruc.Size,
			SequenceNumber: sequenceNumber,
		}
		err = repository.SaveBlob(tx, &blobEntity)
		if err != nil {
			tx.Rollback()
			return err
		}
		sequenceNumber += 1
	}
	tx.Commit()
	return nil
}

func (sms *SqlMetadataStore) DeleteObject(bucketName string, key string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}

	exists, err := repository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	if !*exists {
		tx.Rollback()
		return ErrNoSuchBucket
	}

	objectEntity, err := repository.FindObjectByBucketNameAndKey(tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	if objectEntity != nil {
		err = repository.DeleteBlobByObjectId(tx, objectEntity.Id)
		if err != nil {
			tx.Rollback()
			return err
		}

		err = repository.DeleteObjectById(tx, objectEntity.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	tx.Commit()
	return nil
}

func (sms *SqlMetadataStore) CreateMultipartUpload(bucketName string, key string) (*InitiateMultipartUploadResult, error) {
	return &InitiateMultipartUploadResult{
		UploadId: ""}, nil
}

func (sms *SqlMetadataStore) UploadPart(bucketName string, key string, uploadId string, partNumber uint16, blob Blob) error {
	return nil
}

func (sms *SqlMetadataStore) CompleteMultipartUpload(bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	return &CompleteMultipartUploadResult{}, nil
}

func (sms *SqlMetadataStore) AbortMultipartUpload(bucketName string, key string, uploadId string) (*AbortMultipartResult, error) {
	return &AbortMultipartResult{
		Blobs: []Blob{}}, nil
}
