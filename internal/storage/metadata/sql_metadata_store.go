package metadata

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	blobRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blob"
	bucketRepository "github.com/jdillenkofer/pithos/internal/storage/repository/bucket"
	objectRepository "github.com/jdillenkofer/pithos/internal/storage/repository/object"
	"github.com/oklog/ulid/v2"
)

type SqlMetadataStore struct {
	bucketRepository bucketRepository.BucketRepository
	objectRepository objectRepository.ObjectRepository
	blobRepository   blobRepository.BlobRepository
}

func NewSqlMetadataStore(db *sql.DB, bucketRepository bucketRepository.BucketRepository, objectRepository objectRepository.ObjectRepository, blobRepository blobRepository.BlobRepository) (*SqlMetadataStore, error) {
	return &SqlMetadataStore{
		bucketRepository: bucketRepository,
		objectRepository: objectRepository,
		blobRepository:   blobRepository,
	}, nil
}

func (sms *SqlMetadataStore) Start(ctx context.Context) error {
	return nil
}

func (sms *SqlMetadataStore) Stop(ctx context.Context) error {
	return nil
}

func (sms *SqlMetadataStore) GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	return sms.blobRepository.FindInUseBlobIds(ctx, tx)
}

func (sms *SqlMetadataStore) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *exists {
		return ErrBucketAlreadyExists
	}

	err = sms.bucketRepository.SaveBucket(ctx, tx, &bucketRepository.BucketEntity{
		Name: bucketName,
	})
	if err != nil {
		return err
	}

	return nil
}

func (sms *SqlMetadataStore) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return ErrNoSuchBucket
	}

	containsBucketObjects, err := sms.objectRepository.ContainsBucketObjectsByBucketName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *containsBucketObjects {
		return ErrBucketNotEmpty
	}

	err = sms.bucketRepository.DeleteBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}

	return nil
}

func (sms *SqlMetadataStore) ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error) {
	bucketEntities, err := sms.bucketRepository.FindAllBuckets(ctx, tx)
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucketEntity bucketRepository.BucketEntity) Bucket {
		return Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		}
	}, bucketEntities)

	return buckets, nil
}

func (sms *SqlMetadataStore) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*Bucket, error) {
	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, ErrNoSuchBucket
	}

	bucket := Bucket{
		Name:         bucketEntity.Name,
		CreationDate: bucketEntity.CreatedAt,
	}

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

func (sms *SqlMetadataStore) listObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(ctx, tx, bucketName, prefix, startAfter)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	objects := []Object{}
	objectEntities, err := sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx, tx, bucketName, prefix, startAfter)
	if err != nil {
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
			blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
			if err != nil {
				return nil, err
			}
			blobs := []Blob{}
			for _, blobEntity := range blobEntities {
				blobStruc := Blob{
					Id:   blobEntity.BlobId,
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

	listBucketResult := ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    *keyCount > maxKeys,
	}
	return &listBucketResult, nil
}

func (sms *SqlMetadataStore) ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	return sms.listObjects(ctx, tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (sms *SqlMetadataStore) HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*Object, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, ErrNoSuchKey
	}
	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	blobs := sliceutils.Map(func(blobEntity blobRepository.BlobEntity) Blob {
		return Blob{
			Id:   blobEntity.BlobId,
			ETag: blobEntity.ETag,
			Size: blobEntity.Size,
		}
	}, blobEntities)

	object := Object{
		Key:          key,
		LastModified: objectEntity.UpdatedAt,
		ETag:         objectEntity.ETag,
		Size:         objectEntity.Size,
		Blobs:        blobs,
	}
	return &object, nil
}

func (sms *SqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName string, object *Object) error {
	existsBucket, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*existsBucket {
		return ErrNoSuchBucket
	}

	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, object.Key)
	if err != nil {
		return err
	}
	if oldObjectEntity != nil {
		// object already exists
		err = sms.blobRepository.DeleteBlobsByObjectId(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return err
		}
		err = sms.objectRepository.DeleteObjectById(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return err
		}
	}
	objectEntity := objectRepository.ObjectEntity{
		BucketName:   bucketName,
		Key:          object.Key,
		ETag:         object.ETag,
		Size:         object.Size,
		UploadStatus: objectRepository.UploadStatusCompleted,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	objectId := objectEntity.Id
	if err != nil {
		return err
	}
	sequenceNumber := 0
	for _, blobStruc := range object.Blobs {
		blobEntity := blobRepository.BlobEntity{
			BlobId:         blobStruc.Id,
			ObjectId:       *objectId,
			ETag:           blobStruc.ETag,
			Size:           blobStruc.Size,
			SequenceNumber: sequenceNumber,
		}
		err = sms.blobRepository.SaveBlob(ctx, tx, &blobEntity)
		if err != nil {
			return err
		}
		sequenceNumber += 1
	}

	return nil
}

func (sms *SqlMetadataStore) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return err
	}

	if objectEntity != nil {
		err = sms.blobRepository.DeleteBlobsByObjectId(ctx, tx, *objectEntity.Id)
		if err != nil {
			return err
		}

		err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sms *SqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*InitiateMultipartUploadResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	objectEntity := objectRepository.ObjectEntity{
		BucketName:   bucketName,
		Key:          key,
		ETag:         "",
		Size:         -1,
		UploadId:     ulid.Make().String(),
		UploadStatus: objectRepository.UploadStatusPending,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
	}

	return &InitiateMultipartUploadResult{
		UploadId: objectEntity.UploadId,
	}, nil
}

func (sms *SqlMetadataStore) UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blob Blob) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return ErrNoSuchKey
	}

	blobEntity := blobRepository.BlobEntity{
		BlobId:         blob.Id,
		ObjectId:       *objectEntity.Id,
		ETag:           blob.ETag,
		Size:           blob.Size,
		SequenceNumber: int(partNumber),
	}
	err = sms.blobRepository.SaveBlob(ctx, tx, &blobEntity)
	if err != nil {
		return err
	}
	return nil
}

func (sms *SqlMetadataStore) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, ErrNoSuchKey
	}

	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	// Validate SequenceNumbers and calculate totalSize
	var totalSize int64 = 0
	hash := md5.New()
	for i, blobEntity := range blobEntities {
		if i+1 != blobEntity.SequenceNumber {
			return nil, ErrUploadWithInvalidSequenceNumber
		}
		totalSize += blobEntity.Size

		_, err = hash.Write([]byte(blobEntity.ETag))
		if err != nil {
			return nil, err
		}
	}
	etag := "\"" + hex.EncodeToString(hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)) + "\""

	deletedBlobs := []Blob{}

	// Remove old objects
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if oldObjectEntity != nil {
		oldBlobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}

		err = sms.blobRepository.DeleteBlobsByObjectId(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}

		deletedBlobs = sliceutils.Map(func(blobEntity blobRepository.BlobEntity) Blob {
			return Blob{
				Id:   blobEntity.BlobId,
				ETag: blobEntity.ETag,
				Size: blobEntity.Size,
			}
		}, oldBlobEntities)

		err = sms.objectRepository.DeleteObjectById(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}
	}

	objectEntity.UploadStatus = objectRepository.UploadStatusCompleted
	objectEntity.Size = totalSize
	objectEntity.ETag = etag
	err = sms.objectRepository.SaveObject(ctx, tx, objectEntity)
	if err != nil {
		return nil, err
	}

	return &CompleteMultipartUploadResult{
		DeletedBlobs: deletedBlobs,
		ETag:         etag,
	}, nil
}

func (sms *SqlMetadataStore) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*AbortMultipartResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, ErrNoSuchKey
	}

	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.blobRepository.DeleteBlobsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	blobs := sliceutils.Map(func(blobEntity blobRepository.BlobEntity) Blob {
		return Blob{
			Id:   blobEntity.BlobId,
			ETag: blobEntity.ETag,
			Size: blobEntity.Size,
		}
	}, blobEntities)

	err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &AbortMultipartResult{
		DeletedBlobs: blobs,
	}, nil
}
