package metadata

import (
	"database/sql"
	"slices"
	"strings"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type SqlMetadataStore struct {
	bucketRepository repository.BucketRepository
	objectRepository repository.ObjectRepository
	blobRepository   repository.BlobRepository
}

func NewSqlMetadataStore() (*SqlMetadataStore, error) {
	return &SqlMetadataStore{
		bucketRepository: repository.NewBucketRepository(),
		objectRepository: repository.NewObjectRepository(),
		blobRepository:   repository.NewBlobRepository(),
	}, nil
}

func (sms *SqlMetadataStore) Start() error {
	return nil
}

func (sms *SqlMetadataStore) Stop() error {
	return nil
}

func (sms *SqlMetadataStore) CreateBucket(tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return err
	}
	if *exists {
		return ErrBucketAlreadyExists
	}

	err = sms.bucketRepository.SaveBucket(tx, &repository.BucketEntity{
		Name: bucketName,
	})
	if err != nil {
		return err
	}

	return nil
}

func (sms *SqlMetadataStore) DeleteBucket(tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return ErrNoSuchBucket
	}

	containsBucketObjects, err := sms.objectRepository.ContainsBucketObjectsByBucketName(tx, bucketName)
	if err != nil {
		return err
	}
	if *containsBucketObjects {
		return ErrBucketNotEmpty
	}

	err = sms.bucketRepository.DeleteBucketByName(tx, bucketName)
	if err != nil {
		return err
	}

	return nil
}

func (sms *SqlMetadataStore) ListBuckets(tx *sql.Tx) ([]Bucket, error) {
	bucketEntities, err := sms.bucketRepository.FindAllBuckets(tx)
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucketEntity repository.BucketEntity) Bucket {
		return Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		}
	}, bucketEntities)

	return buckets, nil
}

func (sms *SqlMetadataStore) HeadBucket(tx *sql.Tx, bucketName string) (*Bucket, error) {
	bucketEntity, err := sms.bucketRepository.FindBucketByName(tx, bucketName)
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

func (sms *SqlMetadataStore) listObjects(tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(tx, bucketName, prefix, startAfter)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	objects := []Object{}
	objectEntities, err := sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(tx, bucketName, prefix, startAfter)
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
			blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(tx, *objectEntity.Id)
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

func (sms *SqlMetadataStore) ListObjects(tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	return sms.listObjects(tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (sms *SqlMetadataStore) HeadObject(tx *sql.Tx, bucketName string, key string) (*Object, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, ErrNoSuchKey
	}
	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	blobs := sliceutils.Map(func(blobEntity repository.BlobEntity) Blob {
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

func (sms *SqlMetadataStore) PutObject(tx *sql.Tx, bucketName string, object *Object) error {
	existsBucket, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return err
	}
	if !*existsBucket {
		return ErrNoSuchBucket
	}

	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(tx, bucketName, object.Key)
	if err != nil {
		return err
	}
	if oldObjectEntity != nil {
		// object already exists
		err = sms.blobRepository.DeleteBlobsByObjectId(tx, *oldObjectEntity.Id)
		if err != nil {
			return err
		}
		err = sms.objectRepository.DeleteObjectById(tx, *oldObjectEntity.Id)
		if err != nil {
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
	err = sms.objectRepository.SaveObject(tx, &objectEntity)
	objectId := objectEntity.Id
	if err != nil {
		return err
	}
	sequenceNumber := 0
	for _, blobStruc := range object.Blobs {
		blobEntity := repository.BlobEntity{
			BlobId:         blobStruc.Id,
			ObjectId:       *objectId,
			ETag:           blobStruc.ETag,
			Size:           blobStruc.Size,
			SequenceNumber: sequenceNumber,
		}
		err = sms.blobRepository.SaveBlob(tx, &blobEntity)
		if err != nil {
			return err
		}
		sequenceNumber += 1
	}

	return nil
}

func (sms *SqlMetadataStore) DeleteObject(tx *sql.Tx, bucketName string, key string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(tx, bucketName, key)
	if err != nil {
		return err
	}

	if objectEntity != nil {
		err = sms.blobRepository.DeleteBlobsByObjectId(tx, *objectEntity.Id)
		if err != nil {
			return err
		}

		err = sms.objectRepository.DeleteObjectById(tx, *objectEntity.Id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sms *SqlMetadataStore) CreateMultipartUpload(tx *sql.Tx, bucketName string, key string) (*InitiateMultipartUploadResult, error) {
	return &InitiateMultipartUploadResult{
		UploadId: "",
	}, nil
}

func (sms *SqlMetadataStore) UploadPart(tx *sql.Tx, bucketName string, key string, uploadId string, partNumber uint16, blob Blob) error {
	return nil
}

func (sms *SqlMetadataStore) CompleteMultipartUpload(tx *sql.Tx, bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	return &CompleteMultipartUploadResult{}, nil
}

func (sms *SqlMetadataStore) AbortMultipartUpload(tx *sql.Tx, bucketName string, key string, uploadId string) (*AbortMultipartResult, error) {
	return &AbortMultipartResult{
		Blobs: []Blob{},
	}, nil
}
