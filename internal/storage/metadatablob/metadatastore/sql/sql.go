package sql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blob"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/oklog/ulid/v2"
)

type sqlMetadataStore struct {
	bucketRepository bucket.Repository
	objectRepository object.Repository
	blobRepository   blob.Repository
}

func New(db database.Database, bucketRepository bucket.Repository, objectRepository object.Repository, blobRepository blob.Repository) (metadatastore.MetadataStore, error) {
	return &sqlMetadataStore{
		bucketRepository: bucketRepository,
		objectRepository: objectRepository,
		blobRepository:   blobRepository,
	}, nil
}

func (sms *sqlMetadataStore) Start(ctx context.Context) error {
	return nil
}

func (sms *sqlMetadataStore) Stop(ctx context.Context) error {
	return nil
}

func (sms *sqlMetadataStore) GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	return sms.blobRepository.FindInUseBlobIds(ctx, tx)
}

func (sms *sqlMetadataStore) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *exists {
		return metadatastore.ErrBucketAlreadyExists
	}

	err = sms.bucketRepository.SaveBucket(ctx, tx, &bucket.Entity{
		Name: bucketName,
	})
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	containsBucketObjects, err := sms.objectRepository.ContainsBucketObjectsByBucketName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *containsBucketObjects {
		return metadatastore.ErrBucketNotEmpty
	}

	err = sms.bucketRepository.DeleteBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) ListBuckets(ctx context.Context, tx *sql.Tx) ([]metadatastore.Bucket, error) {
	bucketEntities, err := sms.bucketRepository.FindAllBuckets(ctx, tx)
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucketEntity bucket.Entity) metadatastore.Bucket {
		return metadatastore.Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		}
	}, bucketEntities)

	return buckets, nil
}

func (sms *sqlMetadataStore) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*metadatastore.Bucket, error) {
	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	bucket := metadatastore.Bucket{
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

func (sms *sqlMetadataStore) listObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int32) (*metadatastore.ListBucketResult, error) {
	keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(ctx, tx, bucketName, prefix, startAfter)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	objects := []metadatastore.Object{}
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
		if int32(len(objects)) < maxKeys {
			blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
			if err != nil {
				return nil, err
			}
			blobs := []metadatastore.Blob{}
			for _, blobEntity := range blobEntities {
				blobStruc := metadatastore.Blob{
					Id:   blobEntity.BlobId,
					ETag: blobEntity.ETag,
					Size: blobEntity.Size,
				}
				blobs = append(blobs, blobStruc)
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key, prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, metadatastore.Object{
					Key:          objectEntity.Key,
					LastModified: objectEntity.UpdatedAt,
					ETag:         objectEntity.ETag,
					Size:         objectEntity.Size,
					Blobs:        blobs,
				})
			}
		}
	}

	listBucketResult := metadatastore.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    int32(*keyCount) > maxKeys,
	}
	return &listBucketResult, nil
}

func (sms *sqlMetadataStore) ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int32) (*metadatastore.ListBucketResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	return sms.listObjects(ctx, tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (sms *sqlMetadataStore) HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*metadatastore.Object, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}
	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	blobs := sliceutils.Map(func(blobEntity blob.Entity) metadatastore.Blob {
		return metadatastore.Blob{
			Id:   blobEntity.BlobId,
			ETag: blobEntity.ETag,
			Size: blobEntity.Size,
		}
	}, blobEntities)

	return &metadatastore.Object{
		Key:          key,
		ContentType:  objectEntity.ContentType,
		LastModified: objectEntity.UpdatedAt,
		ETag:         objectEntity.ETag,
		Size:         objectEntity.Size,
		Blobs:        blobs,
	}, nil
}

func (sms *sqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName string, obj *metadatastore.Object) error {
	existsBucket, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*existsBucket {
		return metadatastore.ErrNoSuchBucket
	}

	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
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
	objectEntity := object.Entity{
		BucketName:   bucketName,
		ContentType:  obj.ContentType,
		Key:          obj.Key,
		ETag:         obj.ETag,
		Size:         obj.Size,
		UploadStatus: object.UploadStatusCompleted,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	objectId := objectEntity.Id
	if err != nil {
		return err
	}
	sequenceNumber := 0
	for _, blobStruc := range obj.Blobs {
		blobEntity := blob.Entity{
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

func (sms *sqlMetadataStore) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
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

func (sms *sqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, contentType string) (*metadatastore.InitiateMultipartUploadResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity := object.Entity{
		BucketName:   bucketName,
		Key:          key,
		ContentType:  contentType,
		ETag:         "",
		Size:         -1,
		UploadId:     ulid.Make().String(),
		UploadStatus: object.UploadStatusPending,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
	}

	return &metadatastore.InitiateMultipartUploadResult{
		UploadId: objectEntity.UploadId,
	}, nil
}

func (sms *sqlMetadataStore) UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blb metadatastore.Blob) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	blobEntity := blob.Entity{
		BlobId:         blb.Id,
		ObjectId:       *objectEntity.Id,
		ETag:           blb.ETag,
		Size:           blb.Size,
		SequenceNumber: int(partNumber),
	}
	err = sms.blobRepository.SaveBlob(ctx, tx, &blobEntity)
	if err != nil {
		return err
	}
	return nil
}

func (sms *sqlMetadataStore) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*metadatastore.CompleteMultipartUploadResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
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
			return nil, metadatastore.ErrUploadWithInvalidSequenceNumber
		}
		totalSize += blobEntity.Size

		_, err = hash.Write([]byte(blobEntity.ETag))
		if err != nil {
			return nil, err
		}
	}
	etag := "\"" + hex.EncodeToString(hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)) + "\""

	deletedBlobs := []metadatastore.Blob{}

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

		deletedBlobs = sliceutils.Map(func(blobEntity blob.Entity) metadatastore.Blob {
			return metadatastore.Blob{
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

	objectEntity.UploadStatus = object.UploadStatusCompleted
	objectEntity.Size = totalSize
	objectEntity.ETag = etag
	err = sms.objectRepository.SaveObject(ctx, tx, objectEntity)
	if err != nil {
		return nil, err
	}

	return &metadatastore.CompleteMultipartUploadResult{
		DeletedBlobs: deletedBlobs,
		ETag:         etag,
	}, nil
}

func (sms *sqlMetadataStore) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*metadatastore.AbortMultipartResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	blobEntities, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.blobRepository.DeleteBlobsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	blobs := sliceutils.Map(func(blobEntity blob.Entity) metadatastore.Blob {
		return metadatastore.Blob{
			Id:   blobEntity.BlobId,
			ETag: blobEntity.ETag,
			Size: blobEntity.Size,
		}
	}, blobEntities)

	err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.AbortMultipartResult{
		DeletedBlobs: blobs,
	}, nil
}

func (sms *sqlMetadataStore) ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*metadatastore.ListMultipartUploadsResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	keyCount, err := sms.objectRepository.CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	uploads := []metadatastore.Upload{}
	objectEntities, err := sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
	if err != nil {
		return nil, err
	}

	nextKeyMarker := ""
	nextUploadIdMarker := ""

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key, delimiter)
			if commonPrefix != nil && !slices.Contains(commonPrefixes, *commonPrefix) {
				commonPrefixes = append(commonPrefixes, *commonPrefix)
			}
		}
		if int32(len(uploads)) < maxUploads {
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key, prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				uploads = append(uploads, metadatastore.Upload{
					Key:       objectEntity.Key,
					UploadId:  objectEntity.UploadId,
					Initiated: objectEntity.CreatedAt,
				})
			}
			nextKeyMarker = objectEntity.Key
			nextUploadIdMarker = objectEntity.UploadId
		}
	}

	listMultipartUploadsResult := metadatastore.ListMultipartUploadsResult{
		Bucket:             bucketName,
		KeyMarker:          keyMarker,
		UploadIdMarker:     uploadIdMarker,
		Prefix:             prefix,
		Delimiter:          delimiter,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIdMarker,
		MaxUploads:         maxUploads,
		CommonPrefixes:     commonPrefixes,
		Uploads:            uploads,
		IsTruncated:        int32(*keyCount) > maxUploads,
	}
	return &listMultipartUploadsResult, nil
}

func (sms *sqlMetadataStore) ListParts(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumberMarker string, maxParts int32) (*metadatastore.ListPartsResult, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil || objectEntity.UploadStatus != object.UploadStatusPending {
		return nil, metadatastore.ErrNoSuchKey
	}

	blobs, err := sms.blobRepository.FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	partNumberMarkerI64, err := strconv.ParseInt(partNumberMarker, 10, 32)
	if err != nil {
		return nil, err
	}
	partNumberMarkerI32 := int32(partNumberMarkerI64)

	parts := []*metadatastore.Part{}
	startOffset := -1
	var nextPartNumberMarker *string = nil
	isTruncated := false
	for idx, blob := range blobs {
		sequenceNumberI32 := int32(blob.SequenceNumber)
		if sequenceNumberI32 <= partNumberMarkerI32 {
			startOffset = idx
			continue
		}
		parts = append(parts, &metadatastore.Part{
			ETag:         blob.ETag,
			LastModified: blob.UpdatedAt,
			PartNumber:   sequenceNumberI32,
			Size:         blob.Size,
		})
		if len(parts) >= int(maxParts) {
			isTruncated = len(blobs)-(startOffset+1) > int(maxParts)
			lastPartNumberMarker := strconv.Itoa(blob.SequenceNumber)
			nextPartNumberMarker = &lastPartNumberMarker
			break
		}
	}

	return &metadatastore.ListPartsResult{
		Bucket:               bucketName,
		Key:                  key,
		UploadId:             uploadId,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
	}, nil
}
