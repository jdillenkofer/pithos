package sql

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"hash/crc32"
	"slices"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/crcutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
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
					Id:                blobEntity.BlobId,
					ETag:              blobEntity.ETag,
					ChecksumCRC32:     blobEntity.ChecksumCRC32,
					ChecksumCRC32C:    blobEntity.ChecksumCRC32C,
					ChecksumCRC64NVME: blobEntity.ChecksumCRC64NVME,
					ChecksumSHA1:      blobEntity.ChecksumSHA1,
					ChecksumSHA256:    blobEntity.ChecksumSHA256,
					Size:              blobEntity.Size,
				}
				blobs = append(blobs, blobStruc)
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key, prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, metadatastore.Object{
					Key:               objectEntity.Key,
					LastModified:      objectEntity.UpdatedAt,
					ETag:              objectEntity.ETag,
					ChecksumCRC32:     objectEntity.ChecksumCRC32,
					ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
					ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
					ChecksumSHA1:      objectEntity.ChecksumSHA1,
					ChecksumSHA256:    objectEntity.ChecksumSHA256,
					ChecksumType:      objectEntity.ChecksumType,
					Size:              objectEntity.Size,
					Blobs:             blobs,
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
			Id:                blobEntity.BlobId,
			ETag:              blobEntity.ETag,
			ChecksumCRC32:     blobEntity.ChecksumCRC32,
			ChecksumCRC32C:    blobEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: blobEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      blobEntity.ChecksumSHA1,
			ChecksumSHA256:    blobEntity.ChecksumSHA256,
			Size:              blobEntity.Size,
		}
	}, blobEntities)

	return &metadatastore.Object{
		Key:               key,
		ContentType:       objectEntity.ContentType,
		LastModified:      objectEntity.UpdatedAt,
		ETag:              objectEntity.ETag,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
		Size:              objectEntity.Size,
		Blobs:             blobs,
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
			BlobId:            blobStruc.Id,
			ObjectId:          *objectId,
			ETag:              blobStruc.ETag,
			ChecksumCRC32:     blobStruc.ChecksumCRC32,
			ChecksumCRC32C:    blobStruc.ChecksumCRC32C,
			ChecksumCRC64NVME: blobStruc.ChecksumCRC64NVME,
			ChecksumSHA1:      blobStruc.ChecksumSHA1,
			ChecksumSHA256:    blobStruc.ChecksumSHA256,
			Size:              blobStruc.Size,
			SequenceNumber:    sequenceNumber,
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

func (sms *sqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, contentType *string, checksumType *string) (*metadatastore.InitiateMultipartUploadResult, error) {
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
		UploadId:     ptrutils.ToPtr(ulid.Make().String()),
		UploadStatus: object.UploadStatusPending,
		ChecksumType: checksumType,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
	}

	return &metadatastore.InitiateMultipartUploadResult{
		UploadId: *objectEntity.UploadId,
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
		BlobId:            blb.Id,
		ObjectId:          *objectEntity.Id,
		ETag:              blb.ETag,
		ChecksumCRC32:     blb.ChecksumCRC32,
		ChecksumCRC32C:    blb.ChecksumCRC32C,
		ChecksumCRC64NVME: blb.ChecksumCRC64NVME,
		ChecksumSHA1:      blb.ChecksumSHA1,
		ChecksumSHA256:    blb.ChecksumSHA256,
		Size:              blb.Size,
		SequenceNumber:    int(partNumber),
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

	checksumType := metadatastore.ChecksumTypeFullObject
	if objectEntity.ChecksumType != nil {
		checksumType = *objectEntity.ChecksumType
	}

	// Validate SequenceNumbers and calculate totalSize
	var totalSize int64 = 0
	etagMd5Hash := md5.New()

	skipCrc32 := false
	skipCrc32c := false
	skipCrc64Nvme := false
	skipSha1 := false
	skipSha256 := false

	// -- The following variables are only used by checksumType Composite
	crc32Hash := crc32.NewIEEE()
	crc32cHash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()
	// --

	// -- The following variables are only used by checksumType FullObject
	var crc32Combined *[]byte = nil
	var crc32cCombined *[]byte = nil
	var crc64NvmeCombined *[]byte = nil
	// --

	for i, blobEntity := range blobEntities {
		if i+1 != blobEntity.SequenceNumber {
			return nil, metadatastore.ErrUploadWithInvalidSequenceNumber
		}
		totalSize += blobEntity.Size

		_, err = etagMd5Hash.Write([]byte(blobEntity.ETag))
		if err != nil {
			return nil, err
		}

		if checksumType == metadatastore.ChecksumTypeComposite {
			if blobEntity.ChecksumCRC32 != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumCRC32)
				if err != nil {
					return nil, err
				}
				_, err = crc32Hash.Write(data)
				if err != nil {
					return nil, err
				}
			} else {
				skipCrc32 = true
			}

			if blobEntity.ChecksumCRC32C != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumCRC32C)
				if err != nil {
					return nil, err
				}
				_, err = crc32cHash.Write(data)
				if err != nil {
					return nil, err
				}
			} else {
				skipCrc32c = true
			}

			// not supported for checksumType Composite
			skipCrc64Nvme = true

			if blobEntity.ChecksumSHA1 != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumSHA1)
				if err != nil {
					return nil, err
				}
				_, err = sha1Hash.Write(data)
				if err != nil {
					return nil, err
				}
			} else {
				skipSha1 = true
			}

			if blobEntity.ChecksumSHA256 != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumSHA256)
				if err != nil {
					return nil, err
				}
				_, err = sha256Hash.Write(data)
				if err != nil {
					return nil, err
				}
			} else {
				skipSha256 = true
			}
		} else if checksumType == metadatastore.ChecksumTypeFullObject {
			if blobEntity.ChecksumCRC32 != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumCRC32)
				if err != nil {
					return nil, err
				}
				if crc32Combined == nil {
					crc32Combined = &data
				} else {
					crc32Combined = ptrutils.ToPtr(crcutils.CombineCrc32(*crc32Combined, data, blobEntity.Size))
				}
			} else {
				skipCrc32 = true
			}

			if blobEntity.ChecksumCRC32C != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumCRC32C)
				if err != nil {
					return nil, err
				}
				if crc32cCombined == nil {
					crc32cCombined = &data
				} else {
					crc32cCombined = ptrutils.ToPtr(crcutils.CombineCrc32c(*crc32cCombined, data, blobEntity.Size))
				}
			} else {
				skipCrc32c = true
			}

			if blobEntity.ChecksumCRC64NVME != nil {
				data, err := base64.StdEncoding.DecodeString(*blobEntity.ChecksumCRC64NVME)
				if err != nil {
					return nil, err
				}
				if crc64NvmeCombined == nil {
					crc64NvmeCombined = &data
				} else {
					crc64NvmeCombined = ptrutils.ToPtr(crcutils.CombineCrc64Nvme(*crc64NvmeCombined, data, blobEntity.Size))
				}
			} else {
				skipCrc64Nvme = true
			}

			skipSha1 = true
			skipSha256 = true
		}
	}

	etag := "\"" + hex.EncodeToString(etagMd5Hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)) + "\""

	var crc32Digest *string = nil
	var crc32cDigest *string = nil
	var crc64NvmeDigest *string = nil
	var sha1Digest *string = nil
	var sha256Digest *string = nil

	if checksumType == metadatastore.ChecksumTypeComposite {
		if !skipCrc32 {
			crc32Digest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(crc32Hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)))
		}
		if !skipCrc32c {
			crc32cDigest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(crc32cHash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)))
		}
		if !skipSha1 {
			sha1Digest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(sha1Hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)))
		}
		if !skipSha256 {
			sha256Digest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(sha256Hash.Sum([]byte{})) + "-" + strconv.Itoa(len(blobEntities)))
		}
	} else if checksumType == metadatastore.ChecksumTypeFullObject {
		if !skipCrc32 && crc32Combined != nil {
			crc32Digest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(*crc32Combined))
		}
		if !skipCrc32c && crc32cCombined != nil {
			crc32cDigest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(*crc32cCombined))
		}
		if !skipCrc64Nvme && crc64NvmeCombined != nil {
			crc64NvmeDigest = ptrutils.ToPtr(base64.StdEncoding.EncodeToString(*crc64NvmeCombined))
		}
	}

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
				Id:                blobEntity.BlobId,
				ETag:              blobEntity.ETag,
				ChecksumCRC32:     blobEntity.ChecksumCRC32,
				ChecksumCRC32C:    blobEntity.ChecksumCRC32C,
				ChecksumCRC64NVME: blobEntity.ChecksumCRC64NVME,
				ChecksumSHA1:      blobEntity.ChecksumSHA1,
				ChecksumSHA256:    blobEntity.ChecksumSHA256,
				Size:              blobEntity.Size,
			}
		}, oldBlobEntities)

		err = sms.objectRepository.DeleteObjectById(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}
	}

	objectEntity.UploadStatus = object.UploadStatusCompleted
	objectEntity.UploadId = nil
	objectEntity.Size = totalSize
	objectEntity.ETag = etag
	objectEntity.ChecksumCRC32 = crc32Digest
	objectEntity.ChecksumCRC32C = crc32cDigest
	objectEntity.ChecksumCRC64NVME = crc64NvmeDigest
	objectEntity.ChecksumSHA1 = sha1Digest
	objectEntity.ChecksumSHA256 = sha256Digest
	objectEntity.ChecksumType = ptrutils.ToPtr(checksumType)
	err = sms.objectRepository.SaveObject(ctx, tx, objectEntity)
	if err != nil {
		return nil, err
	}

	return &metadatastore.CompleteMultipartUploadResult{
		DeletedBlobs:      deletedBlobs,
		ETag:              objectEntity.ETag,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
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
			Id:                blobEntity.BlobId,
			ETag:              blobEntity.ETag,
			ChecksumCRC32:     blobEntity.ChecksumCRC32,
			ChecksumCRC32C:    blobEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: blobEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      blobEntity.ChecksumSHA1,
			ChecksumSHA256:    blobEntity.ChecksumSHA256,
			Size:              blobEntity.Size,
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
					UploadId:  *objectEntity.UploadId,
					Initiated: objectEntity.CreatedAt,
				})
			}
			nextKeyMarker = objectEntity.Key
			nextUploadIdMarker = *objectEntity.UploadId
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

	var partNumberMarkerI32 int32 = 0
	if partNumberMarker != "" {
		partNumberMarkerI64, err := strconv.ParseInt(partNumberMarker, 10, 32)
		if err != nil {
			return nil, err
		}
		partNumberMarkerI32 = int32(partNumberMarkerI64)
	}

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
			ETag:              blob.ETag,
			ChecksumCRC32:     blob.ChecksumCRC32,
			ChecksumCRC32C:    blob.ChecksumCRC32C,
			ChecksumCRC64NVME: blob.ChecksumCRC64NVME,
			ChecksumSHA1:      blob.ChecksumSHA1,
			ChecksumSHA256:    blob.ChecksumSHA256,
			LastModified:      blob.UpdatedAt,
			PartNumber:        sequenceNumberI32,
			Size:              blob.Size,
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
