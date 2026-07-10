package sql

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, contentType *string, checksumType *string, opts *metadatastore.CreateMultipartUploadOptions) (*metadatastore.InitiateMultipartUploadResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CreateMultipartUpload")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if checksumType == nil {
		checksumType = ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject)
	}

	objectEntity := object.Entity{
		BucketName:     bucketName,
		Key:            key,
		ContentType:    contentType,
		ETag:           "",
		ChecksumType:   checksumType,
		Size:           -1,
		IsLatest:       false,
		IsDeleteMarker: false,
		UploadId:       ptrutils.ToPtr(metadatastore.NewRandomUploadId()),
		UploadStatus:   object.UploadStatusPending,
	}
	if opts != nil {
		// The class chosen at CreateMultipartUpload is carried to the final
		// object because CompleteMultipartUpload reuses this row.
		objectEntity.StorageClass = opts.StorageClass
	}
	if opts != nil && opts.Metadata != nil {
		applySystemMetadataToEntity(&objectEntity, *opts.Metadata)
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
	}

	// Persist any tags supplied via x-amz-tagging and any user-defined metadata
	// on the pending object. They are carried over when the upload is completed
	// (the object row is reused).
	if opts != nil && len(opts.Tags) > 0 {
		if err := sms.replaceObjectTags(ctx, tx, *objectEntity.Id, opts.Tags); err != nil {
			return nil, err
		}
	}
	if opts != nil && opts.Metadata != nil && len(opts.Metadata.UserMetadata) > 0 {
		if err := sms.replaceObjectUserMetadata(ctx, tx, *objectEntity.Id, opts.Metadata.UserMetadata); err != nil {
			return nil, err
		}
	}

	return &metadatastore.InitiateMultipartUploadResult{
		UploadId: *objectEntity.UploadId,
	}, nil
}

func (sms *sqlMetadataStore) GetMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId) (*metadatastore.Upload, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetMultipartUpload")
	defer span.End()

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

	return &metadatastore.Upload{
		Key:          objectEntity.Key,
		UploadId:     *objectEntity.UploadId,
		Initiated:    objectEntity.CreatedAt,
		StorageClass: objectEntity.StorageClass,
	}, nil
}

func (sms *sqlMetadataStore) UploadPart(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadID metadatastore.UploadId, partNumber int32, blb metadatastore.Part) (*metadatastore.PartMutationResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.UploadPart")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadID)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}
	unreferencedParts, err := sms.removePartRowsByObjectIdAndSequenceNumber(ctx, tx, *objectEntity.Id, int(partNumber))
	if err != nil {
		return nil, err
	}
	if err := sms.savePartRows(ctx, tx, *objectEntity.Id, []metadatastore.Part{blb}, int(partNumber)); err != nil {
		return nil, err
	}
	return &metadatastore.PartMutationResult{UnreferencedParts: unreferencedParts}, nil
}

func trimETagQuotes(etag string) string {
	return strings.Trim(etag, "\"")
}

func declaredPartChecksumMatches(declared *string, stored *string) bool {
	if declared == nil || stored == nil {
		return true
	}
	return *declared == *stored
}

// validateCompleteMultipartUploadParts checks the client-declared completion
// manifest against the uploaded parts loaded in this transaction.
func validateCompleteMultipartUploadParts(declaredParts []metadatastore.CompleteMultipartUploadPart, storedParts []part.Entity) error {
	if len(declaredParts) == 0 {
		return nil
	}

	storedPartsByNumber := map[int32]part.Entity{}
	for _, storedPart := range storedParts {
		storedPartsByNumber[int32(storedPart.SequenceNumber)] = storedPart
	}

	previousPartNumber := int32(0)
	for _, declaredPart := range declaredParts {
		if declaredPart.PartNumber <= previousPartNumber {
			return metadatastore.ErrInvalidPartOrder
		}
		previousPartNumber = declaredPart.PartNumber

		storedPart, ok := storedPartsByNumber[declaredPart.PartNumber]
		if !ok {
			return metadatastore.ErrInvalidPart
		}
		if declaredPart.ETag != "" && trimETagQuotes(declaredPart.ETag) != trimETagQuotes(storedPart.ETag) {
			return metadatastore.ErrInvalidPart
		}
		if !declaredPartChecksumMatches(declaredPart.ChecksumCRC32, storedPart.ChecksumCRC32) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumCRC32C, storedPart.ChecksumCRC32C) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumCRC64NVME, storedPart.ChecksumCRC64NVME) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumSHA1, storedPart.ChecksumSHA1) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumSHA256, storedPart.ChecksumSHA256) {
			return metadatastore.ErrInvalidPart
		}
	}

	if len(declaredParts) != len(storedPartsByNumber) {
		return metadatastore.ErrInvalidPart
	}

	return nil
}

func (sms *sqlMetadataStore) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, checksumInput *metadatastore.ChecksumInput, opts *metadatastore.CompleteMultipartUploadOptions) (*metadatastore.CompleteMultipartUploadResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CompleteMultipartUpload")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	checksumType := metadatastore.ChecksumTypeFullObject
	if objectEntity.ChecksumType != nil {
		checksumType = *objectEntity.ChecksumType
	}

	// Validate SequenceNumbers and calculate totalSize
	var totalSize int64 = 0
	parts := make([]checksumutils.PartChecksums, len(partEntities))

	for i, partEntity := range partEntities {
		if i+1 != partEntity.SequenceNumber {
			return nil, metadatastore.ErrUploadWithInvalidSequenceNumber
		}
		totalSize += partEntity.Size

		parts[i] = checksumutils.PartChecksums{
			ETag:              partEntity.ETag,
			ChecksumCRC32:     partEntity.ChecksumCRC32,
			ChecksumCRC32C:    partEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      partEntity.ChecksumSHA1,
			ChecksumSHA256:    partEntity.ChecksumSHA256,
			Size:              partEntity.Size,
		}
	}

	if opts != nil {
		if err = validateCompleteMultipartUploadParts(opts.Parts, partEntities); err != nil {
			return nil, err
		}
	}

	calculatedChecksums, err := checksumutils.CalculateMultipartChecksums(parts, checksumType)
	if err != nil {
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, calculatedChecksums)
	if err != nil {
		return nil, err
	}

	deletedParts := []metadatastore.Part{}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)

	latestObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	objectExists := latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker

	// Evaluate conditional headers (AWS S3 compatible behaviour):
	//   If-Match:      the current object's ETag must match the supplied value.
	//   If-None-Match: "*" means the operation must fail when any object exists.
	if opts != nil {
		if opts.IfMatchETag != nil {
			// If-Match: * — any existing non-delete-marker object satisfies the condition; fail when absent.
			// If-Match: <etag> — existing ETag must match exactly; fail otherwise.
			if *opts.IfMatchETag == metadatastore.ETagWildcard {
				if !objectExists {
					return nil, metadatastore.ErrPreconditionFailed
				}
			} else {
				if !objectExists || latestObjectEntity.ETag != *opts.IfMatchETag {
					return nil, metadatastore.ErrPreconditionFailed
				}
			}
		}
		if opts.IfNoneMatchStar {
			// If-None-Match: * — fail when any existing non-delete-marker object currently exists at the key.
			if objectExists {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}
	}
	if opts != nil && opts.IfNoneMatchStar {
		latestObjectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
		if err != nil {
			return nil, err
		}
		if latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker {
			return nil, metadatastore.ErrPreconditionFailed
		}
	}

	if opts != nil && (opts.IfMatchETag != nil || opts.IfNoneMatchStar) && latestObjectEntity != nil {
		lockedObjectEntity := *latestObjectEntity
		locked, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, latestObjectEntity.OptimisticLockVersion)
		if err != nil {
			return nil, err
		}
		if !*locked {
			return nil, metadatastore.ErrPreconditionFailed
		}
		latestObjectEntity = &lockedObjectEntity
	}

	if versioningEnabled {
		newVersionID := metadatastore.NewRandomUploadId().String()
		objectEntity.VersionID = &newVersionID
	} else {
		nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, key)
		if err != nil {
			return nil, err
		}
		if opts != nil && opts.IfNoneMatchStar && nullVersionEntity != nil {
			return nil, metadatastore.ErrPreconditionFailed
		}
		if nullVersionEntity != nil {
			unreferencedParts, err := sms.removePartRowsByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			deletedParts = append(deletedParts, unreferencedParts...)

			if opts != nil && opts.IfMatchETag != nil && latestObjectEntity != nil && latestObjectEntity.Id != nil && nullVersionEntity.Id != nil && *latestObjectEntity.Id == *nullVersionEntity.Id {
				deleted, deleteErr := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *nullVersionEntity.Id, latestObjectEntity.OptimisticLockVersion)
				if deleteErr != nil {
					return nil, deleteErr
				}
				if !*deleted {
					return nil, metadatastore.ErrPreconditionFailed
				}
			} else {
				_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}
			}
		}

		nullVersion := "null"
		objectEntity.VersionID = &nullVersion
	}

	if latestObjectEntity != nil {
		latestObjectEntity.IsLatest = false
		err = sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity)
		if err != nil {
			return nil, err
		}
	}

	objectEntity.UploadStatus = object.UploadStatusCompleted
	objectEntity.UploadId = nil
	objectEntity.IsDeleteMarker = false
	objectEntity.IsLatest = true
	objectEntity.Size = totalSize
	objectEntity.ETag = *calculatedChecksums.ETag
	objectEntity.ChecksumCRC32 = calculatedChecksums.ChecksumCRC32
	objectEntity.ChecksumCRC32C = calculatedChecksums.ChecksumCRC32C
	objectEntity.ChecksumCRC64NVME = calculatedChecksums.ChecksumCRC64NVME
	objectEntity.ChecksumSHA1 = calculatedChecksums.ChecksumSHA1
	objectEntity.ChecksumSHA256 = calculatedChecksums.ChecksumSHA256
	objectEntity.ChecksumType = ptrutils.ToPtr(checksumType)

	err = sms.objectRepository.SaveObject(ctx, tx, objectEntity)
	if err != nil {
		if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
			return nil, metadatastore.ErrPreconditionFailed
		}
		return nil, err
	}

	return &metadatastore.CompleteMultipartUploadResult{
		UnreferencedParts: deletedParts,
		ETag:              objectEntity.ETag,
		VersionID:         objectEntity.VersionID,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
	}, nil
}

func (sms *sqlMetadataStore) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId) (*metadatastore.AbortMultipartResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AbortMultipartUpload")
	defer span.End()

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

	parts, err := sms.removePartRowsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.AbortMultipartResult{
		UnreferencedParts: parts,
	}, nil
}

func (sms *sqlMetadataStore) ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListMultipartUploadsOptions) (*metadatastore.ListMultipartUploadsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListMultipartUploads")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	prefix := ""
	if opts.Prefix != nil {
		prefix = *opts.Prefix
	}
	keyMarker := ""
	if opts.KeyMarker != nil {
		keyMarker = *opts.KeyMarker
	}
	uploadIdMarker := ""
	if opts.UploadIdMarker != nil {
		uploadIdMarker = *opts.UploadIdMarker
	}
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	uploads := []metadatastore.Upload{}
	isTruncated := false
	var objectEntities []object.Entity
	if delimiter == "" {
		objectEntities, err = sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimit(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker, opts.MaxUploads+1)
		if err != nil {
			return nil, err
		}
		if int32(len(objectEntities)) > opts.MaxUploads {
			isTruncated = true
			objectEntities = objectEntities[:opts.MaxUploads]
		}
	} else {
		keyCount, err := sms.objectRepository.CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
		if err != nil {
			return nil, err
		}
		isTruncated = int32(*keyCount) > opts.MaxUploads
		objectEntities, err = sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
		if err != nil {
			return nil, err
		}
	}

	nextKeyMarker := ""
	nextUploadIdMarker := ""

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key.String(), delimiter)
			if commonPrefix != nil {
				if _, seen := commonPrefixSet[*commonPrefix]; !seen {
					commonPrefixSet[*commonPrefix] = struct{}{}
					commonPrefixes = append(commonPrefixes, *commonPrefix)
				}
			}
		}
		if int32(len(uploads)) < opts.MaxUploads {
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				uploads = append(uploads, metadatastore.Upload{
					Key:          objectEntity.Key,
					UploadId:     *objectEntity.UploadId,
					Initiated:    objectEntity.CreatedAt,
					StorageClass: objectEntity.StorageClass,
				})
			}
			nextKeyMarker = objectEntity.Key.String()
			nextUploadIdMarker = objectEntity.UploadId.String()
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
		MaxUploads:         opts.MaxUploads,
		CommonPrefixes:     commonPrefixes,
		Uploads:            uploads,
		IsTruncated:        isTruncated,
	}
	return &listMultipartUploadsResult, nil
}

func (sms *sqlMetadataStore) ListParts(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, opts metadatastore.ListPartsOptions) (*metadatastore.ListPartsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListParts")
	defer span.End()

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

	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	var partNumberMarkerI32 int32 = 0
	partNumberMarker := ""
	if opts.PartNumberMarker != nil {
		partNumberMarker = *opts.PartNumberMarker
	}
	if partNumberMarker != "" {
		partNumberMarkerI64, err := strconv.ParseInt(partNumberMarker, 10, 32)
		if err != nil {
			return nil, err
		}
		partNumberMarkerI32 = int32(partNumberMarkerI64)
	}

	parts := []*metadatastore.MultipartPart{}
	var nextPartNumberMarker *string = nil
	isTruncated := false
	for idx, part := range partEntities {
		sequenceNumberI32 := int32(part.SequenceNumber)
		if sequenceNumberI32 <= partNumberMarkerI32 {
			continue
		}
		parts = append(parts, &metadatastore.MultipartPart{
			ETag:              part.ETag,
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			LastModified:      part.UpdatedAt,
			PartNumber:        sequenceNumberI32,
			Size:              part.Size,
		})
		if len(parts) >= int(opts.MaxParts) {
			isTruncated = idx < len(partEntities)-1
			lastPartNumberMarker := strconv.Itoa(part.SequenceNumber)
			nextPartNumberMarker = &lastPartNumberMarker
			break
		}
	}

	return &metadatastore.ListPartsResult{
		BucketName:           bucketName,
		Key:                  key,
		UploadId:             uploadId,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             opts.MaxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
		StorageClass:         objectEntity.StorageClass,
	}, nil
}
