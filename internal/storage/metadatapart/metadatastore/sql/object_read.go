package sql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
)

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

func (sms *sqlMetadataStore) listObjects(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectsOptions) (*metadatastore.ListBucketResult, error) {
	prefix := ""
	if opts.Prefix != nil {
		prefix = *opts.Prefix
	}
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}
	startAfter := ""
	if opts.StartAfter != nil {
		startAfter = *opts.StartAfter
	}
	maxKeys := opts.MaxKeys

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	objects := []metadatastore.Object{}
	listedObjectIds := []ulid.ULID{}
	var err error
	isTruncated := false
	var objectEntities []object.Entity
	if delimiter == "" {
		objectEntities, err = sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimit(ctx, tx, bucketName, prefix, startAfter, maxKeys+1)
		if err != nil {
			return nil, err
		}
		if int32(len(objectEntities)) > maxKeys {
			isTruncated = true
			objectEntities = objectEntities[:maxKeys]
		}
	} else {
		keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(ctx, tx, bucketName, prefix, startAfter)
		if err != nil {
			return nil, err
		}
		isTruncated = int32(*keyCount) > maxKeys
		objectEntities, err = sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx, tx, bucketName, prefix, startAfter)
		if err != nil {
			return nil, err
		}
	}

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
		if int32(len(objects)) < maxKeys {
			var parts []metadatastore.Part = nil
			if !opts.SkipPartFetch {
				// @Perf: Consider optimizing part fetch to reduce database calls
				partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
				if err != nil {
					return nil, err
				}
				for _, partEntity := range partEntities {
					partStruc := metadatastore.Part{
						Id:                partEntity.PartId,
						ETag:              partEntity.ETag,
						ChecksumCRC32:     partEntity.ChecksumCRC32,
						ChecksumCRC32C:    partEntity.ChecksumCRC32C,
						ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
						ChecksumSHA1:      partEntity.ChecksumSHA1,
						ChecksumSHA256:    partEntity.ChecksumSHA256,
						Size:              partEntity.Size,
						StoreName:         partEntity.PartStoreName,
					}
					parts = append(parts, partStruc)
				}
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, metadatastore.Object{
					Key:               objectEntity.Key,
					LastModified:      objectEntity.UpdatedAt,
					VersionID:         objectEntity.VersionID,
					IsDeleteMarker:    objectEntity.IsDeleteMarker,
					ETag:              objectEntity.ETag,
					ChecksumCRC32:     objectEntity.ChecksumCRC32,
					ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
					ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
					ChecksumSHA1:      objectEntity.ChecksumSHA1,
					ChecksumSHA256:    objectEntity.ChecksumSHA256,
					ChecksumType:      objectEntity.ChecksumType,
					Size:              objectEntity.Size,
					StorageClass:      objectEntity.StorageClass,
					Parts:             parts,
				})
				listedObjectIds = append(listedObjectIds, *objectEntity.Id)
			}
		}
	}

	// Load the tag sets of the whole page in one query, so per-object consumers
	// (e.g. tag-based list filtering in the authorizer) don't need a storage
	// lookup per key. Every listed object gets a non-nil map.
	tagEntities, err := sms.tagRepository.FindTagsByObjectIdsOrderByObjectIdAndKeyAsc(ctx, tx, listedObjectIds)
	if err != nil {
		return nil, err
	}
	tagsByObjectId := map[ulid.ULID]map[string]string{}
	for _, tagEntity := range tagEntities {
		if tagsByObjectId[tagEntity.ObjectId] == nil {
			tagsByObjectId[tagEntity.ObjectId] = map[string]string{}
		}
		tagsByObjectId[tagEntity.ObjectId][tagEntity.Key] = tagEntity.Value
	}
	for i, objectId := range listedObjectIds {
		tags := tagsByObjectId[objectId]
		if tags == nil {
			tags = map[string]string{}
		}
		objects[i].Tags = tags
	}

	listBucketResult := metadatastore.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}
	return &listBucketResult, nil
}

func (sms *sqlMetadataStore) ListObjects(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectsOptions) (*metadatastore.ListBucketResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListObjects")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	return sms.listObjects(ctx, tx, bucketName, opts)
}

func (sms *sqlMetadataStore) ListObjectVersions(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectVersionsOptions) (*metadatastore.ListObjectVersionsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListObjectVersions")
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
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}
	keyMarker := ""
	if opts.KeyMarker != nil {
		keyMarker = *opts.KeyMarker
	}
	versionIDMarker := ""
	if opts.VersionIDMarker != nil {
		versionIDMarker = *opts.VersionIDMarker
	}

	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	var entities []object.Entity
	if delimiter == "" {
		// Without a delimiter every row is emitted, so one extra row is enough
		// to detect truncation and the fetch can be bounded in SQL.
		entities, err = sms.objectRepository.FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimit(ctx, tx, bucketName, prefix, keyMarker, versionIDMarker, maxKeys+1)
	} else {
		entities, err = sms.objectRepository.FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDesc(ctx, tx, bucketName, prefix, keyMarker, versionIDMarker)
	}
	if err != nil {
		return nil, err
	}

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	versions := []metadatastore.ObjectVersion{}
	isTruncated := false
	emittedCount := int32(0)
	var lastReturnedKey *string
	var lastReturnedVersionID *string
	var nextKeyMarker *string
	var nextVersionIDMarker *string

	for _, entity := range entities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, entity.Key.String(), delimiter)
			if commonPrefix != nil {
				if _, exists := commonPrefixSet[*commonPrefix]; exists {
					// The entity is represented by an already-emitted common
					// prefix; advance the continuation markers past it so the
					// next page does not re-emit the same prefix.
					k := entity.Key.String()
					lastReturnedKey = &k
					lastReturnedVersionID = entity.VersionID
					continue
				}
				if emittedCount >= maxKeys {
					isTruncated = true
					break
				}
				commonPrefixSet[*commonPrefix] = struct{}{}
				commonPrefixes = append(commonPrefixes, *commonPrefix)
				emittedCount++
				k := entity.Key.String()
				lastReturnedKey = &k
				lastReturnedVersionID = entity.VersionID
				continue
			}
		}

		if emittedCount >= maxKeys {
			isTruncated = true
			break
		}

		keyWithoutPrefix := strings.TrimPrefix(entity.Key.String(), prefix)
		if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
			versionID := ""
			if entity.VersionID != nil {
				versionID = *entity.VersionID
			}
			versions = append(versions, metadatastore.ObjectVersion{
				Key:            entity.Key,
				VersionID:      versionID,
				IsDeleteMarker: entity.IsDeleteMarker,
				IsLatest:       entity.IsLatest,
				LastModified:   entity.UpdatedAt,
				Size:           entity.Size,
				ETag:           &entity.ETag,
				StorageClass:   entity.StorageClass,
			})
			emittedCount++
			nextKey := entity.Key.String()
			lastReturnedKey = &nextKey
			lastReturnedVersionID = entity.VersionID
		}
	}

	if isTruncated {
		nextKeyMarker = lastReturnedKey
		nextVersionIDMarker = lastReturnedVersionID
	}

	return &metadatastore.ListObjectVersionsResult{
		Versions:            versions,
		CommonPrefixes:      commonPrefixes,
		IsTruncated:         isTruncated,
		NextKeyMarker:       nextKeyMarker,
		NextVersionIDMarker: nextVersionIDMarker,
	}, nil
}

func (sms *sqlMetadataStore) HeadObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) (*metadatastore.Object, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadObject")
	defer span.End()

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
	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	parts := sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
		return metadatastore.Part{
			Id:                partEntity.PartId,
			ETag:              partEntity.ETag,
			ChecksumCRC32:     partEntity.ChecksumCRC32,
			ChecksumCRC32C:    partEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      partEntity.ChecksumSHA1,
			ChecksumSHA256:    partEntity.ChecksumSHA256,
			Size:              partEntity.Size,
			StoreName:         partEntity.PartStoreName,
		}
	}, partEntities)

	tags, err := sms.loadObjectTags(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	metadata := systemMetadataFromEntity(objectEntity)
	metadata.UserMetadata, err = sms.loadObjectUserMetadata(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.Object{
		Key:               key,
		ContentType:       objectEntity.ContentType,
		LastModified:      objectEntity.UpdatedAt,
		VersionID:         objectEntity.VersionID,
		IsDeleteMarker:    objectEntity.IsDeleteMarker,
		ETag:              objectEntity.ETag,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
		Size:              objectEntity.Size,
		StorageClass:      objectEntity.StorageClass,
		Parts:             parts,
		Tags:              tags,
		Metadata:          metadata,
	}, nil
}

func (sms *sqlMetadataStore) HeadObjectVersion(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, versionID string) (*metadatastore.Object, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadObjectVersion")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, versionID)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	parts := []metadatastore.Part{}
	if !objectEntity.IsDeleteMarker {
		partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
		if err != nil {
			return nil, err
		}
		parts = sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
			return metadatastore.Part{Id: partEntity.PartId, ETag: partEntity.ETag, ChecksumCRC32: partEntity.ChecksumCRC32, ChecksumCRC32C: partEntity.ChecksumCRC32C, ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME, ChecksumSHA1: partEntity.ChecksumSHA1, ChecksumSHA256: partEntity.ChecksumSHA256, Size: partEntity.Size, StoreName: partEntity.PartStoreName}
		}, partEntities)
	}

	tags, err := sms.loadObjectTags(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	metadata := systemMetadataFromEntity(objectEntity)
	metadata.UserMetadata, err = sms.loadObjectUserMetadata(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.Object{Key: key, ContentType: objectEntity.ContentType, LastModified: objectEntity.UpdatedAt, VersionID: objectEntity.VersionID, IsDeleteMarker: objectEntity.IsDeleteMarker, ETag: objectEntity.ETag, ChecksumCRC32: objectEntity.ChecksumCRC32, ChecksumCRC32C: objectEntity.ChecksumCRC32C, ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME, ChecksumSHA1: objectEntity.ChecksumSHA1, ChecksumSHA256: objectEntity.ChecksumSHA256, ChecksumType: objectEntity.ChecksumType, Size: objectEntity.Size, StorageClass: objectEntity.StorageClass, Parts: parts, Tags: tags, Metadata: metadata}, nil
}
