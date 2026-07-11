package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) (*metadatastore.PartMutationResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObject")
	defer span.End()
	result := metadatastore.PartMutationResult{}

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)

	latestObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return nil, err
	}

	objectExists := latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker
	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			if !objectExists {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else if !objectExists || latestObjectEntity.ETag != *opts.IfMatchETag {
			return nil, metadatastore.ErrPreconditionFailed
		}
	}
	if opts != nil && opts.IfNoneMatchStar && objectExists {
		return nil, metadatastore.ErrPreconditionFailed
	}
	if opts != nil && opts.IfNoneMatchStar {
		latestObjectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
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

	objectEntity := object.Entity{
		BucketName:        bucketName,
		Key:               obj.Key,
		ContentType:       obj.ContentType,
		ETag:              obj.ETag,
		ChecksumCRC32:     obj.ChecksumCRC32,
		ChecksumCRC32C:    obj.ChecksumCRC32C,
		ChecksumCRC64NVME: obj.ChecksumCRC64NVME,
		ChecksumSHA1:      obj.ChecksumSHA1,
		ChecksumSHA256:    obj.ChecksumSHA256,
		ChecksumType:      obj.ChecksumType,
		Size:              obj.Size,
		StorageClass:      obj.StorageClass,
		IsDeleteMarker:    false,
		IsLatest:          true,
		UploadStatus:      object.UploadStatusCompleted,
	}
	applySystemMetadataToEntity(&objectEntity, obj.Metadata)

	if versioningEnabled {
		versionID := metadatastore.NewRandomUploadId().String()
		objectEntity.VersionID = &versionID
		if latestObjectEntity != nil {
			latestObjectEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity); err != nil {
				return nil, err
			}
		}
		if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
			if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
				return nil, metadatastore.ErrPreconditionFailed
			}
			return nil, err
		}
	} else {
		nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
		if err != nil {
			return nil, err
		}
		if opts != nil && opts.IfNoneMatchStar && nullVersionEntity != nil {
			return nil, metadatastore.ErrPreconditionFailed
		}
		nullVersion := "null"
		objectEntity.VersionID = &nullVersion

		if latestObjectEntity != nil {
			latestObjectEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity); err != nil {
				return nil, err
			}
		}

		if nullVersionEntity != nil {
			objectEntity.Id = nullVersionEntity.Id
			objectEntity.OptimisticLockVersion = nullVersionEntity.OptimisticLockVersion
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return nil, metadatastore.ErrPreconditionFailed
				}
				return nil, err
			}
			unreferencedParts, err := sms.removePartRowsByObjectId(ctx, tx, *objectEntity.Id)
			if err != nil {
				return nil, err
			}
			result.UnreferencedParts = append(result.UnreferencedParts, unreferencedParts...)
		} else {
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return nil, metadatastore.ErrPreconditionFailed
				}
				return nil, err
			}
		}
	}

	objectId := objectEntity.Id
	if err = sms.savePartRows(ctx, tx, *objectId, obj.Parts, 0); err != nil {
		return nil, err
	}

	obj.VersionID = objectEntity.VersionID

	// PutObject replaces the object entirely, so its tag set and user metadata
	// are replaced with the values supplied on the new object (empty when none
	// were provided).
	if err := sms.replaceObjectTags(ctx, tx, *objectId, obj.Tags); err != nil {
		return nil, err
	}
	if err := sms.replaceObjectUserMetadata(ctx, tx, *objectId, obj.Metadata.UserMetadata); err != nil {
		return nil, err
	}

	return &result, nil
}

func (sms *sqlMetadataStore) AppendObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.AppendObjectOptions) (*metadatastore.PartMutationResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AppendObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)
	if versioningEnabled {
		return sms.PutObject(ctx, tx, bucketName, obj, nil)
	}

	// Check whether an object already exists at this key.
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return nil, err
	}

	if oldObjectEntity != nil {
		existingParts, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}
		if len(obj.Parts) < len(existingParts) {
			return nil, fmt.Errorf("append part list lost existing parts")
		}
		for i, existingPart := range existingParts {
			if obj.Parts[i].Id != existingPart.PartId {
				return nil, fmt.Errorf("append part prefix mismatch at sequence %d", i)
			}
		}
		updatedEntity := object.Entity{
			Id:             oldObjectEntity.Id,
			BucketName:     bucketName,
			Key:            obj.Key,
			ContentType:    oldObjectEntity.ContentType,
			ETag:           obj.ETag,
			ChecksumType:   obj.ChecksumType,
			Size:           obj.Size,
			StorageClass:   oldObjectEntity.StorageClass,
			VersionID:      oldObjectEntity.VersionID,
			IsLatest:       true,
			IsDeleteMarker: false,
			UploadStatus:   object.UploadStatusCompleted,
		}
		// Appends preserve the object's existing metadata (like its content type).
		applySystemMetadataToEntity(&updatedEntity, systemMetadataFromEntity(oldObjectEntity))
		updated, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &updatedEntity, oldObjectEntity.OptimisticLockVersion)
		if err != nil {
			return nil, err
		}
		if !*updated {
			return nil, metadatastore.ErrCASFailure
		}
		if err = sms.savePartRows(ctx, tx, *updatedEntity.Id, obj.Parts[len(existingParts):], len(existingParts)); err != nil {
			return nil, err
		}
		return &metadatastore.PartMutationResult{}, nil
	}

	// No existing object — create a new one (same semantics as PutObject).
	newEntity := object.Entity{
		BucketName:     bucketName,
		Key:            obj.Key,
		ContentType:    obj.ContentType,
		ETag:           obj.ETag,
		ChecksumType:   obj.ChecksumType,
		Size:           obj.Size,
		StorageClass:   obj.StorageClass,
		VersionID:      ptrutils.ToPtr("null"),
		IsLatest:       true,
		IsDeleteMarker: false,
		UploadStatus:   object.UploadStatusCompleted,
	}
	applySystemMetadataToEntity(&newEntity, obj.Metadata)
	err = sms.objectRepository.SaveObject(ctx, tx, &newEntity)
	if err != nil {
		return nil, err
	}
	if err = sms.savePartRows(ctx, tx, *newEntity.Id, obj.Parts, 0); err != nil {
		return nil, err
	}
	return &metadatastore.PartMutationResult{}, nil
}

func (sms *sqlMetadataStore) TransitionObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, versionID *string, expectedETag string, storageClass string, parts []metadatastore.Part) (*metadatastore.PartMutationResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.TransitionObject")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	var objectEntity *object.Entity
	if versionID != nil {
		objectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, *versionID)
	} else {
		objectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	}
	if err != nil {
		return nil, err
	}
	if objectEntity == nil || objectEntity.IsDeleteMarker {
		return nil, metadatastore.ErrNoSuchKey
	}
	if objectEntity.ETag != expectedETag {
		return nil, metadatastore.ErrPreconditionFailed
	}

	objectEntity.StorageClass = &storageClass
	// Guard against a concurrent replacement between the read and the update.
	updated, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, objectEntity, objectEntity.OptimisticLockVersion)
	if err != nil {
		return nil, err
	}
	if !*updated {
		return nil, metadatastore.ErrPreconditionFailed
	}

	// Replace the part rows with the transitioned parts. Relocated parts carry
	// new ids in the target store; parts that stayed in place keep their ids
	// and arrive with a pre-acquired registry reference, so the removal below
	// cannot condemn them.
	unreferencedParts, err := sms.removePartRowsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	if err := sms.savePartRows(ctx, tx, *objectEntity.Id, parts, 0); err != nil {
		return nil, err
	}
	return &metadatastore.PartMutationResult{UnreferencedParts: unreferencedParts}, nil
}
