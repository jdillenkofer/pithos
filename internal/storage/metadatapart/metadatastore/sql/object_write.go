package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByNameForShare(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	effectiveLock, err := metadatastore.EffectiveObjectLock(lockConfigurationFromBucket(bucketEntity), obj.ObjectLock, time.Now().UTC())
	if err != nil {
		return err
	}
	if metadatastore.IsChecksumlessPut(ctx) && effectiveLock.Retention != nil {
		return metadatastore.ErrObjectLockChecksumRequired
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)

	latestObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	objectExists := latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker
	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			if !objectExists {
				return metadatastore.ErrPreconditionFailed
			}
		} else if !objectExists || latestObjectEntity.ETag != *opts.IfMatchETag {
			return metadatastore.ErrPreconditionFailed
		}
	}
	if opts != nil && opts.IfNoneMatchStar && objectExists {
		return metadatastore.ErrPreconditionFailed
	}
	if opts != nil && opts.IfNoneMatchStar {
		latestObjectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
		if err != nil {
			return err
		}
		if latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker {
			return metadatastore.ErrPreconditionFailed
		}
	}

	if opts != nil && (opts.IfMatchETag != nil || opts.IfNoneMatchStar) && latestObjectEntity != nil {
		lockedObjectEntity := *latestObjectEntity
		locked, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, latestObjectEntity.OptimisticLockVersion)
		if err != nil {
			return err
		}
		if !*locked {
			return metadatastore.ErrPreconditionFailed
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
				return err
			}
		}
		if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
			if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
				return metadatastore.ErrPreconditionFailed
			}
			return err
		}
	} else {
		nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
		if err != nil {
			return err
		}
		if opts != nil && opts.IfNoneMatchStar && nullVersionEntity != nil {
			return metadatastore.ErrPreconditionFailed
		}
		nullVersion := "null"
		objectEntity.VersionID = &nullVersion

		if latestObjectEntity != nil {
			latestObjectEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity); err != nil {
				return err
			}
		}

		if nullVersionEntity != nil {
			if err := sms.checkVersionDeletion(ctx, tx, nullVersionEntity, bucketEntity.ObjectLockEnabled, false); err != nil {
				return err
			}
			objectEntity.Id = nullVersionEntity.Id
			objectEntity.OptimisticLockVersion = nullVersionEntity.OptimisticLockVersion
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return metadatastore.ErrPreconditionFailed
				}
				return err
			}
			if err := sms.removePartRowsByObjectId(ctx, tx, *objectEntity.Id); err != nil {
				return err
			}
		} else {
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return metadatastore.ErrPreconditionFailed
				}
				return err
			}
		}
	}

	if effectiveLock.Retention != nil || effectiveLock.LegalHold != nil {
		if err := sms.saveObjectLock(ctx, tx, *objectEntity.Id, effectiveLock); err != nil {
			return err
		}
	}
	obj.ObjectLock = effectiveLock
	objectId := objectEntity.Id
	if err = sms.savePartRows(ctx, tx, *objectId, obj.Parts, 0); err != nil {
		return err
	}

	obj.VersionID = objectEntity.VersionID
	metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{VersionID: obj.VersionID, Effective: effectiveLock})

	// PutObject replaces the object entirely, so its tag set and user metadata
	// are replaced with the values supplied on the new object (empty when none
	// were provided).
	if err := sms.replaceObjectTags(ctx, tx, *objectId, obj.Tags); err != nil {
		return err
	}
	if err := sms.replaceObjectUserMetadata(ctx, tx, *objectId, obj.Metadata.UserMetadata); err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) AppendObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.AppendObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AppendObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByNameForShare(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)
	if versioningEnabled {
		if opts != nil {
			obj.ObjectLock = opts.ObjectLock
		}
		return sms.PutObject(ctx, tx, bucketName, obj, nil)
	}

	// Check whether an object already exists at this key.
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	if oldObjectEntity != nil {
		if err := sms.checkVersionDeletion(ctx, tx, oldObjectEntity, bucketEntity.ObjectLockEnabled, false); err != nil {
			return err
		}
		existingParts, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return err
		}
		if len(obj.Parts) < len(existingParts) {
			return fmt.Errorf("append part list lost existing parts")
		}
		for i, existingPart := range existingParts {
			if obj.Parts[i].Id != existingPart.PartId {
				return fmt.Errorf("append part prefix mismatch at sequence %d", i)
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
			return err
		}
		if !*updated {
			return metadatastore.ErrCASFailure
		}
		if err = sms.savePartRows(ctx, tx, *updatedEntity.Id, obj.Parts[len(existingParts):], len(existingParts)); err != nil {
			return err
		}
		return nil
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
		return err
	}
	if err = sms.savePartRows(ctx, tx, *newEntity.Id, obj.Parts, 0); err != nil {
		return err
	}
	return nil
}

func (sms *sqlMetadataStore) TransitionObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, versionID *string, expectedETag string, storageClass string, parts []metadatastore.Part) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.TransitionObject")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	var objectEntity *object.Entity
	if versionID != nil {
		objectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, *versionID)
	} else {
		objectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	}
	if err != nil {
		return err
	}
	if objectEntity == nil || objectEntity.IsDeleteMarker {
		return metadatastore.ErrNoSuchKey
	}
	if objectEntity.ETag != expectedETag {
		return metadatastore.ErrPreconditionFailed
	}

	objectEntity.StorageClass = &storageClass
	// Guard against a concurrent replacement between the read and the update.
	updated, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, objectEntity, objectEntity.OptimisticLockVersion)
	if err != nil {
		return err
	}
	if !*updated {
		return metadatastore.ErrPreconditionFailed
	}

	// Replace the part rows with the transitioned parts. Relocated parts carry
	// new ids in the target store; parts that stayed in place keep their ids
	// and arrive with a pre-acquired registry reference, so the removal below
	// cannot condemn them.
	if err := sms.removePartRowsByObjectId(ctx, tx, *objectEntity.Id); err != nil {
		return err
	}
	return sms.savePartRows(ctx, tx, *objectEntity.Id, parts, 0)
}
