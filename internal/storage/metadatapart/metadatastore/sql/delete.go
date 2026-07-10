package sql

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.DeleteObjectOptions) (*metadatastore.DeleteObjectResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteObject")
	defer span.End()
	unreferencedParts := []metadatastore.Part{}

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	versioningStatus := ""
	if bucketEntity.VersioningStatus != nil {
		versioningStatus = *bucketEntity.VersioningStatus
	}

	currentEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}

	if opts != nil && opts.VersionID != nil {
		versionEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, *opts.VersionID)
		if err != nil {
			return nil, err
		}
		if versionEntity == nil {
			return &metadatastore.DeleteObjectResult{VersionID: opts.VersionID}, nil
		}

		if opts.IfMatchETag != nil {
			if *opts.IfMatchETag == metadatastore.ETagWildcard {
				// any existing version matches
			} else if versionEntity.IsDeleteMarker || versionEntity.ETag != *opts.IfMatchETag {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}

		if !versionEntity.IsDeleteMarker {
			removed, removeErr := sms.removePartRowsByObjectId(ctx, tx, *versionEntity.Id)
			err = removeErr
			if err != nil {
				return nil, err
			}
			unreferencedParts = append(unreferencedParts, removed...)
		}

		err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		if versionEntity.IsLatest {
			nextLatest, err := sms.objectRepository.FindLatestObjectByBucketNameAndKeyExcludingID(ctx, tx, bucketName, key, *versionEntity.Id)
			if err != nil {
				return nil, err
			}
			if nextLatest != nil {
				nextLatest.IsLatest = true
				if err := sms.objectRepository.SaveObject(ctx, tx, nextLatest); err != nil {
					return nil, err
				}
			}
		}

		return &metadatastore.DeleteObjectResult{VersionID: versionEntity.VersionID, IsDeleteMarker: versionEntity.IsDeleteMarker, UnreferencedParts: unreferencedParts}, nil
	}

	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			if currentEntity == nil || currentEntity.IsDeleteMarker {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else {
			if currentEntity == nil || currentEntity.IsDeleteMarker || currentEntity.ETag != *opts.IfMatchETag {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}
	}

	if versioningStatus == string(metadatastore.BucketVersioningStatusEnabled) || versioningStatus == string(metadatastore.BucketVersioningStatusSuspended) {
		if versioningStatus == string(metadatastore.BucketVersioningStatusSuspended) {
			nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, key)
			if err != nil {
				return nil, err
			}
			if nullVersionEntity != nil {
				removed, removeErr := sms.removePartRowsByObjectId(ctx, tx, *nullVersionEntity.Id)
				if removeErr != nil {
					return nil, removeErr
				}
				unreferencedParts = append(unreferencedParts, removed...)
				err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}

				err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}

				_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}
			}
		}

		if currentEntity != nil {
			currentEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, currentEntity); err != nil {
				return nil, err
			}
		}
		deleteMarkerVersionID := metadatastore.NewRandomUploadId().String()
		deleteMarker := object.Entity{
			BucketName:     bucketName,
			Key:            key,
			ETag:           "",
			Size:           0,
			VersionID:      &deleteMarkerVersionID,
			IsDeleteMarker: true,
			IsLatest:       true,
			UploadStatus:   object.UploadStatusCompleted,
		}
		if err := sms.objectRepository.SaveObject(ctx, tx, &deleteMarker); err != nil {
			return nil, err
		}
		return &metadatastore.DeleteObjectResult{VersionID: deleteMarker.VersionID, IsDeleteMarker: true, UnreferencedParts: unreferencedParts}, nil
	}

	if currentEntity != nil {
		if opts != nil && opts.IfMatchETag != nil {
			lockedObjectEntity := *currentEntity
			locked, lockErr := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, currentEntity.OptimisticLockVersion)
			if lockErr != nil {
				return nil, lockErr
			}
			if !*locked {
				return nil, metadatastore.ErrPreconditionFailed
			}

			if !lockedObjectEntity.IsDeleteMarker {
				removed, removeErr := sms.removePartRowsByObjectId(ctx, tx, *lockedObjectEntity.Id)
				err = removeErr
				if err != nil {
					return nil, err
				}
				unreferencedParts = append(unreferencedParts, removed...)
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *lockedObjectEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *lockedObjectEntity.Id)
			if err != nil {
				return nil, err
			}

			deleted, deleteErr := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *lockedObjectEntity.Id, lockedObjectEntity.OptimisticLockVersion)
			if deleteErr != nil {
				return nil, deleteErr
			}
			if !*deleted {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else {
			if !currentEntity.IsDeleteMarker {
				removed, removeErr := sms.removePartRowsByObjectId(ctx, tx, *currentEntity.Id)
				err = removeErr
				if err != nil {
					return nil, err
				}
				unreferencedParts = append(unreferencedParts, removed...)
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}

			_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	return &metadatastore.DeleteObjectResult{UnreferencedParts: unreferencedParts}, nil
}
