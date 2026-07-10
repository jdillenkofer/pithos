package metadatapart

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func (mbs *metadataPartStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutObject")
	defer span.End()
	var object metadatastore.Object
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		ifNoneMatchStar := opts != nil && opts.IfNoneMatchStar

		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		var requestedStorageClass *string
		if opts != nil {
			requestedStorageClass = opts.StorageClass
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(requestedStorageClass))

		originalSize, calculatedChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
			return store.PutPart(ctx, tx, *partId, reader)
		})
		if err != nil {
			return err
		}

		err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
		if err != nil {
			return err
		}

		object = metadatastore.Object{
			Key:               key,
			ContentType:       contentType,
			LastModified:      time.Now(),
			ETag:              *calculatedChecksums.ETag,
			ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
			ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
			ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
			ChecksumType:      ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject),
			Size:              *originalSize,
			Parts: []metadatastore.Part{
				{
					Id:                *partId,
					ETag:              *calculatedChecksums.ETag,
					ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
					ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
					ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
					ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
					ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
					Size:              *originalSize,
					StoreName:         storeName,
				},
			},
		}
		if opts != nil {
			object.Tags = opts.Tags
			if opts.Metadata != nil {
				object.Metadata = *opts.Metadata
			}
			object.StorageClass = opts.StorageClass
		}

		metadataPutObjectOptions := &metadatastore.PutObjectOptions{IfNoneMatchStar: ifNoneMatchStar}
		if opts != nil {
			metadataPutObjectOptions.IfMatchETag = opts.IfMatchETag
		}
		metadataResult, err := mbs.metadataStore.PutObject(ctx, tx.SqlTx(), bucketName, &object, metadataPutObjectOptions)
		if err != nil {
			return err
		}
		return mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts)
	})
	if err != nil {
		return nil, err
	}

	return &storage.PutObjectResult{
		VersionID:         object.VersionID,
		ETag:              &object.ETag,
		ChecksumCRC32:     object.ChecksumCRC32,
		ChecksumCRC32C:    object.ChecksumCRC32C,
		ChecksumCRC64NVME: object.ChecksumCRC64NVME,
		ChecksumSHA1:      object.ChecksumSHA1,
		ChecksumSHA256:    object.ChecksumSHA256,
	}, nil
}

func (mbs *metadataPartStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.AppendObject")
	defer span.End()

	var combinedChecksums checksumutils.ChecksumValues
	var totalSize int64
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}
		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled

		// Fetch the existing object (if any).
		existingObject, err := mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		var currentDeleteMarkerErr *storage.CurrentDeleteMarkerError
		if err != nil && !errors.As(err, &currentDeleteMarkerErr) && err != storage.ErrNoSuchKey {
			return err
		}
		if currentDeleteMarkerErr != nil {
			existingObject = nil
		}

		// Validate WriteOffset condition.
		if opts != nil && opts.WriteOffset != nil {
			if existingObject == nil {
				// Object does not exist — only allowed when offset == 0.
				if *opts.WriteOffset != 0 {
					return storage.ErrInvalidWriteOffset
				}
			} else {
				// Object exists — offset must equal current size.
				if *opts.WriteOffset != existingObject.Size {
					return storage.ErrInvalidWriteOffset
				}
			}
		}

		// Write the new part's bytes. Appends keep the existing object's
		// storage class, so new data routes to the store of that class.
		var existingStorageClass *string
		if existingObject != nil {
			existingStorageClass = existingObject.StorageClass
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(existingStorageClass))

		newPartId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		newPartSize, newPartChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(r io.Reader) error {
			return store.PutPart(ctx, tx, *newPartId, r)
		})
		if err != nil {
			return err
		}

		// Validate checksums for the new data chunk (if provided by the caller).
		if err = metadatastore.ValidateChecksums(checksumInput, *newPartChecksums); err != nil {
			return err
		}

		newPart := metadatastore.Part{
			Id:                *newPartId,
			ETag:              *newPartChecksums.ETag,
			ChecksumCRC32:     newPartChecksums.ChecksumCRC32,
			ChecksumCRC32C:    newPartChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: newPartChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      newPartChecksums.ChecksumSHA1,
			ChecksumSHA256:    newPartChecksums.ChecksumSHA256,
			Size:              *newPartSize,
			StoreName:         storeName,
		}

		// Build the combined part list (existing parts first, then new part).
		var allParts []metadatastore.Part
		totalSize = 0
		if existingObject != nil {
			totalSize = existingObject.Size
		}

		// S3 enforces a maximum of 10,000 parts per object.
		const maxAppendParts = 10_000
		existingPartCount := 0
		if existingObject != nil {
			existingPartCount = len(existingObject.Parts)
		}
		if existingPartCount+1 > maxAppendParts {
			return storage.ErrTooManyParts
		}

		if existingObject != nil {
			if versioningEnabled {
				// Versioned append creates a new object version, so existing parts need
				// fresh IDs instead of being reused in the new version metadata.
				allParts = make([]metadatastore.Part, 0, len(existingObject.Parts)+1)
				for _, existingPart := range existingObject.Parts {
					existingPartStore, partErr := mbs.partStores.ByName(existingPart.StoreName)
					if partErr != nil {
						return partErr
					}
					existingPartReader, partErr := existingPartStore.GetPart(ctx, tx, existingPart.Id)
					if partErr != nil {
						return partErr
					}

					newExistingPartID, partErr := partstore.NewRandomPartId()
					if partErr != nil {
						existingPartReader.Close()
						return partErr
					}

					partErr = existingPartStore.PutPart(ctx, tx, *newExistingPartID, existingPartReader)
					existingPartReader.Close()
					if partErr != nil {
						return partErr
					}

					clonedPart := existingPart
					clonedPart.Id = *newExistingPartID
					allParts = append(allParts, clonedPart)
				}
			} else {
				allParts = append(allParts, existingObject.Parts...)
			}
		}

		allParts = append(allParts, newPart)
		totalSize += *newPartSize

		// Compute the whole-object ETag as MD5-of-part-ETags (multipart-style).
		partChecksums := make([]checksumutils.PartChecksums, len(allParts))
		for i, p := range allParts {
			partChecksums[i] = checksumutils.PartChecksums{
				ETag: p.ETag,
				Size: p.Size,
			}
		}
		combinedChecksums, err = checksumutils.CalculateMultipartChecksums(partChecksums, checksumutils.ChecksumTypeFullObject)
		if err != nil {
			return err
		}

		// Determine content type: preserve existing or fall back to nil (unchanged).
		var contentType *string
		if existingObject != nil {
			contentType = existingObject.ContentType
		}

		updatedObject := &metadatastore.Object{
			Key:          key,
			ContentType:  contentType,
			LastModified: time.Now(),
			ETag:         *combinedChecksums.ETag,
			ChecksumType: ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject),
			Size:         totalSize,
			Parts:        allParts,
		}

		metaOpts := &metadatastore.AppendObjectOptions{}
		metadataResult, err := mbs.metadataStore.AppendObject(ctx, tx.SqlTx(), bucketName, updatedObject, metaOpts)
		if err != nil {
			// The sql layer uses a CAS (DELETE WHERE id=X AND etag=Y) to detect a
			// concurrent write that changed the object between our HeadObject read
			// and this update. It surfaces that as ErrCASFailure. From the caller's
			// perspective the object size moved under them, so we normalise the
			// error to ErrInvalidWriteOffset (HTTP 400).
			if err == storage.ErrCASFailure {
				return storage.ErrInvalidWriteOffset
			}
			return err
		}
		return mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts)
	})
	if err != nil {
		return nil, err
	}

	return &storage.AppendObjectResult{
		ETag: *combinedChecksums.ETag,
		Size: totalSize,
	}, nil
}

func (mbs *metadataPartStorage) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.TransitionObjectStorageClass")
	defer span.End()

	if !metadatastore.IsValidStorageClass(targetStorageClass) {
		return storage.ErrInvalidStorageClass
	}

	// Source parts are deleted inline in the same transaction as the metadata
	// swap (crash-safe: the delete is deferred to commit and rolled back on
	// abort, so a crash mid-transition leaves the source intact). GC remains a
	// safety net for target parts orphaned by a crash between copy and commit.

	targetStoreName, targetStore := mbs.partStores.StoreForClass(targetStorageClass)

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var object *metadatastore.Object
		var err error
		var versionID *string
		if opts != nil {
			versionID = opts.VersionID
		}
		if versionID != nil {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *versionID)
		} else {
			object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			return err
		}
		if object.IsDeleteMarker {
			return storage.ErrNoSuchKey
		}
		if opts != nil && opts.IfMatchETag != nil {
			if *opts.IfMatchETag != storage.ETagWildcard && object.ETag != *opts.IfMatchETag {
				return storage.ErrPreconditionFailed
			}
		}

		// When every part already lives in the target store (e.g. the class was
		// only remapped in config, or the classes share a store), just relabel
		// the object without moving any data.
		needsMove := false
		for _, part := range object.Parts {
			if !partStoreNamesEqual(part.StoreName, targetStoreName) {
				needsMove = true
				break
			}
		}

		newParts := object.Parts
		if needsMove {
			newParts = make([]metadatastore.Part, len(object.Parts))
			for i, srcPart := range object.Parts {
				srcStore, err := mbs.partStores.ByName(srcPart.StoreName)
				if err != nil {
					return err
				}
				newPartId, err := partstore.NewRandomPartId()
				if err != nil {
					return err
				}
				srcReader, err := srcStore.GetPart(ctx, tx, srcPart.Id)
				if err != nil {
					return err
				}
				err = targetStore.PutPart(ctx, tx, *newPartId, srcReader)
				srcReader.Close()
				if err != nil {
					return err
				}
				newParts[i] = srcPart
				newParts[i].Id = *newPartId
				newParts[i].StoreName = targetStoreName
			}
		}

		metadataResult, err := mbs.metadataStore.TransitionObject(ctx, tx.SqlTx(), bucketName, key, versionID, object.ETag, targetStorageClass, newParts)
		if err != nil {
			return err
		}
		return mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts)
	})
}

// partStoreNamesEqual compares two part store names where nil means the default
// store.
