package metadatapart

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func (mbs *metadataPartStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteObject")
	defer span.End()

	var result *storage.DeleteObjectResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}

		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled
		versioningSuspended := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusSuspended

		if opts != nil && opts.VersionID != nil {
			_, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else if versioningSuspended {
			_, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, "null")
		} else {
			_, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			if err == storage.ErrNoSuchKey && (opts == nil || opts.VersionID == nil) && (versioningEnabled || versioningSuspended) {
				// Enabled/suspended key-only delete of a missing (or null-version-less)
				// key still creates a delete marker below.
			} else if err == storage.ErrNoSuchKey {
				// Object does not exist.
				if opts != nil && opts.IfMatchETag != nil {
					// Conditional delete: object must exist.
					return storage.ErrPreconditionFailed
				}
				// No condition: silently succeed per S3 semantics.
				result = &storage.DeleteObjectResult{}
				return nil
			} else {
				return err
			}
		}

		var metaOpts *metadatastore.DeleteObjectOptions
		if opts != nil {
			metaOpts = &metadatastore.DeleteObjectOptions{
				VersionID:   opts.VersionID,
				IfMatchETag: opts.IfMatchETag,
			}
		}
		metaResult, err := mbs.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucketName, key, metaOpts)
		if err != nil {
			return err
		}
		if err := mbs.deleteUnreferencedParts(ctx, tx, metaResult.UnreferencedParts); err != nil {
			return err
		}

		result = &storage.DeleteObjectResult{VersionID: metaResult.VersionID, IsDeleteMarker: metaResult.IsDeleteMarker}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func partStoreNamesEqual(a, b *string) bool {
	aName := partstore.DefaultPartStoreName
	if a != nil {
		aName = *a
	}
	bName := partstore.DefaultPartStoreName
	if b != nil {
		bName = *b
	}
	return aName == bName
}

func (mbs *metadataPartStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteObjects")
	defer span.End()

	result := &storage.DeleteObjectsResult{
		Entries: make([]storage.DeleteObjectsEntry, 0, len(entries)),
	}

	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}
		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled
		versioningSuspended := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusSuspended

		for _, entry := range entries {
			var object *metadatastore.Object
			var err error
			if entry.VersionID != nil {
				object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, entry.Key, *entry.VersionID)
			} else if versioningSuspended {
				object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, entry.Key, "null")
			} else {
				object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, entry.Key)
			}
			if err != nil {
				if err == storage.ErrNoSuchKey {
					if entry.IfMatchETag != nil {
						// Object does not exist but ETag condition set → precondition failed for this entry.
						result.Entries = append(result.Entries, storage.DeleteObjectsEntry{
							Key:     entry.Key,
							Deleted: false,
							ErrCode: "PreconditionFailed",
							ErrMsg:  "At least one of the pre-conditions you specified did not hold",
						})
						continue
					}
					if !(entry.VersionID == nil && (versioningEnabled || versioningSuspended)) {
						result.Entries = append(result.Entries, storage.DeleteObjectsEntry{Key: entry.Key, Deleted: true})
						continue
					}
					object = nil
				} else {
					return err
				}
			}

			// Object exists — check conditional ETag if specified.
			if entry.IfMatchETag != nil && (object == nil || object.ETag != *entry.IfMatchETag) {
				result.Entries = append(result.Entries, storage.DeleteObjectsEntry{
					Key:     entry.Key,
					Deleted: false,
					ErrCode: "PreconditionFailed",
					ErrMsg:  "At least one of the pre-conditions you specified did not hold",
				})
				continue
			}

			var metaOpts *metadatastore.DeleteObjectOptions
			if entry.IfMatchETag != nil || entry.VersionID != nil {
				metaOpts = &metadatastore.DeleteObjectOptions{VersionID: entry.VersionID, IfMatchETag: entry.IfMatchETag}
			}
			metaResult, err := mbs.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucketName, entry.Key, metaOpts)
			if err != nil {
				return err
			}
			if err := mbs.deleteUnreferencedParts(ctx, tx, metaResult.UnreferencedParts); err != nil {
				return err
			}

			resultEntry := storage.DeleteObjectsEntry{Key: entry.Key, DeleteMarker: &metaResult.IsDeleteMarker, Deleted: true}
			if metaResult.IsDeleteMarker && entry.VersionID == nil {
				resultEntry.DeleteMarkerVersionID = metaResult.VersionID
			} else {
				resultEntry.VersionID = metaResult.VersionID
			}
			result.Entries = append(result.Entries, resultEntry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
