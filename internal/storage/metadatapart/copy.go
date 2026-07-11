package metadatapart

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func (mbs *metadataPartStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CopyObject")
	defer span.End()

	var result storage.CopyObjectResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var srcObject *metadatastore.Object
		var err error
		if opts != nil && opts.SourceVersionID != nil {
			srcObject, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), srcBucket, srcKey, *opts.SourceVersionID)
		} else {
			srcObject, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), srcBucket, srcKey)
		}
		if err != nil {
			return err
		}
		if srcObject.IsDeleteMarker {
			versionID := ""
			if srcObject.VersionID != nil {
				versionID = *srcObject.VersionID
			}
			if opts != nil && opts.SourceVersionID != nil {
				return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: srcObject.LastModified}
			}
			return &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}
		if !objectPartManifestComplete(srcObject) {
			return storage.ErrNoSuchKey
		}

		if opts != nil {
			if err := evaluateCopySourceConditions(opts.CopySourceConditions, srcObject); err != nil {
				return err
			}
		}

		lastModified := time.Now()
		dstObject := metadatastore.Object{
			Key:          dstKey,
			LastModified: lastModified,
		}
		if opts != nil {
			// The destination class comes from the copy request only; the
			// source's class is never carried over (matching AWS).
			dstObject.StorageClass = opts.StorageClass
		}
		dstStoreName, dstStore := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(dstObject.StorageClass))

		if opts != nil && opts.Range != nil {
			// Ranged copy: produce a single-part destination object containing the
			// requested byte range, with a freshly computed ETag.
			normalizedRanges, err := normalizeAndValidateRanges([]storage.ByteRange{*opts.Range}, srcObject.Size)
			if err != nil {
				return err
			}
			rangeReader, err := mbs.createRangeReader(ctx, tx, srcObject, normalizedRanges[0])
			if err != nil {
				return err
			}
			defer rangeReader.Close()

			newPartId, err := partstore.NewRandomPartId()
			if err != nil {
				return err
			}
			size, checksums, err := checksumutils.CalculateChecksumsStreaming(ctx, rangeReader, func(r io.Reader) error {
				return dstStore.PutPart(ctx, tx, *newPartId, r)
			})
			if err != nil {
				return err
			}
			dedupedPartID, refPreAcquired, err := mbs.dedupeFreshPart(ctx, tx, dstStoreName, dstStore, *newPartId, checksums, *size)
			if err != nil {
				return err
			}
			dstObject.ETag = *checksums.ETag
			dstObject.ChecksumCRC32 = checksums.ChecksumCRC32
			dstObject.ChecksumCRC32C = checksums.ChecksumCRC32C
			dstObject.ChecksumCRC64NVME = checksums.ChecksumCRC64NVME
			dstObject.ChecksumSHA1 = checksums.ChecksumSHA1
			dstObject.ChecksumSHA256 = checksums.ChecksumSHA256
			dstObject.ChecksumType = ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject)
			dstObject.Size = *size
			dstObject.Parts = []metadatastore.Part{
				{
					Id:                dedupedPartID,
					ETag:              *checksums.ETag,
					ChecksumCRC32:     checksums.ChecksumCRC32,
					ChecksumCRC32C:    checksums.ChecksumCRC32C,
					ChecksumCRC64NVME: checksums.ChecksumCRC64NVME,
					ChecksumSHA1:      checksums.ChecksumSHA1,
					ChecksumSHA256:    checksums.ChecksumSHA256,
					Size:              *size,
					StoreName:         dstStoreName,
					RefPreAcquired:    refPreAcquired,
				},
			}
		} else {
			// Full copies within one physical store share source parts. Cross-store
			// parts are copied because a part id belongs to exactly one store.
			newParts := make([]metadatastore.Part, len(srcObject.Parts))
			sharedPartIDs := make([]partstore.PartId, 0, len(srcObject.Parts))
			for i, srcPart := range srcObject.Parts {
				if partStoreNamesEqual(srcPart.StoreName, dstStoreName) {
					newParts[i] = srcPart
					newParts[i].RefPreAcquired = true
					sharedPartIDs = append(sharedPartIDs, srcPart.Id)
					continue
				}
				// The source part's stored checksums can identify identical
				// content already in the destination store without reading
				// any bytes.
				dedupEntry, canDedup := dedupEntryForPartMetadata(dstStoreName, srcPart)
				if canDedup {
					sharedID, err := mbs.tryShareDedupPart(ctx, tx, dedupEntry)
					if err != nil {
						return err
					}
					if sharedID != nil {
						newParts[i] = srcPart
						newParts[i].Id = *sharedID
						newParts[i].StoreName = dstStoreName
						newParts[i].RefPreAcquired = true
						continue
					}
				}
				newPartId, err := partstore.NewRandomPartId()
				if err != nil {
					return err
				}
				srcStore, err := mbs.partStores.ByName(srcPart.StoreName)
				if err != nil {
					return err
				}
				srcReader, err := srcStore.GetPart(ctx, tx, srcPart.Id)
				if err != nil {
					return err
				}
				err = dstStore.PutPart(ctx, tx, *newPartId, srcReader)
				srcReader.Close()
				if err != nil {
					return err
				}
				newParts[i] = srcPart
				newParts[i].Id = *newPartId
				newParts[i].StoreName = dstStoreName
				if canDedup {
					dedupEntry.PartId = *newPartId
					if _, err := mbs.metadataStore.TryIndexDedupPart(ctx, tx.SqlTx(), dedupEntry); err != nil {
						return err
					}
				}
			}
			if len(sharedPartIDs) > 0 {
				added, err := mbs.metadataStore.TryAddPartReferences(ctx, tx.SqlTx(), sharedPartIDs)
				if err != nil {
					return err
				}
				if !added {
					return storage.ErrNoSuchKey
				}
			}
			dstObject.ETag = srcObject.ETag
			dstObject.ChecksumCRC32 = srcObject.ChecksumCRC32
			dstObject.ChecksumCRC32C = srcObject.ChecksumCRC32C
			dstObject.ChecksumCRC64NVME = srcObject.ChecksumCRC64NVME
			dstObject.ChecksumSHA1 = srcObject.ChecksumSHA1
			dstObject.ChecksumSHA256 = srcObject.ChecksumSHA256
			dstObject.ChecksumType = srcObject.ChecksumType
			dstObject.Size = srcObject.Size
			dstObject.Parts = newParts
		}

		// Content type and metadata follow the metadata directive.
		if opts != nil && opts.ReplaceMetadata {
			dstObject.ContentType = opts.ContentType
			if opts.Metadata != nil {
				dstObject.Metadata = *opts.Metadata
			}
		} else {
			dstObject.ContentType = srcObject.ContentType
			dstObject.Metadata = srcObject.Metadata
			// S3 never carries x-amz-website-redirect-location over from the
			// source; it applies only when supplied on the copy request itself.
			dstObject.Metadata.WebsiteRedirectLocation = nil
			if opts != nil && opts.Metadata != nil {
				dstObject.Metadata.WebsiteRedirectLocation = opts.Metadata.WebsiteRedirectLocation
			}
		}

		// Tags follow the tagging directive: REPLACE uses the supplied tag set,
		// COPY (the default) carries over the source object's tags.
		if opts != nil && opts.ReplaceTags {
			dstObject.Tags = opts.Tags
		} else {
			dstObject.Tags = srcObject.Tags
		}

		metadataResult, err := mbs.metadataStore.PutObject(ctx, tx.SqlTx(), dstBucket, &dstObject, nil)
		if err != nil {
			return err
		}
		if err := mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts); err != nil {
			return err
		}

		result = storage.CopyObjectResult{ETag: dstObject.ETag, LastModified: lastModified, SourceVersionID: srcObject.VersionID, VersionID: dstObject.VersionID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
