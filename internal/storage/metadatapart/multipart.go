package metadatapart

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func convertInitiateMultipartUploadResult(result metadatastore.InitiateMultipartUploadResult) storage.InitiateMultipartUploadResult {
	return storage.InitiateMultipartUploadResult{
		UploadId: result.UploadId,
	}
}

func (mbs *metadataPartStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CreateMultipartUpload")
	defer span.End()

	var metadataOpts *metadatastore.CreateMultipartUploadOptions
	if opts != nil {
		metadataOpts = &metadatastore.CreateMultipartUploadOptions{Tags: opts.Tags, Metadata: opts.Metadata, StorageClass: opts.StorageClass}
	}
	var initiateMultipartUploadResult storage.InitiateMultipartUploadResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		result, err := mbs.metadataStore.CreateMultipartUpload(ctx, tx.SqlTx(), bucketName, key, contentType, checksumType, metadataOpts)
		if err != nil {
			return err
		}
		initiateMultipartUploadResult = convertInitiateMultipartUploadResult(*result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &initiateMultipartUploadResult, nil
}

func (mbs *metadataPartStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.UploadPart")
	defer span.End()

	var calculatedChecksums *checksumutils.ChecksumValues
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		// Parts route to the store of the class chosen at
		// CreateMultipartUpload, so the upload must be resolved before the
		// part bytes are streamed.
		upload, err := mbs.metadataStore.GetMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId)
		if err != nil {
			return err
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(upload.StorageClass))

		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		originalSize, checksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
			return store.PutPart(ctx, tx, *partId, reader)
		})
		if err != nil {
			return err
		}
		calculatedChecksums = checksums

		err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
		if err != nil {
			return err
		}

		metadataResult, err := mbs.metadataStore.UploadPart(ctx, tx.SqlTx(), bucketName, key, uploadId, partNumber, metadatastore.Part{
			Id:                *partId,
			ETag:              *calculatedChecksums.ETag,
			ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
			ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
			ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
			Size:              *originalSize,
			StoreName:         storeName,
		})
		if err != nil {
			return err
		}
		return mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts)
	})
	if err != nil {
		return nil, err
	}
	return &storage.UploadPartResult{
		ETag:              *calculatedChecksums.ETag,
		ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
		ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
		ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
		ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
		ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
	}, nil
}

func findWhollyCoveredPart(parts []metadatastore.Part, byteRange storage.ByteRange, objectSize int64) *metadatastore.Part {
	start := int64(0)
	if byteRange.Start != nil {
		start = *byteRange.Start
	}
	end := objectSize
	if byteRange.End != nil {
		end = *byteRange.End
	}
	offset := int64(0)
	for i := range parts {
		partEnd := offset + parts[i].Size
		if start == offset && end == partEnd {
			return &parts[i]
		}
		offset = partEnd
	}
	return nil
}

func (mbs *metadataPartStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.UploadPartCopy")
	defer span.End()

	var result storage.UploadPartCopyResult
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

		if opts != nil {
			if err := evaluateCopySourceConditions(opts.CopySourceConditions, srcObject); err != nil {
				return err
			}
		}

		// Default to the whole source object when no range is given.
		copyRange := storage.ByteRange{}
		if opts != nil && opts.Range != nil {
			copyRange = *opts.Range
		}
		normalizedRanges, err := normalizeAndValidateRanges([]storage.ByteRange{copyRange}, srcObject.Size)
		if err != nil {
			return err
		}
		// The copied part routes to the store of the class chosen at
		// CreateMultipartUpload for the destination upload.
		upload, err := mbs.metadataStore.GetMultipartUpload(ctx, tx.SqlTx(), dstBucket, dstKey, uploadId)
		if err != nil {
			return err
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(upload.StorageClass))
		coveredPart := findWhollyCoveredPart(srcObject.Parts, normalizedRanges[0], srcObject.Size)
		if coveredPart != nil && coveredPart.ChecksumSHA256 != nil && partStoreNamesEqual(coveredPart.StoreName, storeName) {
			added, err := mbs.metadataStore.TryAddPartReferences(ctx, tx.SqlTx(), []partstore.PartId{coveredPart.Id})
			if err != nil {
				return err
			}
			if !added {
				return storage.ErrNoSuchKey
			}
			sharedPart := *coveredPart
			sharedPart.RefPreAcquired = true
			metadataResult, err := mbs.metadataStore.UploadPart(ctx, tx.SqlTx(), dstBucket, dstKey, uploadId, partNumber, sharedPart)
			if err != nil {
				return err
			}
			if err := mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts); err != nil {
				return err
			}
			result = storage.UploadPartCopyResult{ETag: coveredPart.ETag, LastModified: time.Now(), SourceVersionID: srcObject.VersionID}
			return nil
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
			return store.PutPart(ctx, tx, *newPartId, r)
		})
		if err != nil {
			return err
		}

		metadataResult, err := mbs.metadataStore.UploadPart(ctx, tx.SqlTx(), dstBucket, dstKey, uploadId, partNumber, metadatastore.Part{
			Id:                *newPartId,
			ETag:              *checksums.ETag,
			ChecksumCRC32:     checksums.ChecksumCRC32,
			ChecksumCRC32C:    checksums.ChecksumCRC32C,
			ChecksumCRC64NVME: checksums.ChecksumCRC64NVME,
			ChecksumSHA1:      checksums.ChecksumSHA1,
			ChecksumSHA256:    checksums.ChecksumSHA256,
			Size:              *size,
			StoreName:         storeName,
		})
		if err != nil {
			return err
		}
		if err := mbs.deleteUnreferencedParts(ctx, tx, metadataResult.UnreferencedParts); err != nil {
			return err
		}

		result = storage.UploadPartCopyResult{ETag: *checksums.ETag, LastModified: time.Now(), SourceVersionID: srcObject.VersionID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func convertCompleteMultipartUploadResult(result metadatastore.CompleteMultipartUploadResult) storage.CompleteMultipartUploadResult {
	return storage.CompleteMultipartUploadResult{
		Location:          result.Location,
		VersionID:         result.VersionID,
		ETag:              result.ETag,
		ChecksumCRC32:     result.ChecksumCRC32,
		ChecksumCRC32C:    result.ChecksumCRC32C,
		ChecksumCRC64NVME: result.ChecksumCRC64NVME,
		ChecksumSHA1:      result.ChecksumSHA1,
		ChecksumSHA256:    result.ChecksumSHA256,
		ChecksumType:      result.ChecksumType,
	}
}

func (mbs *metadataPartStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CompleteMultipartUpload")
	defer span.End()
	var completeMultipartUploadResult storage.CompleteMultipartUploadResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		result, err := mbs.metadataStore.CompleteMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId, checksumInput, opts)
		if err != nil {
			return err
		}
		if err := mbs.deleteUnreferencedParts(ctx, tx, result.UnreferencedParts); err != nil {
			return err
		}
		completeMultipartUploadResult = convertCompleteMultipartUploadResult(*result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &completeMultipartUploadResult, nil
}

func (mbs *metadataPartStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.AbortMultipartUpload")
	defer span.End()
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId)
		if err != nil {
			return err
		}
		return mbs.deleteUnreferencedParts(ctx, tx, abortMultipartUploadResult.UnreferencedParts)
	})
}

func convertListMultipartUploadsResult(mlistMultipartUploadsResult metadatastore.ListMultipartUploadsResult) storage.ListMultipartUploadsResult {
	return storage.ListMultipartUploadsResult{
		BucketName:         mlistMultipartUploadsResult.Bucket,
		KeyMarker:          mlistMultipartUploadsResult.KeyMarker,
		UploadIdMarker:     mlistMultipartUploadsResult.UploadIdMarker,
		NextKeyMarker:      mlistMultipartUploadsResult.NextKeyMarker,
		Prefix:             mlistMultipartUploadsResult.Prefix,
		Delimiter:          mlistMultipartUploadsResult.Delimiter,
		NextUploadIdMarker: mlistMultipartUploadsResult.NextUploadIdMarker,
		MaxUploads:         mlistMultipartUploadsResult.MaxUploads,
		CommonPrefixes:     mlistMultipartUploadsResult.CommonPrefixes,
		Uploads: sliceutils.Map(func(mUpload metadatastore.Upload) storage.Upload {
			return storage.Upload{
				Key:          mUpload.Key,
				UploadId:     mUpload.UploadId,
				Initiated:    mUpload.Initiated,
				StorageClass: mUpload.StorageClass,
			}
		}, mlistMultipartUploadsResult.Uploads),
		IsTruncated: mlistMultipartUploadsResult.IsTruncated,
	}
}

func (mbs *metadataPartStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListMultipartUploads")
	defer span.End()

	var mListMultipartUploadsResult *metadatastore.ListMultipartUploadsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListMultipartUploadsResult, err = mbs.metadataStore.ListMultipartUploads(ctx, tx.SqlTx(), bucketName, metadatastore.ListMultipartUploadsOptions{
			Prefix:         opts.Prefix,
			Delimiter:      opts.Delimiter,
			KeyMarker:      opts.KeyMarker,
			UploadIdMarker: opts.UploadIdMarker,
			MaxUploads:     opts.MaxUploads,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listMultipartUploadsResult := convertListMultipartUploadsResult(*mListMultipartUploadsResult)
	return &listMultipartUploadsResult, nil
}

func convertListPartsResult(mlistPartsResult metadatastore.ListPartsResult) storage.ListPartsResult {
	return storage.ListPartsResult{
		BucketName:           mlistPartsResult.BucketName,
		Key:                  mlistPartsResult.Key,
		UploadId:             mlistPartsResult.UploadId,
		PartNumberMarker:     mlistPartsResult.PartNumberMarker,
		NextPartNumberMarker: mlistPartsResult.NextPartNumberMarker,
		MaxParts:             mlistPartsResult.MaxParts,
		IsTruncated:          mlistPartsResult.IsTruncated,
		Parts: sliceutils.Map(func(part *metadatastore.MultipartPart) *storage.MultipartPart {
			return &storage.MultipartPart{
				ETag:              part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      part.LastModified,
				PartNumber:        part.PartNumber,
				Size:              part.Size,
			}
		}, mlistPartsResult.Parts),
		StorageClass: mlistPartsResult.StorageClass,
	}
}

func (mbs *metadataPartStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListParts")
	defer span.End()

	var mListPartsResult *metadatastore.ListPartsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListPartsResult, err = mbs.metadataStore.ListParts(ctx, tx.SqlTx(), bucketName, key, uploadId, metadatastore.ListPartsOptions{
			PartNumberMarker: opts.PartNumberMarker,
			MaxParts:         opts.MaxParts,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listPartsResult := convertListPartsResult(*mListPartsResult)
	return &listPartsResult, nil
}
