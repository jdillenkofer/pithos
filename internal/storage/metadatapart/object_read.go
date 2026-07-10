package metadatapart

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func convertObject(mObject metadatastore.Object) storage.Object {
	return storage.Object{
		Key:               mObject.Key,
		ContentType:       mObject.ContentType,
		LastModified:      mObject.LastModified,
		VersionID:         mObject.VersionID,
		IsDeleteMarker:    mObject.IsDeleteMarker,
		ETag:              mObject.ETag,
		ChecksumCRC32:     mObject.ChecksumCRC32,
		ChecksumCRC32C:    mObject.ChecksumCRC32C,
		ChecksumCRC64NVME: mObject.ChecksumCRC64NVME,
		ChecksumSHA1:      mObject.ChecksumSHA1,
		ChecksumSHA256:    mObject.ChecksumSHA256,
		ChecksumType:      mObject.ChecksumType,
		Size:              mObject.Size,
		StorageClass:      mObject.StorageClass,
		Tags:              mObject.Tags,
		Metadata:          mObject.Metadata,
	}
}

func convertListBucketResult(mListBucketResult metadatastore.ListBucketResult) storage.ListBucketResult {
	return storage.ListBucketResult{
		Objects:        sliceutils.Map(convertObject, mListBucketResult.Objects),
		CommonPrefixes: mListBucketResult.CommonPrefixes,
		IsTruncated:    mListBucketResult.IsTruncated,
	}
}

func (mbs *metadataPartStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListObjects")
	defer span.End()

	var mListBucketResult *metadatastore.ListBucketResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListBucketResult, err = mbs.metadataStore.ListObjects(ctx, tx.SqlTx(), bucketName, metadatastore.ListObjectsOptions{
			Prefix:        opts.Prefix,
			Delimiter:     opts.Delimiter,
			StartAfter:    opts.StartAfter,
			MaxKeys:       opts.MaxKeys,
			SkipPartFetch: true,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listBucketResult := convertListBucketResult(*mListBucketResult)
	return &listBucketResult, nil
}

func (mbs *metadataPartStorage) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListObjectVersions")
	defer span.End()

	var metaResult *metadatastore.ListObjectVersionsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		metaResult, err = mbs.metadataStore.ListObjectVersions(ctx, tx.SqlTx(), bucketName, metadatastore.ListObjectVersionsOptions{
			Prefix:          opts.Prefix,
			Delimiter:       opts.Delimiter,
			KeyMarker:       opts.KeyMarker,
			VersionIDMarker: opts.VersionIDMarker,
			MaxKeys:         opts.MaxKeys,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	versions := sliceutils.Map(func(v metadatastore.ObjectVersion) storage.ObjectVersion {
		return storage.ObjectVersion{Key: v.Key, VersionID: v.VersionID, IsDeleteMarker: v.IsDeleteMarker, IsLatest: v.IsLatest, LastModified: v.LastModified, Size: v.Size, ETag: v.ETag, StorageClass: v.StorageClass}
	}, metaResult.Versions)

	return &storage.ListObjectVersionsResult{
		Versions:            versions,
		CommonPrefixes:      metaResult.CommonPrefixes,
		IsTruncated:         metaResult.IsTruncated,
		NextKeyMarker:       metaResult.NextKeyMarker,
		NextVersionIDMarker: metaResult.NextVersionIDMarker,
	}, nil
}

func (mbs *metadataPartStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.HeadObject")
	defer span.End()

	var mObject *metadatastore.Object
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		if opts != nil && opts.VersionID != nil {
			mObject, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else {
			mObject, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			return err
		}

		if mObject.IsDeleteMarker {
			versionID := ""
			if mObject.VersionID != nil {
				versionID = *mObject.VersionID
			}
			if opts != nil && opts.VersionID != nil {
				return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: mObject.LastModified}
			}
			return &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if opts.IfMatchETag != nil {
				if *opts.IfMatchETag != storage.ETagWildcard && mObject.ETag != *opts.IfMatchETag {
					return storage.ErrPreconditionFailed
				}
			}
			if opts.IfNoneMatchETag != nil {
				if *opts.IfNoneMatchETag == storage.ETagWildcard || mObject.ETag == *opts.IfNoneMatchETag {
					return storage.ErrNotModified
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	o := convertObject(*mObject)
	return &o, err
}

// normalizeAndValidateRanges converts suffix ranges to absolute ranges and validates all ranges.
// Returns an error if any range is invalid.
func normalizeAndValidateRanges(ranges []storage.ByteRange, objectSize int64) ([]storage.ByteRange, error) {
	normalized := make([]storage.ByteRange, len(ranges))

	for i, byteRange := range ranges {
		// Handle suffix range (e.g., bytes=-500 means last 500 bytes)
		if byteRange.Start == nil && byteRange.End != nil {
			if *byteRange.End <= 0 {
				return nil, storage.ErrInvalidRange
			}
			suffixLength := min(*byteRange.End, objectSize)
			start := objectSize - suffixLength
			end := objectSize
			normalized[i] = storage.ByteRange{Start: &start, End: &end}
			continue
		}

		// Validate normal ranges
		if byteRange.Start != nil && *byteRange.Start < 0 {
			return nil, storage.ErrInvalidRange
		}
		// Per RFC 7233: if the range end exceeds the object size, clamp it to the object size.
		if byteRange.End != nil && *byteRange.End > objectSize {
			clamped := objectSize
			byteRange.End = &clamped
		}
		if byteRange.Start != nil && byteRange.End != nil && *byteRange.Start >= *byteRange.End {
			return nil, storage.ErrInvalidRange
		}

		normalized[i] = byteRange
	}

	return normalized, nil
}

// evaluateCopySourceConditions enforces the x-amz-copy-source-if-* preconditions
// against the source object of a server-side copy. Time comparisons use second
// granularity (HTTP dates carry no sub-second component) to match S3 behaviour.
// Returns storage.ErrPreconditionFailed when a precondition fails.
func evaluateCopySourceConditions(conditions storage.CopySourceConditions, object *metadatastore.Object) error {
	ifMatchPassed := false
	if conditions.IfMatch != nil {
		ifMatchPassed = *conditions.IfMatch == storage.ETagWildcard || *conditions.IfMatch == object.ETag
		if !ifMatchPassed {
			return storage.ErrPreconditionFailed
		}
	}
	if conditions.IfNoneMatch != nil {
		if *conditions.IfNoneMatch == storage.ETagWildcard || *conditions.IfNoneMatch == object.ETag {
			return storage.ErrPreconditionFailed
		}
	}
	lastModified := object.LastModified.Truncate(time.Second)
	if conditions.IfUnmodifiedSince != nil && !(conditions.IfMatch != nil && ifMatchPassed) && lastModified.After(*conditions.IfUnmodifiedSince) {
		return storage.ErrPreconditionFailed
	}
	if conditions.IfModifiedSince != nil && !lastModified.After(*conditions.IfModifiedSince) {
		return storage.ErrPreconditionFailed
	}
	return nil
}

// createRangeReader creates a reader for a specific byte range of an object.
func (mbs *metadataPartStorage) createRangeReader(ctx context.Context, tx database.Tx, object *metadatastore.Object, byteRange storage.ByteRange) (io.ReadCloser, error) {
	startByte := byteRange.Start
	endByte := byteRange.End

	parts := make([]partRange, 0)
	var partsSizeUntilNow int64
	var globalStart int64
	if startByte != nil {
		globalStart = *startByte
	}
	globalEnd := object.Size
	if endByte != nil {
		globalEnd = *endByte
	}
	if globalStart >= globalEnd {
		return nil, storage.ErrInvalidRange
	}

	for _, part := range object.Parts {
		partStart := partsSizeUntilNow
		partEnd := partStart + part.Size

		// Skip parts before the requested range.
		if globalStart >= partEnd {
			partsSizeUntilNow = partEnd
			continue
		}
		// Stop once we're past the requested range.
		if globalEnd <= partStart {
			break
		}

		rangeStartInPart := int64(0)
		if globalStart > partStart {
			rangeStartInPart = globalStart - partStart
		}
		rangeEndInPart := part.Size
		if globalEnd < partEnd {
			rangeEndInPart = globalEnd - partStart
		}
		if rangeEndInPart < rangeStartInPart {
			return nil, fmt.Errorf("invalid part range computed")
		}

		store, err := mbs.partStores.ByName(part.StoreName)
		if err != nil {
			return nil, err
		}

		limit := rangeEndInPart - rangeStartInPart
		parts = append(parts, partRange{
			id:    part.Id,
			store: store,
			skip:  rangeStartInPart,
			limit: &limit,
		})

		partsSizeUntilNow = partEnd
	}
	if len(parts) == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	return &lazyPartSequenceReadCloser{
		ctx:   ctx,
		tx:    tx,
		parts: parts,
	}, nil
}

func (mbs *metadataPartStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetObject")
	defer span.End()

	// When every part store can read without an ambient transaction
	// (filesystem, sftp — everything except DB-backed part content), the
	// metadata lookup runs in a short transaction that ends before streaming
	// starts. Slow clients then no longer pin a pooled database connection for
	// the lifetime of their download. A single DB-backed part store in the set
	// keeps the transaction open until every reader is closed (an object's
	// parts may live in any store), preserving snapshot semantics for the part
	// content itself.
	txFreeStreaming := mbs.partStores.SupportsTxFreeGetPart()

	// The context handed to WithTx carries the (short-lived) transaction;
	// BeginTx reuses a transaction found in the context, so tx-free readers
	// must capture the pre-transaction context instead or a later internal
	// BeginTx (e.g. the outbox lookup) would pick up the finalized one.
	streamCtx := ctx

	var storageObject storage.Object
	getObjectInTx := func(ctx context.Context, tx database.Tx) ([]io.ReadCloser, error) {
		var object *metadatastore.Object
		var err error
		if opts != nil && opts.VersionID != nil {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else {
			object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			return nil, err
		}

		if object.IsDeleteMarker {
			versionID := ""
			if object.VersionID != nil {
				versionID = *object.VersionID
			}
			if opts != nil && opts.VersionID != nil {
				return nil, &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: object.LastModified}
			}
			return nil, &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if opts.IfMatchETag != nil {
				if *opts.IfMatchETag != storage.ETagWildcard && object.ETag != *opts.IfMatchETag {
					return nil, storage.ErrPreconditionFailed
				}
			}
			if opts.IfNoneMatchETag != nil {
				if *opts.IfNoneMatchETag == storage.ETagWildcard || object.ETag == *opts.IfNoneMatchETag {
					return nil, storage.ErrNotModified
				}
			}
		}

		// Default to full object if no ranges specified
		effectiveRanges := ranges
		if len(effectiveRanges) == 0 {
			effectiveRanges = []storage.ByteRange{{Start: nil, End: nil}}
		}

		// Normalize suffix ranges and validate
		effectiveRanges, err = normalizeAndValidateRanges(effectiveRanges, object.Size)
		if err != nil {
			return nil, err
		}

		// The range readers are lazy: they don't touch the part store until
		// first read. With tx-free streaming they carry no transaction at all
		// and use the pre-transaction context.
		readerCtx := ctx
		readerTx := tx
		if txFreeStreaming {
			readerCtx = streamCtx
			readerTx = nil
		}

		// Create readers for each range
		var readers []io.ReadCloser
		for _, byteRange := range effectiveRanges {
			reader, err := mbs.createRangeReader(readerCtx, readerTx, object, byteRange)
			if err != nil {
				for _, r := range readers {
					r.Close()
				}
				return nil, err
			}
			readers = append(readers, reader)
		}

		storageObject = convertObject(*object)
		return readers, nil
	}

	var readers []io.ReadCloser
	var err error
	if txFreeStreaming {
		err = database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			readers, err = getObjectInTx(ctx, tx)
			return err
		})
	} else {
		readers, err = database.WithTxReadClosers(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, getObjectInTx)
	}
	if err != nil {
		return nil, nil, err
	}

	return &storageObject, readers, nil
}
