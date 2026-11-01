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
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func convertObject(mObject metadatastore.Object) storage.Object {
	sizes := make([]int64, len(mObject.Parts))
	for i, part := range mObject.Parts {
		sizes[i] = part.Size
	}
	return storage.Object{PartSizes: sizes,
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
		ObjectLock:        mObject.ObjectLock,
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
			Prefix:     opts.Prefix,
			Delimiter:  opts.Delimiter,
			StartAfter: opts.StartAfter,
			MaxKeys:    opts.MaxKeys,
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
// openRangeReaders returns one lazy reader per byte range of object. The
// readers hold a reader lease on the parts they read, taken for a metadata
// snapshot no earlier than snapshotAt; the lease ends with ctx.
func (mbs *metadataPartStorage) openRangeReaders(ctx context.Context, tx database.Tx, object *metadatastore.Object, byteRanges []storage.ByteRange, snapshotAt time.Time) ([]io.ReadCloser, error) {
	plans := make([][]partRange, len(byteRanges))
	var partIds []partstore.PartId
	seen := make(map[partstore.PartId]struct{})
	for i, byteRange := range byteRanges {
		parts, err := mbs.planRange(object, byteRange)
		if err != nil {
			return nil, err
		}
		plans[i] = parts
		for _, part := range parts {
			if _, ok := seen[part.id]; !ok {
				seen[part.id] = struct{}{}
				partIds = append(partIds, part.id)
			}
		}
	}
	lease := mbs.readerLeases.begin(ctx, snapshotAt, partIds)
	defer lease.done()
	readers := make([]io.ReadCloser, len(plans))
	for i, parts := range plans {
		if len(parts) == 0 {
			readers[i] = io.NopCloser(bytes.NewReader(nil))
			continue
		}
		readers[i] = lease.wrap(&lazyPartSequenceReadCloser{
			ctx:   ctx,
			tx:    tx,
			parts: parts,
		})
	}
	return readers, nil
}

// planRange returns the part slices that make up byteRange of object.
func (mbs *metadataPartStorage) planRange(object *metadatastore.Object, byteRange storage.ByteRange) ([]partRange, error) {
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
	return parts, nil
}

// objectRead is what GetObject looked up: the object and the byte ranges to
// stream from it. No ranges means an empty body.
type objectRead struct {
	object *metadatastore.Object
	ranges []storage.ByteRange
	size   int64
}

func (mbs *metadataPartStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetObject")
	defer span.End()

	ambientTx, ambient := database.AmbientTx(ctx, mbs.db)
	snapshotAt := database.SnapshotTime(ctx, mbs.db)

	lookupObject := func(ctx context.Context, tx database.Tx) (*objectRead, error) {
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

		// Default to the full object if no ranges were specified. For a
		// multipart-part request, interpret ranges relative to that part and
		// translate them to the object's byte offsets within this transaction.
		effectiveRanges := ranges
		objectSize := object.Size
		partOffset := int64(0)
		if opts != nil && opts.PartNumber != nil {
			partNumber := int(*opts.PartNumber)
			if partNumber < 1 || partNumber > len(object.Parts) {
				return nil, storage.ErrInvalidRange
			}
			for i := 0; i < partNumber-1; i++ {
				partOffset += object.Parts[i].Size
			}
			objectSize = object.Parts[partNumber-1].Size
		}
		if opts != nil && opts.PartNumber != nil && objectSize == 0 {
			if len(ranges) > 0 {
				return nil, storage.ErrInvalidRange
			}
			return &objectRead{object: object, size: 0}, nil
		}
		if len(effectiveRanges) == 0 {
			if opts != nil && opts.PartNumber != nil {
				zero, partEnd := int64(0), objectSize
				effectiveRanges = []storage.ByteRange{{Start: &zero, End: &partEnd}}
			} else {
				effectiveRanges = []storage.ByteRange{{Start: nil, End: nil}}
			}
		}

		// Normalize suffix ranges and validate against the selected object/part.
		effectiveRanges, err = normalizeAndValidateRanges(effectiveRanges, objectSize)
		if err != nil {
			return nil, err
		}
		if opts != nil && opts.PartNumber != nil {
			for i := range effectiveRanges {
				if effectiveRanges[i].Start == nil {
					zero := int64(0)
					effectiveRanges[i].Start = &zero
				}
				if effectiveRanges[i].End == nil {
					partEnd := objectSize
					effectiveRanges[i].End = &partEnd
				}
				start := *effectiveRanges[i].Start + partOffset
				end := *effectiveRanges[i].End + partOffset
				effectiveRanges[i].Start = &start
				effectiveRanges[i].End = &end
			}
		}

		return &objectRead{object: object, ranges: effectiveRanges, size: objectSize}, nil
	}

	// The metadata lookup runs in a short transaction that ends before
	// streaming starts, so it never pins a snapshot for a whole download.
	// The object and its parts come from separate statements; one snapshot
	// keeps them consistent with each other.
	lookupOptions := &sql.TxOptions{ReadOnly: true}
	if !ambient && mbs.db.GetDatabaseType() == database.DB_TYPE_POSTGRES {
		lookupOptions.Isolation = sql.LevelRepeatableRead
	}
	var read *objectRead
	err := database.WithTx(ctx, mbs.db, lookupOptions, func(ctx context.Context, tx database.Tx) error {
		var err error
		read, err = lookupObject(ctx, tx)
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	// The range readers are lazy: they don't touch the part store until
	// first read. Reader leases protect the parts they read.
	openReaders := func(ctx context.Context, tx database.Tx) ([]io.ReadCloser, error) {
		if len(read.ranges) == 0 {
			return []io.ReadCloser{io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return mbs.openRangeReaders(ctx, tx, read.object, read.ranges, snapshotAt)
	}
	var readers []io.ReadCloser
	switch {
	case ambient:
		// Readers inside a caller's transaction keep using it: they may read
		// parts that transaction wrote and has not committed yet.
		readers, err = openReaders(ctx, ambientTx)
	case mbs.partStores.Capabilities().Has(partstore.CapabilityTxFreeGetPart):
		// Slow clients then pin no pooled database connection for the
		// lifetime of their download.
		readers, err = openReaders(ctx, nil)
	default:
		// A part store without tx-free reads streams within a transaction
		// that stays open until every reader is closed (an object's parts may
		// live in any store). READ COMMITTED, the PostgreSQL default here,
		// keeps it from holding back cleanup for the whole download.
		readers, err = database.WithTxReadClosers(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, openReaders)
	}
	if err != nil {
		return nil, nil, err
	}

	storageObject := convertObject(*read.object)
	storageObject.Size = read.size
	return &storageObject, readers, nil
}
