package metadatablob

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/gc"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/task"
)

type metadataBlobStorage struct {
	*lifecycle.ValidatedLifecycle
	db            database.Database
	metadataStore metadatastore.MetadataStore
	blobStore     blobstore.BlobStore
	blobGC        gc.BlobGarbageCollector
	gcTaskHandle  *task.TaskHandle
	tracer        trace.Tracer
}

// Compile-time check to ensure metadataBlobStorage implements storage.Storage
var _ storage.Storage = (*metadataBlobStorage)(nil)

func NewStorage(db database.Database, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("MetadataBlobStorage")
	if err != nil {
		return nil, err
	}
	blobGC, err := gc.New(db, metadataStore, blobStore)
	if err != nil {
		return nil, err
	}
	return &metadataBlobStorage{
		ValidatedLifecycle: lifecycle,
		db:                 db,
		metadataStore:      metadataStore,
		blobStore:          blobStore,
		blobGC:             blobGC,
		gcTaskHandle:       nil,
		tracer:             otel.Tracer("internal/storage/metadatablob"),
	}, nil
}

func (mbs *metadataBlobStorage) Start(ctx context.Context) error {
	if err := mbs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := mbs.metadataStore.Start(ctx); err != nil {
		return err
	}
	if err := mbs.blobStore.Start(ctx); err != nil {
		return err
	}

	mbs.gcTaskHandle = task.Start(mbs.blobGC.RunGCLoop)

	return nil
}

func (mbs *metadataBlobStorage) Stop(ctx context.Context) error {
	if err := mbs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	slog.Debug("Stopping GCLoop task")
	if mbs.gcTaskHandle != nil {
		mbs.gcTaskHandle.Cancel()
		joinedWithTimeout := mbs.gcTaskHandle.JoinWithTimeout(30 * time.Second)
		if joinedWithTimeout {
			slog.Debug("GCLoop joined with timeout of 30s")
		} else {
			slog.Debug("GCLoop joined without timeout")
		}
	}
	if err := mbs.metadataStore.Stop(ctx); err != nil {
		return err
	}
	if err := mbs.blobStore.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (mbs *metadataBlobStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.CreateBucket")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	err = mbs.metadataStore.CreateBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (mbs *metadataBlobStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.DeleteBucket")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	err = mbs.metadataStore.DeleteBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func convertBucket(mBucket metadatastore.Bucket) storage.Bucket {
	return storage.Bucket{
		Name:         mBucket.Name,
		CreationDate: mBucket.CreationDate,
	}
}

func (mbs *metadataBlobStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.ListBuckets")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mBuckets, err := mbs.metadataStore.ListBuckets(ctx, tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return sliceutils.Map(convertBucket, mBuckets), nil
}

func (mbs *metadataBlobStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.HeadBucket")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mBucket, err := mbs.metadataStore.HeadBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	b := convertBucket(*mBucket)
	return &b, err
}

func convertObject(mObject metadatastore.Object) storage.Object {
	return storage.Object{
		Key:               mObject.Key,
		ContentType:       mObject.ContentType,
		LastModified:      mObject.LastModified,
		ETag:              mObject.ETag,
		ChecksumCRC32:     mObject.ChecksumCRC32,
		ChecksumCRC32C:    mObject.ChecksumCRC32C,
		ChecksumCRC64NVME: mObject.ChecksumCRC64NVME,
		ChecksumSHA1:      mObject.ChecksumSHA1,
		ChecksumSHA256:    mObject.ChecksumSHA256,
		ChecksumType:      mObject.ChecksumType,
		Size:              mObject.Size,
	}
}

func convertListBucketResult(mListBucketResult metadatastore.ListBucketResult) storage.ListBucketResult {
	return storage.ListBucketResult{
		Objects:        sliceutils.Map(convertObject, mListBucketResult.Objects),
		CommonPrefixes: mListBucketResult.CommonPrefixes,
		IsTruncated:    mListBucketResult.IsTruncated,
	}
}

func (mbs *metadataBlobStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.ListObjects")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListBucketResult, err := mbs.metadataStore.ListObjects(ctx, tx, bucketName, metadatastore.ListObjectsOptions{
		Prefix:        opts.Prefix,
		Delimiter:     opts.Delimiter,
		StartAfter:    opts.StartAfter,
		MaxKeys:       opts.MaxKeys,
		SkipBlobFetch: true,
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	listBucketResult := convertListBucketResult(*mListBucketResult)
	return &listBucketResult, nil
}

func (mbs *metadataBlobStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.HeadObject")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mObject, err := mbs.metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
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
		if byteRange.End != nil && *byteRange.End > objectSize {
			return nil, storage.ErrInvalidRange
		}
		if byteRange.Start != nil && byteRange.End != nil && *byteRange.Start >= *byteRange.End {
			return nil, storage.ErrInvalidRange
		}

		normalized[i] = byteRange
	}

	return normalized, nil
}

// createRangeReader creates a reader for a specific byte range of an object.
func (mbs *metadataBlobStorage) createRangeReader(ctx context.Context, tx *sql.Tx, object *metadatastore.Object, byteRange storage.ByteRange) (io.ReadCloser, error) {
	startByte := byteRange.Start
	endByte := byteRange.End

	var blobReaders []io.ReadCloser
	var blobsSizeUntilNow int64
	var bytesSkipped int64
	newStartByteOffset := int64(0)
	if startByte != nil {
		newStartByteOffset = *startByte
	}

	skippingAtTheStart := true
	for _, blob := range object.Blobs {
		// Skip blobs past the requested range
		if endByte != nil && *endByte <= blobsSizeUntilNow {
			break
		}

		blobsSizeUntilNow += blob.Size

		// Skip blobs before the requested range
		if skippingAtTheStart && newStartByteOffset >= blob.Size {
			newStartByteOffset -= blob.Size
			bytesSkipped += blob.Size
			continue
		}
		skippingAtTheStart = false

		blobReader, err := mbs.blobStore.GetBlob(ctx, tx, blob.Id)
		if err != nil {
			// Close any readers we've already opened
			for _, r := range blobReaders {
				r.Close()
			}
			return nil, err
		}
		blobReaders = append(blobReaders, blobReader)
	}

	reader := ioutils.NewMultiReadCloser(blobReaders...)

	// Apply end limit first to avoid offset recalculation
	if endByte != nil {
		reader = ioutils.NewLimitedEndReadCloser(reader, *endByte-bytesSkipped)
	}

	// Skip to start position
	if startByte != nil {
		if _, err := ioutils.SkipNBytes(reader, newStartByteOffset); err != nil {
			reader.Close()
			return nil, err
		}
	}

	return reader, nil
}

func (mbs *metadataBlobStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.GetObject")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback() // Safe to call multiple times

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		return nil, nil, err
	}

	// Default to full object if no ranges specified
	if len(ranges) == 0 {
		ranges = []storage.ByteRange{{Start: nil, End: nil}}
	}

	// Normalize suffix ranges and validate
	ranges, err = normalizeAndValidateRanges(ranges, object.Size)
	if err != nil {
		return nil, nil, err
	}

	// Create readers for each range
	var readers []io.ReadCloser
	for _, byteRange := range ranges {
		reader, err := mbs.createRangeReader(ctx, tx, object, byteRange)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
			return nil, nil, err
		}
		readers = append(readers, reader)
	}

	if err := tx.Commit(); err != nil {
		for _, r := range readers {
			r.Close()
		}
		return nil, nil, err
	}

	storageObject := convertObject(*object)
	return &storageObject, readers, nil
}

func (mbs *metadataBlobStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.PutObject")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	// if we already have such an object,
	// remove all previous blobs
	previousObject, err := mbs.metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil && err != storage.ErrNoSuchKey {
		tx.Rollback()
		return nil, err
	}
	if previousObject != nil {
		for _, blob := range previousObject.Blobs {
			err = mbs.blobStore.DeleteBlob(ctx, tx, blob.Id)
			if err != nil {
				tx.Rollback()
				return nil, err
			}
		}
	}

	blobId, err := blobstore.NewRandomBlobId()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	originalSize, calculatedChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
		return mbs.blobStore.PutBlob(ctx, tx, *blobId, reader)
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	object := metadatastore.Object{
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
		Blobs: []metadatastore.Blob{
			{
				Id:                *blobId,
				ETag:              *calculatedChecksums.ETag,
				ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
				ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
				ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
				ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
				ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
				Size:              *originalSize,
			},
		},
	}

	err = mbs.metadataStore.PutObject(ctx, tx, bucketName, &object)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &storage.PutObjectResult{
		ETag:              &object.ETag,
		ChecksumCRC32:     object.ChecksumCRC32,
		ChecksumCRC32C:    object.ChecksumCRC32C,
		ChecksumCRC64NVME: object.ChecksumCRC64NVME,
		ChecksumSHA1:      object.ChecksumSHA1,
		ChecksumSHA256:    object.ChecksumSHA256,
	}, nil
}

func (mbs *metadataBlobStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.DeleteObject")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, blob := range object.Blobs {
		err = mbs.blobStore.DeleteBlob(ctx, tx, blob.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = mbs.metadataStore.DeleteObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func convertInitiateMultipartUploadResult(result metadatastore.InitiateMultipartUploadResult) storage.InitiateMultipartUploadResult {
	return storage.InitiateMultipartUploadResult{
		UploadId: result.UploadId,
	}
}

func (mbs *metadataBlobStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.CreateMultipartUpload")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, contentType, checksumType)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	initiateMultipartUploadResult := convertInitiateMultipartUploadResult(*result)
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return &initiateMultipartUploadResult, nil
}

func (mbs *metadataBlobStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.UploadPart")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	blobId, err := blobstore.NewRandomBlobId()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	originalSize, calculatedChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
		return mbs.blobStore.PutBlob(ctx, tx, *blobId, reader)
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = mbs.metadataStore.UploadPart(ctx, tx, bucketName, key, uploadId, partNumber, metadatastore.Blob{
		Id:                *blobId,
		ETag:              *calculatedChecksums.ETag,
		ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
		ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
		ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
		ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
		ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
		Size:              *originalSize,
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
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

func convertCompleteMultipartUploadResult(result metadatastore.CompleteMultipartUploadResult) storage.CompleteMultipartUploadResult {
	return storage.CompleteMultipartUploadResult{
		Location:          result.Location,
		ETag:              result.ETag,
		ChecksumCRC32:     result.ChecksumCRC32,
		ChecksumCRC32C:    result.ChecksumCRC32C,
		ChecksumCRC64NVME: result.ChecksumCRC64NVME,
		ChecksumSHA1:      result.ChecksumSHA1,
		ChecksumSHA256:    result.ChecksumSHA256,
		ChecksumType:      result.ChecksumType,
	}
}

func (mbs *metadataBlobStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.CompleteMultipartUpload")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, uploadId, checksumInput)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	deletedBlobs := result.DeletedBlobs
	for _, deletedBlob := range deletedBlobs {
		err = mbs.blobStore.DeleteBlob(ctx, tx, deletedBlob.Id)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	completeMultipartUploadResult := convertCompleteMultipartUploadResult(*result)
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return &completeMultipartUploadResult, nil
}

func (mbs *metadataBlobStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.AbortMultipartUpload")
	defer span.End()

	unblockGC := mbs.blobGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}

	abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		tx.Rollback()
		return err
	}
	deletedBlobs := abortMultipartUploadResult.DeletedBlobs
	for _, deletedBlob := range deletedBlobs {
		err = mbs.blobStore.DeleteBlob(ctx, tx, deletedBlob.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
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
				Key:       mUpload.Key,
				UploadId:  mUpload.UploadId,
				Initiated: mUpload.Initiated,
			}
		}, mlistMultipartUploadsResult.Uploads),
		IsTruncated: mlistMultipartUploadsResult.IsTruncated,
	}
}

func (mbs *metadataBlobStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.ListMultipartUploads")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListMultipartUploadsResult, err := mbs.metadataStore.ListMultipartUploads(ctx, tx, bucketName, metadatastore.ListMultipartUploadsOptions{
		Prefix:         opts.Prefix,
		Delimiter:      opts.Delimiter,
		KeyMarker:      opts.KeyMarker,
		UploadIdMarker: opts.UploadIdMarker,
		MaxUploads:     opts.MaxUploads,
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
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
		Parts: sliceutils.Map(func(part *metadatastore.Part) *storage.Part {
			return &storage.Part{
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
	}
}

func (mbs *metadataBlobStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataBlobStorage.ListParts")
	defer span.End()

	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListPartsResult, err := mbs.metadataStore.ListParts(ctx, tx, bucketName, key, uploadId, metadatastore.ListPartsOptions{
		PartNumberMarker: opts.PartNumberMarker,
		MaxParts:         opts.MaxParts,
	})
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	listPartsResult := convertListPartsResult(*mListPartsResult)
	return &listPartsResult, nil
}
