package metadatablob

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"time"

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
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

func (mbs *metadataBlobStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListBucketResult, err := mbs.metadataStore.ListObjects(ctx, tx, bucketName, prefix, delimiter, startAfter, maxKeys)
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

func (mbs *metadataBlobStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key string) (*storage.Object, error) {
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

func (mbs *metadataBlobStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	blobReaders := []io.ReadCloser{}
	blobsSizeUntilNow := int64(0)
	bytesSkipped := int64(0)
	newStartByteOffset := int64(0)
	if startByte != nil {
		newStartByteOffset = *startByte
	}
	skippingAtTheStart := true
	for _, blob := range object.Blobs {
		// We only get blobs within the requested byte range
		if endByte != nil && *endByte < blobsSizeUntilNow {
			break
		}
		blobsSizeUntilNow += blob.Size
		if skippingAtTheStart && newStartByteOffset >= blob.Size {
			newStartByteOffset -= blob.Size
			bytesSkipped += blob.Size
			continue
		}
		skippingAtTheStart = false

		blobReader, err := mbs.blobStore.GetBlob(ctx, tx, blob.Id)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		blobReaders = append(blobReaders, blobReader)
	}

	var reader io.ReadCloser = ioutils.NewMultiReadCloser(blobReaders...)

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	// We need to apply the LimitedEndReadSeekCloser first, otherwise we need to recalculate the end offset
	// because the LimitedStartSeekCloser changes the offsets
	if endByte != nil {
		reader = ioutils.NewLimitedEndReadCloser(reader, *endByte-bytesSkipped)
	}
	if startByte != nil {
		_, err = ioutils.SkipNBytes(reader, newStartByteOffset)
		if err != nil {
			return nil, err
		}
	}
	return reader, nil
}

func (mbs *metadataBlobStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

	blobId, err := blobstore.GenerateBlobId()
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

func (mbs *metadataBlobStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

func (mbs *metadataBlobStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

func (mbs *metadataBlobStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key string, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	blobId, err := blobstore.GenerateBlobId()
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

func (mbs *metadataBlobStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

func (mbs *metadataBlobStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key string, uploadId string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
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

func (mbs *metadataBlobStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListMultipartUploadsResult, err := mbs.metadataStore.ListMultipartUploads(ctx, tx, bucketName, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
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

func (mbs *metadataBlobStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListPartsResult, err := mbs.metadataStore.ListParts(ctx, tx, bucketName, key, uploadId, partNumberMarker, maxParts)
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
