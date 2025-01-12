package metadatablob

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/gc"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
	"github.com/jdillenkofer/pithos/internal/task"
)

type metadataBlobStorage struct {
	db                 *sql.DB
	startStopValidator *startstopvalidator.StartStopValidator
	metadataStore      metadatastore.MetadataStore
	blobStore          blobstore.BlobStore
	blobGC             gc.BlobGarbageCollector
	gcTaskHandle       *task.TaskHandle
}

func NewStorage(db *sql.DB, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (storage.Storage, error) {
	startStopValidator, err := startstopvalidator.New("MetadataBlobStorage")
	if err != nil {
		return nil, err
	}
	blobGC, err := gc.New(db, metadataStore, blobStore)
	if err != nil {
		return nil, err
	}
	return &metadataBlobStorage{
		db:                 db,
		startStopValidator: startStopValidator,
		metadataStore:      metadataStore,
		blobStore:          blobStore,
		blobGC:             blobGC,
		gcTaskHandle:       nil,
	}, nil
}

func (mbs *metadataBlobStorage) Start(ctx context.Context) error {
	err := mbs.startStopValidator.Start()
	if err != nil {
		return err
	}
	err = mbs.metadataStore.Start(ctx)
	if err != nil {
		return err
	}
	err = mbs.blobStore.Start(ctx)
	if err != nil {
		return err
	}

	mbs.gcTaskHandle = task.Start(mbs.blobGC.RunGCLoop)

	return nil
}

func (mbs *metadataBlobStorage) Stop(ctx context.Context) error {
	err := mbs.startStopValidator.Stop()
	if err != nil {
		return err
	}
	log.Println("Stopping GCLoop task")
	if mbs.gcTaskHandle != nil {
		mbs.gcTaskHandle.Cancel()
		joinedWithTimeout := mbs.gcTaskHandle.JoinWithTimeout(30 * time.Second)
		if joinedWithTimeout {
			log.Println("GCLoop joined with timeout of 30s")
		} else {
			log.Println("GCLoop joined without timeout")
		}
	}
	err = mbs.metadataStore.Stop(ctx)
	if err != nil {
		return err
	}
	err = mbs.blobStore.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (mbs *metadataBlobStorage) CreateBucket(ctx context.Context, bucket string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	err = mbs.metadataStore.CreateBucket(ctx, tx, bucket)
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

func (mbs *metadataBlobStorage) DeleteBucket(ctx context.Context, bucket string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	err = mbs.metadataStore.DeleteBucket(ctx, tx, bucket)
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
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

func (mbs *metadataBlobStorage) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	mBucket, err := mbs.metadataStore.HeadBucket(ctx, tx, bucket)
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
		Key:          mObject.Key,
		LastModified: mObject.LastModified,
		ETag:         mObject.ETag,
		Size:         mObject.Size,
	}
}

func convertListBucketResult(mListBucketResult metadatastore.ListBucketResult) storage.ListBucketResult {
	return storage.ListBucketResult{
		Objects:        sliceutils.Map(convertObject, mListBucketResult.Objects),
		CommonPrefixes: mListBucketResult.CommonPrefixes,
		IsTruncated:    mListBucketResult.IsTruncated,
	}
}

func (mbs *metadataBlobStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*storage.ListBucketResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	mListBucketResult, err := mbs.metadataStore.ListObjects(ctx, tx, bucket, prefix, delimiter, startAfter, maxKeys)
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

func (mbs *metadataBlobStorage) HeadObject(ctx context.Context, bucket string, key string) (*storage.Object, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	mObject, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
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

func (mbs *metadataBlobStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadCloser, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
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

	var reader io.ReadCloser = ioutils.NewMultiReadCloser(blobReaders)

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

func calculateETag(reader io.Reader) (*string, error) {
	hash := md5.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	hexSum := hex.EncodeToString(sum)
	etag := "\"" + hexSum + "\""
	return &etag, nil
}

func (mbs *metadataBlobStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// if we already have such an object,
	// remove all previous blobs
	previousObject, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
	if err != nil && err != storage.ErrNoSuchKey {
		tx.Rollback()
		return err
	}
	if previousObject != nil {
		for _, blob := range previousObject.Blobs {
			err = mbs.blobStore.DeleteBlob(ctx, tx, blob.Id)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	blobId, err := blobstore.GenerateBlobId()
	if err != nil {
		tx.Rollback()
		return err
	}

	originalSize, etag, err := mbs.uploadBlobAndCalculateEtag(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return err
	}

	object := metadatastore.Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         *etag,
		Size:         *originalSize,
		Blobs: []metadatastore.Blob{
			{
				Id:   *blobId,
				ETag: *etag,
				Size: *originalSize,
			},
		},
	}

	err = mbs.metadataStore.PutObject(ctx, tx, bucket, &object)
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

func (mbs *metadataBlobStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
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

	err = mbs.metadataStore.DeleteObject(ctx, tx, bucket, key)
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

func (mbs *metadataBlobStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*storage.InitiateMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CreateMultipartUpload(ctx, tx, bucket, key)
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

func (mbs *metadataBlobStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader) (*storage.UploadPartResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	blobId, err := blobstore.GenerateBlobId()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	originalSize, etag, err := mbs.uploadBlobAndCalculateEtag(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = mbs.metadataStore.UploadPart(ctx, tx, bucket, key, uploadId, partNumber, metadatastore.Blob{
		Id:   *blobId,
		ETag: *etag,
		Size: *originalSize,
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
		ETag: *etag,
	}, nil
}

func (mbs *metadataBlobStorage) uploadBlobAndCalculateEtag(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*int64, *string, error) {
	readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(2)

	doneChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	go func() {
		err := mbs.blobStore.PutBlob(ctx, tx, blobId, readers[0])
		if err != nil {
			errChan <- err
			return
		}
		doneChan <- struct{}{}
	}()

	etagChan := make(chan string, 1)
	errChan2 := make(chan error, 1)
	go func() {
		etag, err := calculateETag(readers[1])
		if err != nil {
			errChan2 <- err
			return
		}
		etagChan <- *etag
	}()

	originalSize, err := io.Copy(writer, reader)
	if err != nil {
		return nil, nil, err
	}
	err = closer.Close()
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-doneChan:
	case err := <-errChan:
		if err != nil {
			return nil, nil, err
		}
	}

	var etag string
	select {
	case etag = <-etagChan:
	case err := <-errChan2:
		if err != nil {
			return nil, nil, err
		}
	}
	return &originalSize, &etag, nil
}

func convertCompleteMultipartUploadResult(result metadatastore.CompleteMultipartUploadResult) storage.CompleteMultipartUploadResult {
	return storage.CompleteMultipartUploadResult{
		Location:       result.Location,
		ETag:           result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
	}
}

func (mbs *metadataBlobStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*storage.CompleteMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CompleteMultipartUpload(ctx, tx, bucket, key, uploadId)
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

func (mbs *metadataBlobStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(ctx, tx, bucket, key, uploadId)
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
