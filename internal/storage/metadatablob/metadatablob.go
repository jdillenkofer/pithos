package metadatablob

import (
	"context"
	"database/sql"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
	"github.com/jdillenkofer/pithos/internal/task"
)

type BlobGarbageCollector struct {
	db              *sql.DB
	collectionMutex sync.RWMutex
	metadataStore   metadatastore.MetadataStore
	blobStore       blobstore.BlobStore
	writeOperations atomic.Int64
}

func NewBlobGarbageCollector(db *sql.DB, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (*BlobGarbageCollector, error) {
	return &BlobGarbageCollector{
		db:              db,
		collectionMutex: sync.RWMutex{},
		writeOperations: atomic.Int64{},
		metadataStore:   metadataStore,
		blobStore:       blobStore,
	}, nil
}

func (blobGC *BlobGarbageCollector) BlockIfGCIsRunning() func() {
	blobGC.writeOperations.Add(1)
	blobGC.collectionMutex.RLock()
	return blobGC.collectionMutex.RUnlock
}

func (blobGC *BlobGarbageCollector) RunGCLoop(stopRunning *atomic.Bool) {
	var lastWriteOperationCount int64 = 0
	for !stopRunning.Load() {
		newWriteOperationCount := blobGC.writeOperations.Load()
		if newWriteOperationCount > lastWriteOperationCount {
			log.Println("Running blob garbage collector")
			err := blobGC.RunGC()
			if err != nil {
				log.Printf("Failure while running garbage collector: %s\n", err)
			} else {
				log.Println("Ran blob garbage collector successfully")
			}
		}
		lastWriteOperationCount = newWriteOperationCount
		for range 30 * 4 {
			time.Sleep(250 * time.Millisecond)
			if stopRunning.Load() {
				return
			}
		}
	}
}

func (blobGC *BlobGarbageCollector) RunGC() error {
	blobGC.collectionMutex.Lock()
	defer blobGC.collectionMutex.Unlock()

	ctx := context.Background()

	tx, err := blobGC.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	existingBlobIds, err := blobGC.blobStore.GetBlobIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	inUseBlobIdMap := make(map[blobstore.BlobId]struct{})
	inUseBlobIds, err := blobGC.metadataStore.GetInUseBlobIds(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, inUseBlobId := range inUseBlobIds {
		inUseBlobIdMap[inUseBlobId] = struct{}{}
	}

	numDeletedBlobs := 0

	for _, existingBlobId := range existingBlobIds {
		if _, hasKey := inUseBlobIdMap[existingBlobId]; !hasKey {
			err = blobGC.blobStore.DeleteBlob(ctx, tx, existingBlobId)
			if err != nil {
				tx.Rollback()
				return err
			}

			numDeletedBlobs += 1
		}
	}

	log.Printf("Garbage Collection deleted %d blobs\n", numDeletedBlobs)

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

type metadataBlobStorage struct {
	db                 *sql.DB
	gcTaskHandle       *task.TaskHandle
	blobGC             *BlobGarbageCollector
	metadataStore      metadatastore.MetadataStore
	blobStore          blobstore.BlobStore
	startStopValidator *startstopvalidator.StartStopValidator
}

func NewStorage(db *sql.DB, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (storage.Storage, error) {
	blobGC, err := NewBlobGarbageCollector(db, metadataStore, blobStore)
	if err != nil {
		return nil, err
	}
	startStopValidator, err := startstopvalidator.New("MetadataBlobStorage")
	if err != nil {
		return nil, err
	}
	return &metadataBlobStorage{
		db:                 db,
		gcTaskHandle:       nil,
		blobGC:             blobGC,
		metadataStore:      metadataStore,
		blobStore:          blobStore,
		startStopValidator: startStopValidator,
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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

func (mbs *metadataBlobStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	object, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	blobReaders := []io.ReadSeekCloser{}
	for _, blob := range object.Blobs {
		blobReader, err := mbs.blobStore.GetBlob(ctx, tx, blob.Id)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		blobReaders = append(blobReaders, blobReader)
	}

	var reader io.ReadSeekCloser
	reader, err = ioutils.NewMultiReadSeekCloser(blobReaders)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	// We need to apply the LimitedEndReadSeekCloser first, otherwise we need to recalculate the end offset
	// because the LimitedStartSeekCloser changes the offsets
	if endByte != nil {
		reader = ioutils.NewLimitedEndReadSeekCloser(reader, *endByte)
	}
	if startByte != nil {
		reader = ioutils.NewLimitedStartReadSeekCloser(reader, *startByte)
	}
	return reader, nil
}

func (mbs *metadataBlobStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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

	putBlobResult, err := mbs.blobStore.PutBlob(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return err
	}

	object := metadatastore.Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         putBlobResult.ETag,
		Size:         putBlobResult.Size,
		Blobs: []metadatastore.Blob{
			{
				Id:   putBlobResult.BlobId,
				ETag: putBlobResult.ETag,
				Size: putBlobResult.Size,
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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

func (mbs *metadataBlobStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*storage.UploadPartResult, error) {
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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

	putBlobResult, err := mbs.blobStore.PutBlob(ctx, tx, *blobId, data)
	if err != nil {
		return nil, err
	}
	err = mbs.metadataStore.UploadPart(ctx, tx, bucket, key, uploadId, partNumber, metadatastore.Blob{
		Id:   putBlobResult.BlobId,
		ETag: putBlobResult.ETag,
		Size: putBlobResult.Size,
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
		ETag: putBlobResult.ETag,
	}, nil
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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
	unblockGC := mbs.blobGC.BlockIfGCIsRunning()
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
