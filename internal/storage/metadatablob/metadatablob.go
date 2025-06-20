package metadatablob

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"hash/crc32"
	"hash/crc64"
	"io"
	"log"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/gc"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/startstopvalidator"
	"github.com/jdillenkofer/pithos/internal/task"
)

type metadataBlobStorage struct {
	db                 database.Database
	startStopValidator *startstopvalidator.StartStopValidator
	metadataStore      metadatastore.MetadataStore
	blobStore          blobstore.BlobStore
	blobGC             gc.BlobGarbageCollector
	gcTaskHandle       *task.TaskHandle
}

func NewStorage(db database.Database, metadataStore metadatastore.MetadataStore, blobStore blobstore.BlobStore) (storage.Storage, error) {
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
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

func (mbs *metadataBlobStorage) HeadBucket(ctx context.Context, bucket string) (*storage.Bucket, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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

func (mbs *metadataBlobStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int32) (*storage.ListBucketResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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

func calculateCrc32(reader io.Reader) (*string, error) {
	hash := crc32.NewIEEE()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateCrc32c(reader io.Reader) (*string, error) {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateCrc64Nvme(reader io.Reader) (*string, error) {
	hash := crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateSha1(reader io.Reader) (*string, error) {
	hash := sha1.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateSha256(reader io.Reader) (*string, error) {
	hash := sha256.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func (mbs *metadataBlobStorage) PutObject(ctx context.Context, bucket string, key string, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput) error {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
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

	originalSize, calculatedChecksums, err := mbs.uploadBlobAndCalculateChecksums(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	if err != nil {
		tx.Rollback()
		return err
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
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

func (mbs *metadataBlobStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CreateMultipartUpload(ctx, tx, bucket, key, contentType, checksumType)
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

func (mbs *metadataBlobStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
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

	originalSize, calculatedChecksums, err := mbs.uploadBlobAndCalculateChecksums(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = mbs.metadataStore.UploadPart(ctx, tx, bucket, key, uploadId, partNumber, metadatastore.Blob{
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

func (mbs *metadataBlobStorage) uploadBlobAndCalculateChecksums(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*int64, *metadatastore.ChecksumValues, error) {
	readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(7)

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

	crc32Chan := make(chan string, 1)
	errChan3 := make(chan error, 1)
	go func() {
		crc32, err := calculateCrc32(readers[2])
		if err != nil {
			errChan3 <- err
			return
		}
		crc32Chan <- *crc32
	}()

	crc32cChan := make(chan string, 1)
	errChan4 := make(chan error, 1)
	go func() {
		crc32c, err := calculateCrc32c(readers[3])
		if err != nil {
			errChan4 <- err
			return
		}
		crc32cChan <- *crc32c
	}()

	crc64nvmeChan := make(chan string, 1)
	errChan5 := make(chan error, 1)
	go func() {
		crc64nvme, err := calculateCrc64Nvme(readers[4])
		if err != nil {
			errChan5 <- err
			return
		}
		crc64nvmeChan <- *crc64nvme
	}()

	sha1Chan := make(chan string, 1)
	errChan6 := make(chan error, 1)
	go func() {
		sha1, err := calculateSha1(readers[5])
		if err != nil {
			errChan6 <- err
			return
		}
		sha1Chan <- *sha1
	}()

	sha256Chan := make(chan string, 1)
	errChan7 := make(chan error, 1)
	go func() {
		sha256, err := calculateSha256(readers[6])
		if err != nil {
			errChan7 <- err
			return
		}
		sha256Chan <- *sha256
	}()

	// @Note: We need a anonymous function here,
	// because defers are always scoped to the function.
	// But if we don't directly defer close after the copy,
	// we deadlock the program
	originalSize, err := func() (*int64, error) {
		defer closer.Close()
		originalSize, err := io.Copy(writer, reader)
		if err != nil {
			return nil, err
		}
		return &originalSize, nil
	}()
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

	var checksumCRC32 string
	select {
	case checksumCRC32 = <-crc32Chan:
	case err := <-errChan3:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumCRC32C string
	select {
	case checksumCRC32C = <-crc32cChan:
	case err := <-errChan4:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumCRC64NVME string
	select {
	case checksumCRC64NVME = <-crc64nvmeChan:
	case err := <-errChan5:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumSHA1 string
	select {
	case checksumSHA1 = <-sha1Chan:
	case err := <-errChan6:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumSHA256 string
	select {
	case checksumSHA256 = <-sha256Chan:
	case err := <-errChan7:
		if err != nil {
			return nil, nil, err
		}
	}

	checksums := &metadatastore.ChecksumValues{
		ETag:              &etag,
		ChecksumCRC32:     &checksumCRC32,
		ChecksumCRC32C:    &checksumCRC32C,
		ChecksumCRC64NVME: &checksumCRC64NVME,
		ChecksumSHA1:      &checksumSHA1,
		ChecksumSHA256:    &checksumSHA256,
	}
	return originalSize, checksums, nil
}

func convertCompleteMultipartUploadResult(result metadatastore.CompleteMultipartUploadResult) storage.CompleteMultipartUploadResult {
	return storage.CompleteMultipartUploadResult{
		Location:       result.Location,
		ETag:           result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
		ChecksumType:   result.ChecksumType,
	}
}

func (mbs *metadataBlobStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	unblockGC := mbs.blobGC.PreventGCFromRunning()
	defer unblockGC()
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CompleteMultipartUpload(ctx, tx, bucket, key, uploadId, checksumInput)
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
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
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

func convertListMultipartUploadsResult(mlistMultipartUploadsResult metadatastore.ListMultipartUploadsResult) storage.ListMultipartUploadsResult {
	return storage.ListMultipartUploadsResult{
		Bucket:             mlistMultipartUploadsResult.Bucket,
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

func (mbs *metadataBlobStorage) ListMultipartUploads(ctx context.Context, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*storage.ListMultipartUploadsResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListMultipartUploadsResult, err := mbs.metadataStore.ListMultipartUploads(ctx, tx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
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
		Bucket:               mlistPartsResult.Bucket,
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

func (mbs *metadataBlobStorage) ListParts(ctx context.Context, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*storage.ListPartsResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	mListPartsResult, err := mbs.metadataStore.ListParts(ctx, tx, bucket, key, uploadId, partNumberMarker, maxParts)
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
