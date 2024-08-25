package storage

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
)

type MetadataBlobStorage struct {
	db            *sql.DB
	metadataStore metadata.MetadataStore
	blobStore     blob.BlobStore
}

func NewMetadataBlobStorage(db *sql.DB, metadataStore metadata.MetadataStore, blobStore blob.BlobStore) (*MetadataBlobStorage, error) {
	return &MetadataBlobStorage{
		db:            db,
		metadataStore: metadataStore,
		blobStore:     blobStore,
	}, nil
}

func (mbs *MetadataBlobStorage) Start(ctx context.Context) error {
	err := mbs.metadataStore.Start(ctx)
	if err != nil {
		return err
	}
	err = mbs.blobStore.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (mbs *MetadataBlobStorage) Stop(ctx context.Context) error {
	err := mbs.metadataStore.Stop(ctx)
	if err != nil {
		return err
	}
	err = mbs.blobStore.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (mbs *MetadataBlobStorage) CreateBucket(ctx context.Context, bucket string) error {
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

func (mbs *MetadataBlobStorage) DeleteBucket(ctx context.Context, bucket string) error {
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

func convertBucket(mBucket metadata.Bucket) Bucket {
	return Bucket{
		Name:         mBucket.Name,
		CreationDate: mBucket.CreationDate,
	}
}

func (mbs *MetadataBlobStorage) ListBuckets(ctx context.Context) ([]Bucket, error) {
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

func (mbs *MetadataBlobStorage) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
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

func convertObject(mObject metadata.Object) Object {
	return Object{
		Key:          mObject.Key,
		LastModified: mObject.LastModified,
		ETag:         mObject.ETag,
		Size:         mObject.Size,
	}
}

func convertListBucketResult(mListBucketResult metadata.ListBucketResult) ListBucketResult {
	return ListBucketResult{
		Objects:        sliceutils.Map(convertObject, mListBucketResult.Objects),
		CommonPrefixes: mListBucketResult.CommonPrefixes,
		IsTruncated:    mListBucketResult.IsTruncated,
	}
}

func (mbs *MetadataBlobStorage) ListObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
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

func (mbs *MetadataBlobStorage) HeadObject(ctx context.Context, bucket string, key string) (*Object, error) {
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

func (mbs *MetadataBlobStorage) GetObject(ctx context.Context, bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
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

func (mbs *MetadataBlobStorage) PutObject(ctx context.Context, bucket string, key string, reader io.Reader) error {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// if we already have such an object,
	// remove all previous blobs
	previousObject, err := mbs.metadataStore.HeadObject(ctx, tx, bucket, key)
	if err != nil && err != ErrNoSuchKey {
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

	blobId, err := blob.GenerateBlobId()
	if err != nil {
		tx.Rollback()
		return err
	}

	putBlobResult, err := mbs.blobStore.PutBlob(ctx, tx, *blobId, reader)
	if err != nil {
		tx.Rollback()
		return err
	}

	object := metadata.Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         putBlobResult.ETag,
		Size:         putBlobResult.Size,
		Blobs: []metadata.Blob{
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

func (mbs *MetadataBlobStorage) DeleteObject(ctx context.Context, bucket string, key string) error {
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

func convertInitiateMultipartUploadResult(result metadata.InitiateMultipartUploadResult) InitiateMultipartUploadResult {
	return InitiateMultipartUploadResult{
		UploadId: result.UploadId,
	}
}

func (mbs *MetadataBlobStorage) CreateMultipartUpload(ctx context.Context, bucket string, key string) (*InitiateMultipartUploadResult, error) {
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

func (mbs *MetadataBlobStorage) UploadPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32, data io.Reader) (*UploadPartResult, error) {
	tx, err := mbs.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	blobId, err := blob.GenerateBlobId()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	putBlobResult, err := mbs.blobStore.PutBlob(ctx, tx, *blobId, data)
	if err != nil {
		return nil, err
	}
	err = mbs.metadataStore.UploadPart(ctx, tx, bucket, key, uploadId, partNumber, metadata.Blob{
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
	return &UploadPartResult{
		ETag: putBlobResult.ETag,
	}, nil
}

func convertCompleteMultipartUploadResult(result metadata.CompleteMultipartUploadResult) CompleteMultipartUploadResult {
	return CompleteMultipartUploadResult{
		Location:       result.Location,
		ETag:           result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
	}
}

func (mbs *MetadataBlobStorage) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
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

func (mbs *MetadataBlobStorage) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadId string) error {
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
