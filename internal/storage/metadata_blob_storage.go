package storage

import (
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

func (mbs *MetadataBlobStorage) Start() error {
	err := mbs.metadataStore.Start()
	if err != nil {
		return err
	}
	err = mbs.blobStore.Start()
	if err != nil {
		return err
	}
	return nil
}

func (mbs *MetadataBlobStorage) Stop() error {
	err := mbs.metadataStore.Stop()
	if err != nil {
		return err
	}
	err = mbs.blobStore.Stop()
	if err != nil {
		return err
	}
	return nil
}

func (mbs *MetadataBlobStorage) CreateBucket(bucket string) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	err = mbs.metadataStore.CreateBucket(tx, bucket)
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

func (mbs *MetadataBlobStorage) DeleteBucket(bucket string) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	err = mbs.metadataStore.DeleteBucket(tx, bucket)
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

func (mbs *MetadataBlobStorage) ListBuckets() ([]Bucket, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	mBuckets, err := mbs.metadataStore.ListBuckets(tx)
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

func (mbs *MetadataBlobStorage) HeadBucket(bucket string) (*Bucket, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	mBucket, err := mbs.metadataStore.HeadBucket(tx, bucket)
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

func (mbs *MetadataBlobStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	mListBucketResult, err := mbs.metadataStore.ListObjects(tx, bucket, prefix, delimiter, startAfter, maxKeys)
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

func (mbs *MetadataBlobStorage) HeadObject(bucket string, key string) (*Object, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	mObject, err := mbs.metadataStore.HeadObject(tx, bucket, key)
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

func (mbs *MetadataBlobStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	object, err := mbs.metadataStore.HeadObject(tx, bucket, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	blobReaders := []io.ReadSeekCloser{}
	for _, blob := range object.Blobs {
		blobReader, err := mbs.blobStore.GetBlob(tx, blob.Id)
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

func (mbs *MetadataBlobStorage) PutObject(bucket string, key string, reader io.Reader) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	// if we already have such an object,
	// remove all previous blobs
	previousObject, err := mbs.metadataStore.HeadObject(tx, bucket, key)
	if err != nil && err != ErrNoSuchKey {
		tx.Rollback()
		return err
	}
	if previousObject != nil {
		for _, blob := range previousObject.Blobs {
			err = mbs.blobStore.DeleteBlob(tx, blob.Id)
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

	putBlobResult, err := mbs.blobStore.PutBlob(tx, *blobId, reader)
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

	err = mbs.metadataStore.PutObject(tx, bucket, &object)
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

func (mbs *MetadataBlobStorage) DeleteObject(bucket string, key string) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	object, err := mbs.metadataStore.HeadObject(tx, bucket, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, blob := range object.Blobs {
		err = mbs.blobStore.DeleteBlob(tx, blob.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = mbs.metadataStore.DeleteObject(tx, bucket, key)
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

func (mbs *MetadataBlobStorage) CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CreateMultipartUpload(tx, bucket, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	initiateMultipartUploadResult := convertInitiateMultipartUploadResult(*result)
	tx.Commit()
	return &initiateMultipartUploadResult, nil
}

func (mbs *MetadataBlobStorage) UploadPart(bucket string, key string, uploadId string, partNumber uint16, data io.Reader) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	blobId, err := blob.GenerateBlobId()
	if err != nil {
		tx.Rollback()
		return err
	}

	putBlobResult, err := mbs.blobStore.PutBlob(tx, *blobId, data)
	if err != nil {
		return err
	}
	err = mbs.metadataStore.UploadPart(tx, bucket, key, uploadId, partNumber, metadata.Blob{
		Id:   putBlobResult.BlobId,
		ETag: putBlobResult.ETag,
		Size: putBlobResult.Size,
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
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

func (mbs *MetadataBlobStorage) CompleteMultipartUpload(bucket string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	tx, err := mbs.db.Begin()
	if err != nil {
		return nil, err
	}

	result, err := mbs.metadataStore.CompleteMultipartUpload(tx, bucket, key, uploadId)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	completeMultipartUploadResult := convertCompleteMultipartUploadResult(*result)
	tx.Commit()
	return &completeMultipartUploadResult, nil
}

func (mbs *MetadataBlobStorage) AbortMultipartUpload(bucket string, key string, uploadId string) error {
	tx, err := mbs.db.Begin()
	if err != nil {
		return err
	}

	abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(tx, bucket, key, uploadId)
	if err != nil {
		tx.Rollback()
		return err
	}
	blobs := abortMultipartUploadResult.Blobs
	for _, blob := range blobs {
		err = mbs.blobStore.DeleteBlob(tx, blob.Id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}
