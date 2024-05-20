package storage

import (
	"io"
	"log"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
)

type MetadataBlobStorage struct {
	metadataStore metadata.MetadataStore
	blobStore     blob.BlobStore
}

func NewMetadataBlobStorage(metadataStore metadata.MetadataStore, blobStore blob.BlobStore) (*MetadataBlobStorage, error) {
	return &MetadataBlobStorage{
		metadataStore: metadataStore,
		blobStore:     blobStore,
	}, nil
}

func (mbs *MetadataBlobStorage) CreateBucket(bucket string) error {
	return mbs.metadataStore.CreateBucket(bucket)
}

func (mbs *MetadataBlobStorage) DeleteBucket(bucket string) error {
	return mbs.metadataStore.DeleteBucket(bucket)
}

func convertBucket(mBucket metadata.Bucket) Bucket {
	return Bucket{
		Name:         mBucket.Name,
		CreationDate: mBucket.CreationDate,
	}
}

func (mbs *MetadataBlobStorage) ListBuckets() ([]Bucket, error) {
	mBuckets, err := mbs.metadataStore.ListBuckets()
	if err != nil {
		return nil, err
	}
	return sliceutils.Map(convertBucket, mBuckets), nil
}

func (mbs *MetadataBlobStorage) HeadBucket(bucket string) (*Bucket, error) {
	mBucket, err := mbs.metadataStore.HeadBucket(bucket)
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
	mListBucketResult, err := mbs.metadataStore.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		return nil, err
	}
	listBucketResult := convertListBucketResult(*mListBucketResult)
	return &listBucketResult, nil
}

func (mbs *MetadataBlobStorage) HeadObject(bucket string, key string) (*Object, error) {
	mObject, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return nil, err
	}
	o := convertObject(*mObject)
	return &o, err
}

func (mbs *MetadataBlobStorage) GetObject(bucket string, key string, startByte *int64, endByte *int64) (io.ReadSeekCloser, error) {
	object, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return nil, err
	}
	blobReaders := []io.ReadSeekCloser{}
	for _, blob := range object.Blobs {
		blobReader, err := mbs.blobStore.GetBlob(blob.Id)
		if err != nil {
			return nil, err
		}
		blobReaders = append(blobReaders, blobReader)
	}
	var reader io.ReadSeekCloser
	reader, err = ioutils.NewMultiReadSeekCloser(blobReaders)
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
	putBlobResult, err := mbs.blobStore.PutBlob(reader)
	if err != nil {
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
	return mbs.metadataStore.PutObject(bucket, &object)
}

func (mbs *MetadataBlobStorage) DeleteObject(bucket string, key string) error {
	object, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return err
	}
	for _, blob := range object.Blobs {
		err = mbs.blobStore.DeleteBlob(blob.Id)
		if err != nil {
			log.Printf("Failed to delete blob: %v", err)
		}
	}
	return mbs.metadataStore.DeleteObject(bucket, key)
}

func convertInitiateMultipartUploadResult(result metadata.InitiateMultipartUploadResult) InitiateMultipartUploadResult {
	return InitiateMultipartUploadResult{
		UploadId: result.UploadId,
	}
}

func (mbs *MetadataBlobStorage) CreateMultipartUpload(bucket string, key string) (*InitiateMultipartUploadResult, error) {
	result, err := mbs.metadataStore.CreateMultipartUpload(bucket, key)
	if err != nil {
		return nil, err
	}
	initiateMultipartUploadResult := convertInitiateMultipartUploadResult(*result)
	return &initiateMultipartUploadResult, nil
}

func (mbs *MetadataBlobStorage) UploadPart(bucket string, key string, uploadId string, partNumber uint16, data io.Reader) error {
	putBlobResult, err := mbs.blobStore.PutBlob(data)
	if err != nil {
		return err
	}
	err = mbs.metadataStore.UploadPart(bucket, key, uploadId, partNumber, metadata.Blob{
		Id:   putBlobResult.BlobId,
		ETag: putBlobResult.ETag,
		Size: putBlobResult.Size,
	})
	return err
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
	result, err := mbs.metadataStore.CompleteMultipartUpload(bucket, key, uploadId)
	if err != nil {
		return nil, err
	}
	completeMultipartUploadResult := convertCompleteMultipartUploadResult(*result)
	return &completeMultipartUploadResult, nil
}

func (mbs *MetadataBlobStorage) AbortMultipartUpload(bucket string, key string, uploadId string) error {
	abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(bucket, key, uploadId)
	if err != nil {
		return err
	}
	blobs := abortMultipartUploadResult.Blobs
	for _, blob := range blobs {
		err = mbs.blobStore.DeleteBlob(blob.Id)
		if err != nil {
			log.Printf("Failed to delete blob: %v", err)
		}
	}
	return nil
}
