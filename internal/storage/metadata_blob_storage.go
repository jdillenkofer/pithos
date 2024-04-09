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

func (mbs *MetadataBlobStorage) ListObjects(bucket string, prefix string, delimiter string, startAfter string, maxKeys int) ([]Object, []string, error) {
	mObjects, commonPrefixes, err := mbs.metadataStore.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		return nil, nil, err
	}
	return sliceutils.Map(convertObject, mObjects), commonPrefixes, nil
}

func (mbs *MetadataBlobStorage) HeadObject(bucket string, key string) (*Object, error) {
	mObject, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return nil, err
	}
	o := convertObject(*mObject)
	return &o, err
}

func (mbs *MetadataBlobStorage) GetObject(bucket string, key string) (io.ReadCloser, error) {
	object, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return nil, err
	}
	blobReaders := []io.ReadCloser{}
	for _, blobId := range object.BlobIds {
		blobReader, err := mbs.blobStore.GetBlob(blobId)
		if err != nil {
			return nil, err
		}
		blobReaders = append(blobReaders, blobReader)
	}
	return ioutils.NewMultiReadCloser(blobReaders), nil
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
		BlobIds: []blob.BlobId{
			putBlobResult.BlobId,
		},
	}
	return mbs.metadataStore.PutObject(bucket, &object)
}

func (mbs *MetadataBlobStorage) DeleteObject(bucket string, key string) error {
	object, err := mbs.metadataStore.HeadObject(bucket, key)
	if err != nil {
		return err
	}
	for _, blobId := range object.BlobIds {
		err = mbs.blobStore.DeleteBlob(blobId)
		if err != nil {
			log.Printf("Failed to delete blob: %v", err)
		}
	}
	return mbs.metadataStore.DeleteObject(bucket, key)
}

func (mbs *MetadataBlobStorage) Clear() error {
	err := mbs.metadataStore.Clear()
	if err != nil {
		return err
	}
	err = mbs.blobStore.Clear()
	return err
}
