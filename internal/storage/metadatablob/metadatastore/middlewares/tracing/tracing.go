package tracing

import (
	"context"
	"database/sql"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type tracingMetadataStoreMiddleware struct {
	regionName         string
	innerMetadataStore metadatastore.MetadataStore
}

func New(regionName string, innerMetadataStore metadatastore.MetadataStore) (metadatastore.MetadataStore, error) {
	return &tracingMetadataStoreMiddleware{
		regionName:         regionName,
		innerMetadataStore: innerMetadataStore,
	}, nil
}

func (tmsm *tracingMetadataStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".Start()").End()
	return tmsm.innerMetadataStore.Start(ctx)
}

func (tmsm *tracingMetadataStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".Stop()").End()
	return tmsm.innerMetadataStore.Stop(ctx)
}

func (tmsm *tracingMetadataStoreMiddleware) GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".GetInUseBlobIds()").End()
	return tmsm.innerMetadataStore.GetInUseBlobIds(ctx, tx)
}

func (tmsm *tracingMetadataStoreMiddleware) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".CreateBucket()").End()

	return tmsm.innerMetadataStore.CreateBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".DeleteBucket()").End()

	return tmsm.innerMetadataStore.DeleteBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) ListBuckets(ctx context.Context, tx *sql.Tx) ([]metadatastore.Bucket, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListBuckets()").End()

	return tmsm.innerMetadataStore.ListBuckets(ctx, tx)
}

func (tmsm *tracingMetadataStoreMiddleware) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*metadatastore.Bucket, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".HeadBucket()").End()

	return tmsm.innerMetadataStore.HeadBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int32) (*metadatastore.ListBucketResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListObjects()").End()

	return tmsm.innerMetadataStore.ListObjects(ctx, tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (tmsm *tracingMetadataStoreMiddleware) HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*metadatastore.Object, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".HeadObject()").End()

	return tmsm.innerMetadataStore.HeadObject(ctx, tx, bucketName, key)
}

func (tmsm *tracingMetadataStoreMiddleware) PutObject(ctx context.Context, tx *sql.Tx, bucketName string, object *metadatastore.Object) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".PutObject()").End()

	return tmsm.innerMetadataStore.PutObject(ctx, tx, bucketName, object)
}

func (tmsm *tracingMetadataStoreMiddleware) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".DeleteObject()").End()

	return tmsm.innerMetadataStore.DeleteObject(ctx, tx, bucketName, key)
}

func (tmsm *tracingMetadataStoreMiddleware) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, contentType string) (*metadatastore.InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".CreateMultipartUpload()").End()

	return tmsm.innerMetadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, contentType)
}

func (tmsm *tracingMetadataStoreMiddleware) UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blob metadatastore.Blob) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".UploadPart()").End()

	return tmsm.innerMetadataStore.UploadPart(ctx, tx, bucketName, key, uploadId, partNumber, blob)
}

func (tmsm *tracingMetadataStoreMiddleware) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*metadatastore.CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".CompleteMultipartUpload()").End()

	return tmsm.innerMetadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, uploadId)
}

func (tmsm *tracingMetadataStoreMiddleware) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*metadatastore.AbortMultipartResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".AbortMultipartUpload()").End()

	return tmsm.innerMetadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, uploadId)
}

func (tmsm *tracingMetadataStoreMiddleware) ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*metadatastore.ListMultipartUploadsResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListMultipartUploads()").End()

	return tmsm.innerMetadataStore.ListMultipartUploads(ctx, tx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads)
}

func (tmsm *tracingMetadataStoreMiddleware) ListParts(ctx context.Context, tx *sql.Tx, bucket string, key string, uploadId string, partNumberMarker string, maxParts int32) (*metadatastore.ListPartsResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListParts()").End()

	return tmsm.innerMetadataStore.ListParts(ctx, tx, bucket, key, uploadId, partNumberMarker, maxParts)
}
