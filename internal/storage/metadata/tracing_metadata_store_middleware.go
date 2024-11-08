package metadata

import (
	"context"
	"database/sql"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
)

type TracingMetadataStoreMiddleware struct {
	regionName         string
	innerMetadataStore MetadataStore
}

func NewTracingMetadataStoreMiddleware(regionName string, innerMetadataStore MetadataStore) (*TracingMetadataStoreMiddleware, error) {
	return &TracingMetadataStoreMiddleware{
		regionName:         regionName,
		innerMetadataStore: innerMetadataStore,
	}, nil
}

func (tmsm *TracingMetadataStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".Start()").End()
	return tmsm.innerMetadataStore.Start(ctx)
}

func (tmsm *TracingMetadataStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".Stop()").End()
	return tmsm.innerMetadataStore.Stop(ctx)
}

func (tmsm *TracingMetadataStoreMiddleware) GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".GetInUseBlobIds()").End()
	return tmsm.innerMetadataStore.GetInUseBlobIds(ctx, tx)
}

func (tmsm *TracingMetadataStoreMiddleware) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".CreateBucket()").End()

	return tmsm.innerMetadataStore.CreateBucket(ctx, tx, bucketName)
}

func (tmsm *TracingMetadataStoreMiddleware) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".DeleteBucket()").End()

	return tmsm.innerMetadataStore.DeleteBucket(ctx, tx, bucketName)
}

func (tmsm *TracingMetadataStoreMiddleware) ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListBuckets()").End()

	return tmsm.innerMetadataStore.ListBuckets(ctx, tx)
}

func (tmsm *TracingMetadataStoreMiddleware) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*Bucket, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".HeadBucket()").End()

	return tmsm.innerMetadataStore.HeadBucket(ctx, tx, bucketName)
}

func (tmsm *TracingMetadataStoreMiddleware) ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) (*ListBucketResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".ListObjects()").End()

	return tmsm.innerMetadataStore.ListObjects(ctx, tx, bucketName, prefix, delimiter, startAfter, maxKeys)
}

func (tmsm *TracingMetadataStoreMiddleware) HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*Object, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".HeadObject()").End()

	return tmsm.innerMetadataStore.HeadObject(ctx, tx, bucketName, key)
}

func (tmsm *TracingMetadataStoreMiddleware) PutObject(ctx context.Context, tx *sql.Tx, bucketName string, object *Object) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".PutObject()").End()

	return tmsm.innerMetadataStore.PutObject(ctx, tx, bucketName, object)
}

func (tmsm *TracingMetadataStoreMiddleware) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".DeleteObject()").End()

	return tmsm.innerMetadataStore.DeleteObject(ctx, tx, bucketName, key)
}

func (tmsm *TracingMetadataStoreMiddleware) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*InitiateMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".CreateMultipartUpload()").End()

	return tmsm.innerMetadataStore.CreateMultipartUpload(ctx, tx, bucketName, key)
}

func (tmsm *TracingMetadataStoreMiddleware) UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blob Blob) error {
	defer trace.StartRegion(ctx, tmsm.regionName+".UploadPart()").End()

	return tmsm.innerMetadataStore.UploadPart(ctx, tx, bucketName, key, uploadId, partNumber, blob)
}

func (tmsm *TracingMetadataStoreMiddleware) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".CompleteMultipartUpload()").End()

	return tmsm.innerMetadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, uploadId)
}

func (tmsm *TracingMetadataStoreMiddleware) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*AbortMultipartResult, error) {
	defer trace.StartRegion(ctx, tmsm.regionName+".AbortMultipartUpload()").End()

	return tmsm.innerMetadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, uploadId)
}
