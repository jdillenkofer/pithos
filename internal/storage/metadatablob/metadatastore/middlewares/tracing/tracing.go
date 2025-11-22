package tracing

import (
	"context"
	"database/sql"

	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type tracingMetadataStoreMiddleware struct {
	regionName         string
	innerMetadataStore metadatastore.MetadataStore
	tracer             oteltrace.Tracer
}

// Compile-time check to ensure tracingMetadataStoreMiddleware implements metadatastore.MetadataStore
var _ metadatastore.MetadataStore = (*tracingMetadataStoreMiddleware)(nil)

func New(regionName string, innerMetadataStore metadatastore.MetadataStore) (metadatastore.MetadataStore, error) {
	return &tracingMetadataStoreMiddleware{
		regionName:         regionName,
		innerMetadataStore: innerMetadataStore,
		tracer:             otel.Tracer("github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/middlewares/tracing"),
	}, nil
}

func (tmsm *tracingMetadataStoreMiddleware) Start(ctx context.Context) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".Start")
	defer span.End()
	return tmsm.innerMetadataStore.Start(ctx)
}

func (tmsm *tracingMetadataStoreMiddleware) Stop(ctx context.Context) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".Stop")
	defer span.End()
	return tmsm.innerMetadataStore.Stop(ctx)
}

func (tmsm *tracingMetadataStoreMiddleware) GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".GetInUseBlobIds")
	defer span.End()
	return tmsm.innerMetadataStore.GetInUseBlobIds(ctx, tx)
}

func (tmsm *tracingMetadataStoreMiddleware) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".CreateBucket")
	defer span.End()

	return tmsm.innerMetadataStore.CreateBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".DeleteBucket")
	defer span.End()

	return tmsm.innerMetadataStore.DeleteBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) ListBuckets(ctx context.Context, tx *sql.Tx) ([]metadatastore.Bucket, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".ListBuckets")
	defer span.End()

	return tmsm.innerMetadataStore.ListBuckets(ctx, tx)
}

func (tmsm *tracingMetadataStoreMiddleware) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.Bucket, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".HeadBucket")
	defer span.End()

	return tmsm.innerMetadataStore.HeadBucket(ctx, tx, bucketName)
}

func (tmsm *tracingMetadataStoreMiddleware) ListObjects(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectsOptions) (*metadatastore.ListBucketResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".ListObjects")
	defer span.End()

	return tmsm.innerMetadataStore.ListObjects(ctx, tx, bucketName, opts)
}

func (tmsm *tracingMetadataStoreMiddleware) HeadObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) (*metadatastore.Object, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".HeadObject")
	defer span.End()

	return tmsm.innerMetadataStore.HeadObject(ctx, tx, bucketName, key)
}

func (tmsm *tracingMetadataStoreMiddleware) PutObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, object *metadatastore.Object) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".PutObject")
	defer span.End()

	return tmsm.innerMetadataStore.PutObject(ctx, tx, bucketName, object)
}

func (tmsm *tracingMetadataStoreMiddleware) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".DeleteObject")
	defer span.End()

	return tmsm.innerMetadataStore.DeleteObject(ctx, tx, bucketName, key)
}

func (tmsm *tracingMetadataStoreMiddleware) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, contentType *string, checksumType *string) (*metadatastore.InitiateMultipartUploadResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".CreateMultipartUpload")
	defer span.End()

	return tmsm.innerMetadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, contentType, checksumType)
}

func (tmsm *tracingMetadataStoreMiddleware) UploadPart(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, partNumber int32, blob metadatastore.Blob) error {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".UploadPart")
	defer span.End()

	return tmsm.innerMetadataStore.UploadPart(ctx, tx, bucketName, key, uploadId, partNumber, blob)
}

func (tmsm *tracingMetadataStoreMiddleware) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, checksumInput *metadatastore.ChecksumInput) (*metadatastore.CompleteMultipartUploadResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".CompleteMultipartUpload")
	defer span.End()

	return tmsm.innerMetadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, uploadId, checksumInput)
}

func (tmsm *tracingMetadataStoreMiddleware) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId) (*metadatastore.AbortMultipartResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".AbortMultipartUpload")
	defer span.End()

	return tmsm.innerMetadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, uploadId)
}

func (tmsm *tracingMetadataStoreMiddleware) ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListMultipartUploadsOptions) (*metadatastore.ListMultipartUploadsResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".ListMultipartUploads")
	defer span.End()

	return tmsm.innerMetadataStore.ListMultipartUploads(ctx, tx, bucketName, opts)
}

func (tmsm *tracingMetadataStoreMiddleware) ListParts(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, opts metadatastore.ListPartsOptions) (*metadatastore.ListPartsResult, error) {
	ctx, span := tmsm.tracer.Start(ctx, tmsm.regionName+".ListParts")
	defer span.End()

	return tmsm.innerMetadataStore.ListParts(ctx, tx, bucketName, key, uploadId, opts)
}
