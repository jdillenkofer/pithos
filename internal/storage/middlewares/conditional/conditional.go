package conditional

import (
	"context"
	"database/sql"
	"io"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
)

const maxCopyMemoryCacheSize = 10 * 1000 * 1000

type conditionalStorageMiddleware struct {
	*lifecycle.ValidatedLifecycle
	delegator.DelegatingStorage
	bucketToStorageMap map[string]storage.Storage
	tracer             trace.Tracer
}

// Compile-time check to ensure conditionalStorageMiddleware implements storage.Storage
var _ storage.Storage = (*conditionalStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*conditionalStorageMiddleware)(nil)

func (csm *conditionalStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, csm.Next, csm, fn)
}

func NewStorageMiddleware(bucketToStorageMap map[string]storage.Storage, defaultStorage storage.Storage) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("ConditionalStorageMiddleware")
	if err != nil {
		return nil, err
	}

	return &conditionalStorageMiddleware{
		ValidatedLifecycle: lifecycle,
		DelegatingStorage:  delegator.Wrap(defaultStorage),
		bucketToStorageMap: bucketToStorageMap,
		tracer:             otel.Tracer("internal/storage/middlewares/conditional"),
	}, nil
}

func (csm *conditionalStorageMiddleware) Start(ctx context.Context) error {
	if err := csm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}

	for _, bucketStorage := range csm.bucketToStorageMap {
		if err := bucketStorage.Start(ctx); err != nil {
			return err
		}
	}

	return csm.Next.Start(ctx)
}

func (csm *conditionalStorageMiddleware) Stop(ctx context.Context) error {
	if err := csm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}

	for _, bucketStorage := range csm.bucketToStorageMap {
		if err := bucketStorage.Stop(ctx); err != nil {
			return err
		}
	}

	return csm.Next.Stop(ctx)
}

func (csm *conditionalStorageMiddleware) lookupStorage(bucketName storage.BucketName) storage.Storage {
	storage, ok := csm.bucketToStorageMap[bucketName.String()]
	if ok {
		return storage
	}
	return csm.Next
}

func (csm *conditionalStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CreateBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CreateBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListBuckets")
	defer span.End()

	allBuckets := []storage.Bucket{}

	// Include buckets from all specific storages
	for _, bucketStorage := range csm.bucketToStorageMap {
		buckets, err := bucketStorage.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}
		allBuckets = append(allBuckets, buckets...)
	}

	// Include buckets from default storage
	buckets, err := csm.Next.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	allBuckets = append(allBuckets, buckets...)

	slices.SortFunc(allBuckets, func(a storage.Bucket, b storage.Bucket) int { return strings.Compare(a.Name.String(), b.Name.String()) })
	return allBuckets, nil
}

func (csm *conditionalStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.HeadBucket")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.HeadBucket(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) GetBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetBucketVersioningConfiguration")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.GetBucketVersioningConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutBucketVersioningConfiguration")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.PutBucketVersioningConfiguration(ctx, bucketName, config)
}

func (csm *conditionalStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListObjects")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListObjects(ctx, bucketName, opts)
}

func (csm *conditionalStorageMiddleware) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListObjectVersions")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.ListObjectVersions(ctx, bucketName, opts)
}

func (csm *conditionalStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.HeadObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.HeadObject(ctx, bucketName, key, opts)
}

func (csm *conditionalStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.GetObject(ctx, bucketName, key, ranges, opts)
}

func (csm *conditionalStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.PutObject(ctx, bucketName, key, contentType, reader, checksumInput, opts)
}

func (csm *conditionalStorageMiddleware) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.TransitionObjectStorageClass")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.TransitionObjectStorageClass(ctx, bucketName, key, targetStorageClass, opts)
}

func copySourceConditionsSatisfied(conditions storage.CopySourceConditions, object *storage.Object) error {
	ifMatchPassed := false
	if conditions.IfMatch != nil {
		ifMatchPassed = *conditions.IfMatch == storage.ETagWildcard || *conditions.IfMatch == object.ETag
		if !ifMatchPassed {
			return storage.ErrPreconditionFailed
		}
	}
	if conditions.IfNoneMatch != nil {
		if *conditions.IfNoneMatch == storage.ETagWildcard || *conditions.IfNoneMatch == object.ETag {
			return storage.ErrPreconditionFailed
		}
	}
	lastModified := object.LastModified.Truncate(time.Second)
	if conditions.IfUnmodifiedSince != nil && !(conditions.IfMatch != nil && ifMatchPassed) && lastModified.After(*conditions.IfUnmodifiedSince) {
		return storage.ErrPreconditionFailed
	}
	if conditions.IfModifiedSince != nil && !lastModified.After(*conditions.IfModifiedSince) {
		return storage.ErrPreconditionFailed
	}
	return nil
}

func readSourceForCopy(ctx context.Context, srcStorage storage.Storage, srcBucket storage.BucketName, srcKey storage.ObjectKey, sourceVersionID *string, copyRange *storage.ByteRange, conditions storage.CopySourceConditions) (*storage.Object, []io.ReadCloser, error) {
	headOpts := &storage.HeadObjectOptions{VersionID: sourceVersionID}
	srcObject, err := srcStorage.HeadObject(ctx, srcBucket, srcKey, headOpts)
	if err != nil {
		return nil, nil, err
	}
	if err := copySourceConditionsSatisfied(conditions, srcObject); err != nil {
		return nil, nil, err
	}

	var ranges []storage.ByteRange
	if copyRange != nil {
		ranges = []storage.ByteRange{*copyRange}
	}
	getOpts := &storage.GetObjectOptions{VersionID: sourceVersionID, IfMatchETag: &srcObject.ETag}
	_, readers, err := srcStorage.GetObject(ctx, srcBucket, srcKey, ranges, getOpts)
	if err != nil {
		return nil, nil, err
	}
	return srcObject, readers, nil
}

func closeReaders(readers []io.ReadCloser) {
	for _, reader := range readers {
		_ = reader.Close()
	}
}

func multiReader(readers []io.ReadCloser) io.Reader {
	readerInterfaces := make([]io.Reader, len(readers))
	for i, reader := range readers {
		readerInterfaces[i] = reader
	}
	return io.MultiReader(readerInterfaces...)
}

func cachedCopyBody(readers []io.ReadCloser) (io.ReadSeekCloser, error) {
	return ioutils.NewSmartCachedReadSeekCloser(multiReader(readers), maxCopyMemoryCacheSize)
}

func (csm *conditionalStorageMiddleware) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CopyObject")
	defer span.End()

	srcStorage := csm.lookupStorage(srcBucket)
	dstStorage := csm.lookupStorage(dstBucket)
	if srcStorage == dstStorage {
		return dstStorage.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
	}

	var copyRange *storage.ByteRange
	var conditions storage.CopySourceConditions
	var sourceVersionID *string
	var contentType *string
	if opts != nil {
		sourceVersionID = opts.SourceVersionID
		copyRange = opts.Range
		conditions = opts.CopySourceConditions
	}
	srcObject, readers, err := readSourceForCopy(ctx, srcStorage, srcBucket, srcKey, sourceVersionID, copyRange, conditions)
	if err != nil {
		return nil, err
	}
	defer closeReaders(readers)

	if opts != nil && opts.ReplaceMetadata {
		contentType = opts.ContentType
	} else {
		contentType = srcObject.ContentType
	}
	body, err := cachedCopyBody(readers)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	putResult, err := dstStorage.PutObject(ctx, dstBucket, dstKey, contentType, body, nil, nil)
	if err != nil {
		return nil, err
	}

	result := &storage.CopyObjectResult{LastModified: time.Now()}
	if putResult.ETag != nil {
		result.ETag = *putResult.ETag
	}
	result.SourceVersionID = srcObject.VersionID
	result.VersionID = putResult.VersionID
	return result, nil
}

func (csm *conditionalStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.AppendObject")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.AppendObject(ctx, bucketName, key, reader, checksumInput, opts)
}

func (csm *conditionalStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteObject")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteObject(ctx, bucketName, key, opts)
}

func (csm *conditionalStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteObjects")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.DeleteObjects(ctx, bucketName, entries)
}

func (csm *conditionalStorageMiddleware) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetObjectTagging")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.GetObjectTagging(ctx, bucketName, key, opts)
}

func (csm *conditionalStorageMiddleware) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutObjectTagging")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.PutObjectTagging(ctx, bucketName, key, tags, opts)
}

func (csm *conditionalStorageMiddleware) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteObjectTagging")
	defer span.End()

	s := csm.lookupStorage(bucketName)
	return s.DeleteObjectTagging(ctx, bucketName, key, opts)
}

func (csm *conditionalStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CreateMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
}

func (csm *conditionalStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.UploadPart")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.UploadPart(ctx, bucketName, key, uploadId, partNumber, reader, checksumInput)
}

func (csm *conditionalStorageMiddleware) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.UploadPartCopy")
	defer span.End()

	srcStorage := csm.lookupStorage(srcBucket)
	dstStorage := csm.lookupStorage(dstBucket)
	if srcStorage == dstStorage {
		return dstStorage.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
	}

	var copyRange *storage.ByteRange
	var conditions storage.CopySourceConditions
	var sourceVersionID *string
	if opts != nil {
		sourceVersionID = opts.SourceVersionID
		copyRange = opts.Range
		conditions = opts.CopySourceConditions
	}
	srcObject, readers, err := readSourceForCopy(ctx, srcStorage, srcBucket, srcKey, sourceVersionID, copyRange, conditions)
	if err != nil {
		return nil, err
	}
	defer closeReaders(readers)

	body, err := cachedCopyBody(readers)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	uploadResult, err := dstStorage.UploadPart(ctx, dstBucket, dstKey, uploadId, partNumber, body, nil)
	if err != nil {
		return nil, err
	}
	return &storage.UploadPartCopyResult{
		ETag:            uploadResult.ETag,
		LastModified:    time.Now(),
		SourceVersionID: srcObject.VersionID,
	}, nil
}

func (csm *conditionalStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.CompleteMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
}

func (csm *conditionalStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.AbortMultipartUpload")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
}

func (csm *conditionalStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListMultipartUploads")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListMultipartUploads(ctx, bucketName, opts)
}

func (csm *conditionalStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.ListParts")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.ListParts(ctx, bucketName, key, uploadId, opts)
}

func (csm *conditionalStorageMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetBucketWebsiteConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.GetBucketWebsiteConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutBucketWebsiteConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.PutBucketWebsiteConfiguration(ctx, bucketName, config)
}

func (csm *conditionalStorageMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteBucketWebsiteConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucketWebsiteConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetBucketCORSConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.GetBucketCORSConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutBucketCORSConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.PutBucketCORSConfiguration(ctx, bucketName, config)
}

func (csm *conditionalStorageMiddleware) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteBucketCORSConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucketCORSConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.GetBucketLifecycleConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.GetBucketLifecycleConfiguration(ctx, bucketName)
}

func (csm *conditionalStorageMiddleware) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.PutBucketLifecycleConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.PutBucketLifecycleConfiguration(ctx, bucketName, config)
}

func (csm *conditionalStorageMiddleware) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := csm.tracer.Start(ctx, "ConditionalStorageMiddleware.DeleteBucketLifecycleConfiguration")
	defer span.End()

	storage := csm.lookupStorage(bucketName)
	return storage.DeleteBucketLifecycleConfiguration(ctx, bucketName)
}
