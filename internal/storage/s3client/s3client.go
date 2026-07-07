package s3client

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type s3ClientStorage struct {
	*lifecycle.ValidatedLifecycle
	s3Client *s3.Client
	tracer   trace.Tracer
}

// Compile-time check to ensure s3ClientStorage implements storage.Storage
var _ storage.Storage = (*s3ClientStorage)(nil)

func NewStorage(s3Client *s3.Client) (storage.Storage, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("S3ClientStorage")
	if err != nil {
		return nil, err
	}

	return &s3ClientStorage{
		ValidatedLifecycle: lifecycle,
		s3Client:           s3Client,
		tracer:             otel.Tracer("internal/storage/s3client"),
	}, nil
}

func (rs *s3ClientStorage) Start(ctx context.Context) error {
	return rs.ValidatedLifecycle.Start(ctx)
}

func (rs *s3ClientStorage) Stop(ctx context.Context) error {
	return rs.ValidatedLifecycle.Stop(ctx)
}

func (rs *s3ClientStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CreateBucket")
	defer span.End()

	_, err := rs.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName.String()),
	})
	var bucketAlreadyExistsError *types.BucketAlreadyExists
	if err != nil && errors.As(err, &bucketAlreadyExistsError) {
		return storage.ErrBucketAlreadyExists
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteBucket")
	defer span.End()

	_, err := rs.s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "BucketNotEmpty" {
		return storage.ErrBucketNotEmpty
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListBuckets")
	defer span.End()

	listBucketsResult, err := rs.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucket types.Bucket) storage.Bucket {
		return storage.Bucket{
			Name:         storage.MustNewBucketName(*bucket.Name),
			CreationDate: *bucket.CreationDate,
		}
	}, listBucketsResult.Buckets)
	return buckets, nil
}

func (rs *s3ClientStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.HeadBucket")
	defer span.End()

	_, err := rs.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName.String()),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.Bucket{
		Name:         bucketName,
		CreationDate: time.Time{},
	}, nil
}

func (rs *s3ClientStorage) GetBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetBucketVersioningConfiguration")
	defer span.End()

	result, err := rs.s3Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucketName.String())})
	var notFoundError *types.NotFound
	var noSuchBucketError *types.NoSuchBucket
	var apiErr smithy.APIError
	if err != nil && (errors.As(err, &notFoundError) || errors.As(err, &noSuchBucketError) || (errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchBucket" || apiErr.ErrorCode() == "NotFound"))) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	if result.Status == "" {
		return &storage.BucketVersioningConfiguration{}, nil
	}
	status := storage.BucketVersioningStatus(result.Status)
	return &storage.BucketVersioningConfiguration{Status: &status}, nil
}

func (rs *s3ClientStorage) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutBucketVersioningConfiguration")
	defer span.End()

	status := types.BucketVersioningStatusSuspended
	if config != nil && config.Status != nil && *config.Status == storage.BucketVersioningStatusEnabled {
		status = types.BucketVersioningStatusEnabled
	}
	_, err := rs.s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucketName.String()), VersioningConfiguration: &types.VersioningConfiguration{Status: status}})
	return err
}

func (rs *s3ClientStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListObjects")
	defer span.End()

	listObjectsResult, err := rs.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucketName.String()),
		Prefix:     opts.Prefix,
		Delimiter:  opts.Delimiter,
		StartAfter: opts.StartAfter,
		MaxKeys:    aws.Int32(opts.MaxKeys),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	objects := sliceutils.Map(func(object types.Object) storage.Object {
		// S3 list responses only carry the checksum type and algorithm, not
		// the checksum values themselves.
		var checksumType *string
		if object.ChecksumType != "" {
			checksumType = (*string)(&object.ChecksumType)
		}
		return storage.Object{
			Key:          storage.MustNewObjectKey(*object.Key),
			LastModified: *object.LastModified,
			ETag:         *object.ETag,
			ChecksumType: checksumType,
			Size:         *object.Size,
			StorageClass: storageClassFromAWS(object.StorageClass),
		}
	}, listObjectsResult.Contents)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listObjectsResult.CommonPrefixes)
	return &storage.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    *listObjectsResult.IsTruncated,
	}, nil
}

func (rs *s3ClientStorage) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListObjectVersions")
	defer span.End()

	result, err := rs.s3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucketName.String()),
		Prefix:          opts.Prefix,
		Delimiter:       opts.Delimiter,
		KeyMarker:       opts.KeyMarker,
		VersionIdMarker: opts.VersionIDMarker,
		MaxKeys:         aws.Int32(opts.MaxKeys),
	})
	if err != nil {
		return nil, err
	}

	versions := []storage.ObjectVersion{}
	for _, version := range result.Versions {
		if version.Key == nil || version.VersionId == nil || version.LastModified == nil {
			continue
		}
		versions = append(versions, storage.ObjectVersion{Key: storage.MustNewObjectKey(*version.Key), VersionID: *version.VersionId, IsDeleteMarker: false, IsLatest: aws.ToBool(version.IsLatest), LastModified: *version.LastModified, Size: aws.ToInt64(version.Size), ETag: version.ETag, StorageClass: storageClassFromAWS(version.StorageClass)})
	}
	for _, marker := range result.DeleteMarkers {
		if marker.Key == nil || marker.VersionId == nil || marker.LastModified == nil {
			continue
		}
		versions = append(versions, storage.ObjectVersion{Key: storage.MustNewObjectKey(*marker.Key), VersionID: *marker.VersionId, IsDeleteMarker: true, IsLatest: aws.ToBool(marker.IsLatest), LastModified: *marker.LastModified})
	}

	return &storage.ListObjectVersionsResult{Versions: versions, IsTruncated: aws.ToBool(result.IsTruncated), NextKeyMarker: result.NextKeyMarker, NextVersionIDMarker: result.NextVersionIdMarker}, nil
}

func (rs *s3ClientStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.HeadObject")
	defer span.End()

	headObjectResult, err := rs.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
		VersionId: func() *string {
			if opts != nil {
				return opts.VersionID
			}
			return nil
		}(),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	var userMetadata map[string]string
	if len(headObjectResult.Metadata) > 0 {
		userMetadata = headObjectResult.Metadata
	}
	return &storage.Object{
		Key:               key,
		ContentType:       headObjectResult.ContentType,
		LastModified:      *headObjectResult.LastModified,
		VersionID:         headObjectResult.VersionId,
		IsDeleteMarker:    aws.ToBool(headObjectResult.DeleteMarker),
		ETag:              *headObjectResult.ETag,
		ChecksumCRC32:     headObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    headObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: headObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      headObjectResult.ChecksumSHA1,
		ChecksumSHA256:    headObjectResult.ChecksumSHA256,
		ChecksumType:      (*string)(&headObjectResult.ChecksumType),
		Size:              *headObjectResult.ContentLength,
		StorageClass:      storageClassFromAWS(headObjectResult.StorageClass),
		Metadata: storage.ObjectMetadata{
			CacheControl:            headObjectResult.CacheControl,
			ContentDisposition:      headObjectResult.ContentDisposition,
			ContentEncoding:         headObjectResult.ContentEncoding,
			ContentLanguage:         headObjectResult.ContentLanguage,
			Expires:                 headObjectResult.ExpiresString,
			WebsiteRedirectLocation: headObjectResult.WebsiteRedirectLocation,
			UserMetadata:            userMetadata,
		},
	}, nil
}

func (rs *s3ClientStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetObject")
	defer span.End()

	// If no ranges specified, get the entire object
	if len(ranges) == 0 {
		ranges = []storage.ByteRange{{Start: nil, End: nil}}
	}

	// First, get object metadata
	object, err := rs.HeadObject(ctx, bucketName, key, nil)
	if err != nil {
		return nil, nil, err
	}

	// Get each range
	readers := []io.ReadCloser{}
	for _, byteRange := range ranges {
		startByte := byteRange.Start
		endByte := byteRange.End

		var awsRange *string = nil
		if startByte != nil || endByte != nil {
			r := byteRangeToAWSRange(byteRange)
			awsRange = &r
		}
		getObjectResult, err := rs.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName.String()),
			Key:    aws.String(key.String()),
			Range:  awsRange,
			VersionId: func() *string {
				if opts != nil {
					return opts.VersionID
				}
				return nil
			}(),
		})
		var notFoundError *types.NotFound
		if err != nil && errors.As(err, &notFoundError) {
			// Close any readers we've already opened
			for _, r := range readers {
				r.Close()
			}
			return nil, nil, storage.ErrNoSuchBucket
		}
		if err != nil {
			// Close any readers we've already opened
			for _, r := range readers {
				r.Close()
			}
			return nil, nil, err
		}
		readers = append(readers, getObjectResult.Body)
	}

	return object, readers, nil
}

// checksumAlgorithmFromInput maps the x-amz-sdk-checksum-algorithm value of a
// request onto the SDK enum, so the backing S3 validates the payload with the
// same algorithm the client asked for. The algorithm is only forwarded
// together with a precomputed checksum value: without one, the SDK would
// compute the checksum itself, which requires a seekable stream when the
// backend connection cannot use trailing checksums.
func checksumAlgorithmFromInput(checksumInput *storage.ChecksumInput) types.ChecksumAlgorithm {
	if checksumInput == nil || checksumInput.ChecksumAlgorithm == nil {
		return ""
	}
	hasChecksumValue := checksumInput.ChecksumCRC32 != nil ||
		checksumInput.ChecksumCRC32C != nil ||
		checksumInput.ChecksumCRC64NVME != nil ||
		checksumInput.ChecksumSHA1 != nil ||
		checksumInput.ChecksumSHA256 != nil
	if !hasChecksumValue {
		return ""
	}
	return types.ChecksumAlgorithm(strings.ToUpper(*checksumInput.ChecksumAlgorithm))
}

// contentMD5FromETag converts the quoted-hex ETag form that a Content-MD5
// header was parsed into back to the base64 form the SDK expects. Values that
// are not a hex-encoded MD5 digest are dropped.
func contentMD5FromETag(etag *string) *string {
	if etag == nil {
		return nil
	}
	digest, err := hex.DecodeString(strings.Trim(*etag, "\""))
	if err != nil || len(digest) != md5.Size {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString(digest)
	return &encoded
}

// storageClassFromAWS converts an AWS SDK storage class enum (each API uses
// its own string type) into the internal representation, mapping the SDK's
// zero value ("not present") to nil.
func storageClassFromAWS[T ~string](storageClass T) *string {
	if storageClass == "" {
		return nil
	}
	value := string(storageClass)
	return &value
}

// parseExpires parses the stored raw Expires header value into a time.Time for
// the AWS SDK, which only accepts a parsed timestamp on requests. Unparseable
// values are dropped.
func parseExpires(expires *string) *time.Time {
	if expires == nil {
		return nil
	}
	if t, err := http.ParseTime(*expires); err == nil {
		return &t
	}
	return nil
}

func (rs *s3ClientStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutObject")
	defer span.End()

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName.String()),
		Key:         aws.String(key.String()),
		ContentType: contentType,
		Body:        reader,
		IfMatch: func() *string {
			if opts != nil {
				return opts.IfMatchETag
			}
			return nil
		}(),
		IfNoneMatch: func() *string {
			if opts != nil && opts.IfNoneMatchStar {
				return aws.String("*")
			}
			return nil
		}(),
	}
	if checksumInput != nil {
		input.ChecksumAlgorithm = checksumAlgorithmFromInput(checksumInput)
		input.ContentMD5 = contentMD5FromETag(checksumInput.ETag)
		input.ChecksumCRC32 = checksumInput.ChecksumCRC32
		input.ChecksumCRC32C = checksumInput.ChecksumCRC32C
		input.ChecksumCRC64NVME = checksumInput.ChecksumCRC64NVME
		input.ChecksumSHA1 = checksumInput.ChecksumSHA1
		input.ChecksumSHA256 = checksumInput.ChecksumSHA256
	}
	if opts != nil && opts.Metadata != nil {
		input.CacheControl = opts.Metadata.CacheControl
		input.ContentDisposition = opts.Metadata.ContentDisposition
		input.ContentEncoding = opts.Metadata.ContentEncoding
		input.ContentLanguage = opts.Metadata.ContentLanguage
		input.Expires = parseExpires(opts.Metadata.Expires)
		input.WebsiteRedirectLocation = opts.Metadata.WebsiteRedirectLocation
		input.Metadata = opts.Metadata.UserMetadata
	}
	if opts != nil && opts.StorageClass != nil {
		input.StorageClass = types.StorageClass(*opts.StorageClass)
	}
	putObjectResult, err := rs.s3Client.PutObject(ctx, input)
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
			return nil, storage.ErrPreconditionFailed
		}
		return nil, err
	}

	return &storage.PutObjectResult{
		ETag:              putObjectResult.ETag,
		ChecksumCRC32:     putObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    putObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: putObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      putObjectResult.ChecksumSHA1,
		ChecksumSHA256:    putObjectResult.ChecksumSHA256,
	}, nil
}

// byteRangeToAWSRange converts a storage.ByteRange (exclusive end) into an S3
// "bytes=start-end" (inclusive end) header value, mirroring GetObject.
func byteRangeToAWSRange(byteRange storage.ByteRange) string {
	if byteRange.Start != nil && byteRange.End != nil {
		return fmt.Sprintf("bytes=%d-%d", *byteRange.Start, *byteRange.End-1)
	}
	if byteRange.Start != nil {
		return fmt.Sprintf("bytes=%d-", *byteRange.Start)
	}
	if byteRange.End != nil {
		return fmt.Sprintf("bytes=-%d", *byteRange.End)
	}
	return ""
}

func copySourceValue(srcBucket storage.BucketName, srcKey storage.ObjectKey, sourceVersionID *string) string {
	value := srcBucket.String() + "/" + url.PathEscape(srcKey.String())
	if sourceVersionID != nil {
		value += "?versionId=" + url.QueryEscape(*sourceVersionID)
	}
	return value
}

func translateS3CopyError(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchBucket":
			return storage.ErrNoSuchBucket
		case "NoSuchKey":
			return storage.ErrNoSuchKey
		case "PreconditionFailed":
			return storage.ErrPreconditionFailed
		}
	}
	var notFoundError *types.NotFound
	if errors.As(err, &notFoundError) {
		return storage.ErrNoSuchBucket
	}
	var noSuchKeyError *types.NoSuchKey
	if errors.As(err, &noSuchKeyError) {
		return storage.ErrNoSuchKey
	}
	return err
}

func (rs *s3ClientStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CopyObject")
	defer span.End()

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket.String()),
		Key:        aws.String(dstKey.String()),
		CopySource: aws.String(copySourceValue(srcBucket, srcKey, nil)),
	}
	if opts != nil {
		input.CopySource = aws.String(copySourceValue(srcBucket, srcKey, opts.SourceVersionID))
		// Ranged CopyObject is a pithos extension that AWS CopyObject cannot
		// express (ranges only exist for UploadPartCopy), so it cannot be forwarded
		// to a remote S3 backend.
		if opts.Range != nil {
			return nil, storage.ErrNotImplemented
		}
		if opts.ReplaceMetadata {
			input.MetadataDirective = types.MetadataDirectiveReplace
			input.ContentType = opts.ContentType
			if opts.Metadata != nil {
				input.CacheControl = opts.Metadata.CacheControl
				input.ContentDisposition = opts.Metadata.ContentDisposition
				input.ContentEncoding = opts.Metadata.ContentEncoding
				input.ContentLanguage = opts.Metadata.ContentLanguage
				input.Expires = parseExpires(opts.Metadata.Expires)
				input.Metadata = opts.Metadata.UserMetadata
			}
		}
		// The website redirect location is never copied from the source; it
		// applies whenever it is supplied on the copy request, regardless of the
		// metadata directive.
		if opts.Metadata != nil {
			input.WebsiteRedirectLocation = opts.Metadata.WebsiteRedirectLocation
		}
		if opts.StorageClass != nil {
			input.StorageClass = types.StorageClass(*opts.StorageClass)
		}
		input.CopySourceIfMatch = opts.CopySourceConditions.IfMatch
		input.CopySourceIfNoneMatch = opts.CopySourceConditions.IfNoneMatch
		input.CopySourceIfModifiedSince = opts.CopySourceConditions.IfModifiedSince
		input.CopySourceIfUnmodifiedSince = opts.CopySourceConditions.IfUnmodifiedSince
	}

	copyObjectResult, err := rs.s3Client.CopyObject(ctx, input)
	if err != nil {
		return nil, translateS3CopyError(err)
	}

	result := &storage.CopyObjectResult{}
	result.VersionID = copyObjectResult.VersionId
	result.SourceVersionID = copyObjectResult.CopySourceVersionId
	if copyObjectResult.CopyObjectResult != nil {
		if copyObjectResult.CopyObjectResult.ETag != nil {
			result.ETag = *copyObjectResult.CopyObjectResult.ETag
		}
		if copyObjectResult.CopyObjectResult.LastModified != nil {
			result.LastModified = *copyObjectResult.CopyObjectResult.LastModified
		}
	}
	return result, nil
}

func (rs *s3ClientStorage) AppendObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, _ io.Reader, _ *storage.ChecksumInput, _ *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	return nil, storage.ErrNotImplemented
}

func (rs *s3ClientStorage) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.TransitionObjectStorageClass")
	defer span.End()

	if opts != nil && opts.VersionID != nil {
		return storage.ErrNotImplemented
	}

	// A remote S3 backend changes an object's storage class via an in-place
	// self copy that keeps the existing metadata.
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName.String()),
		Key:               aws.String(key.String()),
		CopySource:        aws.String(copySourceValue(bucketName, key, nil)),
		MetadataDirective: types.MetadataDirectiveCopy,
		StorageClass:      types.StorageClass(targetStorageClass),
	}
	if opts != nil && opts.IfMatchETag != nil {
		input.CopySourceIfMatch = opts.IfMatchETag
	}
	if _, err := rs.s3Client.CopyObject(ctx, input); err != nil {
		return translateS3CopyError(err)
	}
	return nil
}

func (rs *s3ClientStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteObject")
	defer span.End()

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
	}
	if opts != nil && opts.IfMatchETag != nil {
		input.IfMatch = opts.IfMatchETag
	}
	if opts != nil && opts.VersionID != nil {
		input.VersionId = opts.VersionID
	}
	result, err := rs.s3Client.DeleteObject(ctx, input)
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.DeleteObjectResult{VersionID: result.VersionId, IsDeleteMarker: aws.ToBool(result.DeleteMarker)}, nil
}

func (rs *s3ClientStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteObjects")
	defer span.End()

	identifiers := make([]types.ObjectIdentifier, len(entries))
	for i, entry := range entries {
		k := entry.Key.String()
		identifiers[i] = types.ObjectIdentifier{Key: &k, VersionId: entry.VersionID}
	}

	deleteResult, err := rs.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName.String()),
		Delete: &types.Delete{
			Objects: identifiers,
			Quiet:   aws.Bool(false),
		},
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	result := &storage.DeleteObjectsResult{
		Entries: make([]storage.DeleteObjectsEntry, 0, len(entries)),
	}

	for _, deleted := range deleteResult.Deleted {
		key := storage.MustNewObjectKey(*deleted.Key)
		result.Entries = append(result.Entries, storage.DeleteObjectsEntry{Key: key, VersionID: deleted.VersionId, DeleteMarkerVersionID: deleted.DeleteMarkerVersionId, DeleteMarker: deleted.DeleteMarker, Deleted: true})
	}
	for _, errEntry := range deleteResult.Errors {
		key := storage.MustNewObjectKey(*errEntry.Key)
		code := ""
		msg := ""
		if errEntry.Code != nil {
			code = *errEntry.Code
		}
		if errEntry.Message != nil {
			msg = *errEntry.Message
		}
		result.Entries = append(result.Entries, storage.DeleteObjectsEntry{Key: key, VersionID: errEntry.VersionId, Deleted: false, ErrCode: code, ErrMsg: msg})
	}

	return result, nil
}

func (rs *s3ClientStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CreateMultipartUpload")
	defer span.End()

	checksumTypeStr := types.ChecksumTypeFullObject
	if checksumType != nil {
		checksumTypeStr = types.ChecksumType(*checksumType)
	}
	var tagging *string
	if opts != nil && len(opts.Tags) > 0 {
		values := url.Values{}
		for k, v := range opts.Tags {
			values.Set(k, v)
		}
		tagging = aws.String(values.Encode())
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucketName.String()),
		Key:          aws.String(key.String()),
		ContentType:  contentType,
		ChecksumType: checksumTypeStr,
		Tagging:      tagging,
	}
	if opts != nil && opts.Metadata != nil {
		input.CacheControl = opts.Metadata.CacheControl
		input.ContentDisposition = opts.Metadata.ContentDisposition
		input.ContentEncoding = opts.Metadata.ContentEncoding
		input.ContentLanguage = opts.Metadata.ContentLanguage
		input.Expires = parseExpires(opts.Metadata.Expires)
		input.WebsiteRedirectLocation = opts.Metadata.WebsiteRedirectLocation
		input.Metadata = opts.Metadata.UserMetadata
	}
	if opts != nil && opts.StorageClass != nil {
		input.StorageClass = types.StorageClass(*opts.StorageClass)
	}
	initiateMultipartUploadResult, err := rs.s3Client.CreateMultipartUpload(ctx, input)
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.InitiateMultipartUploadResult{
		UploadId: storage.MustNewUploadId(*initiateMultipartUploadResult.UploadId),
	}, nil
}

func (rs *s3ClientStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.UploadPart")
	defer span.End()

	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucketName.String()),
		Key:        aws.String(key.String()),
		UploadId:   aws.String(uploadId.String()),
		PartNumber: aws.Int32(partNumber),
		Body:       data,
	}
	if checksumInput != nil {
		input.ChecksumAlgorithm = checksumAlgorithmFromInput(checksumInput)
		input.ContentMD5 = contentMD5FromETag(checksumInput.ETag)
		input.ChecksumCRC32 = checksumInput.ChecksumCRC32
		input.ChecksumCRC32C = checksumInput.ChecksumCRC32C
		input.ChecksumCRC64NVME = checksumInput.ChecksumCRC64NVME
		input.ChecksumSHA1 = checksumInput.ChecksumSHA1
		input.ChecksumSHA256 = checksumInput.ChecksumSHA256
	}
	uploadPartResult, err := rs.s3Client.UploadPart(ctx, input)
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.UploadPartResult{
		ETag:              *uploadPartResult.ETag,
		ChecksumCRC32:     uploadPartResult.ChecksumCRC32,
		ChecksumCRC32C:    uploadPartResult.ChecksumCRC32C,
		ChecksumCRC64NVME: uploadPartResult.ChecksumCRC64NVME,
		ChecksumSHA1:      uploadPartResult.ChecksumSHA1,
		ChecksumSHA256:    uploadPartResult.ChecksumSHA256,
	}, nil
}

func (rs *s3ClientStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.UploadPartCopy")
	defer span.End()

	input := &s3.UploadPartCopyInput{
		Bucket:     aws.String(dstBucket.String()),
		Key:        aws.String(dstKey.String()),
		UploadId:   aws.String(uploadId.String()),
		PartNumber: aws.Int32(partNumber),
		CopySource: aws.String(copySourceValue(srcBucket, srcKey, nil)),
	}
	if opts != nil {
		input.CopySource = aws.String(copySourceValue(srcBucket, srcKey, opts.SourceVersionID))
		if opts.Range != nil {
			input.CopySourceRange = aws.String(byteRangeToAWSRange(*opts.Range))
		}
		input.CopySourceIfMatch = opts.CopySourceConditions.IfMatch
		input.CopySourceIfNoneMatch = opts.CopySourceConditions.IfNoneMatch
		input.CopySourceIfModifiedSince = opts.CopySourceConditions.IfModifiedSince
		input.CopySourceIfUnmodifiedSince = opts.CopySourceConditions.IfUnmodifiedSince
	}

	uploadPartCopyResult, err := rs.s3Client.UploadPartCopy(ctx, input)
	if err != nil {
		return nil, translateS3CopyError(err)
	}

	result := &storage.UploadPartCopyResult{}
	result.SourceVersionID = uploadPartCopyResult.CopySourceVersionId
	if uploadPartCopyResult.CopyPartResult != nil {
		if uploadPartCopyResult.CopyPartResult.ETag != nil {
			result.ETag = *uploadPartCopyResult.CopyPartResult.ETag
		}
		if uploadPartCopyResult.CopyPartResult.LastModified != nil {
			result.LastModified = *uploadPartCopyResult.CopyPartResult.LastModified
		}
	}
	return result, nil
}

func mapCompleteMultipartUploadParts(parts []storage.CompleteMultipartUploadPart) []types.CompletedPart {
	completedParts := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		completedParts = append(completedParts, types.CompletedPart{
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			ETag:              aws.String(part.ETag),
			PartNumber:        aws.Int32(part.PartNumber),
		})
	}
	return completedParts
}

func (rs *s3ClientStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.CompleteMultipartUpload")
	defer span.End()

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName.String()),
		Key:      aws.String(key.String()),
		UploadId: aws.String(uploadId.String()),
	}
	if checksumInput != nil {
		if checksumInput.ChecksumType != nil {
			input.ChecksumType = types.ChecksumType(strings.ToUpper(*checksumInput.ChecksumType))
		}
		input.ChecksumCRC32 = checksumInput.ChecksumCRC32
		input.ChecksumCRC32C = checksumInput.ChecksumCRC32C
		input.ChecksumCRC64NVME = checksumInput.ChecksumCRC64NVME
		input.ChecksumSHA1 = checksumInput.ChecksumSHA1
		input.ChecksumSHA256 = checksumInput.ChecksumSHA256
	}
	if opts != nil && len(opts.Parts) > 0 {
		input.MultipartUpload = &types.CompletedMultipartUpload{
			Parts: mapCompleteMultipartUploadParts(opts.Parts),
		}
	}
	completeMultipartUploadResult, err := rs.s3Client.CompleteMultipartUpload(ctx, input)
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.CompleteMultipartUploadResult{
		Location:          *completeMultipartUploadResult.Location,
		VersionID:         completeMultipartUploadResult.VersionId,
		ETag:              *completeMultipartUploadResult.ETag,
		ChecksumCRC32:     completeMultipartUploadResult.ChecksumCRC32,
		ChecksumCRC32C:    completeMultipartUploadResult.ChecksumCRC32C,
		ChecksumCRC64NVME: completeMultipartUploadResult.ChecksumCRC64NVME,
		ChecksumSHA1:      completeMultipartUploadResult.ChecksumSHA1,
		ChecksumSHA256:    completeMultipartUploadResult.ChecksumSHA256,
		ChecksumType:      (*string)(&completeMultipartUploadResult.ChecksumType),
	}, nil
}

func (rs *s3ClientStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.AbortMultipartUpload")
	defer span.End()

	_, err := rs.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName.String()),
		Key:      aws.String(key.String()),
		UploadId: aws.String(uploadId.String()),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListMultipartUploads")
	defer span.End()

	listMultipartUploadsResult, err := rs.s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucketName.String()),
		Prefix:         opts.Prefix,
		Delimiter:      opts.Delimiter,
		KeyMarker:      opts.KeyMarker,
		UploadIdMarker: opts.UploadIdMarker,
		MaxUploads:     aws.Int32(opts.MaxUploads),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	uploads := sliceutils.Map(func(upload types.MultipartUpload) storage.Upload {
		return storage.Upload{
			Key:          storage.MustNewObjectKey(*upload.Key),
			UploadId:     storage.MustNewUploadId(*upload.UploadId),
			Initiated:    *upload.Initiated,
			StorageClass: storageClassFromAWS(upload.StorageClass),
		}
	}, listMultipartUploadsResult.Uploads)
	commonPrefixes := sliceutils.Map(func(commonPrefix types.CommonPrefix) string {
		return *commonPrefix.Prefix
	}, listMultipartUploadsResult.CommonPrefixes)
	return &storage.ListMultipartUploadsResult{
		BucketName:         storage.MustNewBucketName(*listMultipartUploadsResult.Bucket),
		KeyMarker:          *listMultipartUploadsResult.KeyMarker,
		UploadIdMarker:     *listMultipartUploadsResult.UploadIdMarker,
		Prefix:             *listMultipartUploadsResult.Prefix,
		Delimiter:          *listMultipartUploadsResult.Delimiter,
		NextKeyMarker:      *listMultipartUploadsResult.NextKeyMarker,
		NextUploadIdMarker: *listMultipartUploadsResult.NextUploadIdMarker,
		MaxUploads:         *listMultipartUploadsResult.MaxUploads,
		CommonPrefixes:     commonPrefixes,
		Uploads:            uploads,
		IsTruncated:        *listMultipartUploadsResult.IsTruncated,
	}, nil
}

func (rs *s3ClientStorage) ListParts(ctx context.Context, bucketName storage.BucketName, objectName storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.ListParts")
	defer span.End()

	listPartsResult, err := rs.s3Client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:           aws.String(bucketName.String()),
		Key:              aws.String(objectName.String()),
		UploadId:         aws.String(uploadID.String()),
		PartNumberMarker: opts.PartNumberMarker,
		MaxParts:         aws.Int32(opts.MaxParts),
	})
	var notFoundError *types.NotFound
	if err != nil && errors.As(err, &notFoundError) {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}
	return &storage.ListPartsResult{
		BucketName:           storage.MustNewBucketName(*listPartsResult.Bucket),
		Key:                  storage.MustNewObjectKey(*listPartsResult.Key),
		UploadId:             storage.MustNewUploadId(*listPartsResult.UploadId),
		PartNumberMarker:     *listPartsResult.PartNumberMarker,
		NextPartNumberMarker: listPartsResult.NextPartNumberMarker,
		MaxParts:             *listPartsResult.MaxParts,
		IsTruncated:          *listPartsResult.IsTruncated,
		Parts: sliceutils.Map(func(part types.Part) *storage.MultipartPart {
			return &storage.MultipartPart{
				ETag:              *part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      *part.LastModified,
				PartNumber:        *part.PartNumber,
				Size:              *part.Size,
			}
		}, listPartsResult.Parts),
		StorageClass: storageClassFromAWS(listPartsResult.StorageClass),
	}, nil
}

func (rs *s3ClientStorage) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetBucketWebsiteConfiguration")
	defer span.End()

	result, err := rs.s3Client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchWebsiteConfiguration" {
		return nil, storage.ErrNoSuchWebsiteConfiguration
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	config := &storage.WebsiteConfiguration{}
	if result.IndexDocument != nil && result.IndexDocument.Suffix != nil {
		config.IndexDocumentSuffix = *result.IndexDocument.Suffix
	}
	if result.ErrorDocument != nil && result.ErrorDocument.Key != nil {
		config.ErrorDocumentKey = result.ErrorDocument.Key
	}
	if result.RedirectAllRequestsTo != nil && result.RedirectAllRequestsTo.HostName != nil {
		config.RedirectAllRequestsTo = &storage.WebsiteRedirectAllRequestsTo{
			HostName: *result.RedirectAllRequestsTo.HostName,
		}
		if result.RedirectAllRequestsTo.Protocol != "" {
			protocol := string(result.RedirectAllRequestsTo.Protocol)
			config.RedirectAllRequestsTo.Protocol = &protocol
		}
	}
	for _, rule := range result.RoutingRules {
		if rule.Redirect == nil {
			continue
		}
		storageRule := storage.WebsiteRoutingRule{
			Redirect: storage.WebsiteRedirect{
				HostName:             rule.Redirect.HostName,
				ReplaceKeyPrefixWith: rule.Redirect.ReplaceKeyPrefixWith,
				ReplaceKeyWith:       rule.Redirect.ReplaceKeyWith,
				HttpRedirectCode:     rule.Redirect.HttpRedirectCode,
			},
		}
		if rule.Redirect.Protocol != "" {
			protocol := string(rule.Redirect.Protocol)
			storageRule.Redirect.Protocol = &protocol
		}
		if rule.Condition != nil {
			storageRule.Condition = &storage.WebsiteRoutingRuleCondition{
				KeyPrefixEquals:             rule.Condition.KeyPrefixEquals,
				HttpErrorCodeReturnedEquals: rule.Condition.HttpErrorCodeReturnedEquals,
			}
		}
		config.RoutingRules = append(config.RoutingRules, storageRule)
	}
	return config, nil
}

func (rs *s3ClientStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutBucketWebsiteConfiguration")
	defer span.End()

	websiteConfig := &types.WebsiteConfiguration{}
	if config.IndexDocumentSuffix != "" {
		websiteConfig.IndexDocument = &types.IndexDocument{
			Suffix: aws.String(config.IndexDocumentSuffix),
		}
	}
	if config.ErrorDocumentKey != nil {
		websiteConfig.ErrorDocument = &types.ErrorDocument{
			Key: config.ErrorDocumentKey,
		}
	}
	if config.RedirectAllRequestsTo != nil {
		websiteConfig.RedirectAllRequestsTo = &types.RedirectAllRequestsTo{
			HostName: aws.String(config.RedirectAllRequestsTo.HostName),
		}
		if config.RedirectAllRequestsTo.Protocol != nil {
			websiteConfig.RedirectAllRequestsTo.Protocol = types.Protocol(*config.RedirectAllRequestsTo.Protocol)
		}
	}
	for _, rule := range config.RoutingRules {
		routingRule := types.RoutingRule{
			Redirect: &types.Redirect{
				HostName:             rule.Redirect.HostName,
				ReplaceKeyPrefixWith: rule.Redirect.ReplaceKeyPrefixWith,
				ReplaceKeyWith:       rule.Redirect.ReplaceKeyWith,
				HttpRedirectCode:     rule.Redirect.HttpRedirectCode,
			},
		}
		if rule.Redirect.Protocol != nil {
			routingRule.Redirect.Protocol = types.Protocol(*rule.Redirect.Protocol)
		}
		if rule.Condition != nil {
			routingRule.Condition = &types.Condition{
				KeyPrefixEquals:             rule.Condition.KeyPrefixEquals,
				HttpErrorCodeReturnedEquals: rule.Condition.HttpErrorCodeReturnedEquals,
			}
		}
		websiteConfig.RoutingRules = append(websiteConfig.RoutingRules, routingRule)
	}

	_, err := rs.s3Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
		Bucket:               aws.String(bucketName.String()),
		WebsiteConfiguration: websiteConfig,
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteBucketWebsiteConfiguration")
	defer span.End()

	_, err := rs.s3Client.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetBucketCORSConfiguration")
	defer span.End()

	result, err := rs.s3Client.GetBucketCors(ctx, &s3.GetBucketCorsInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchCORSConfiguration" {
		return nil, storage.ErrNoSuchCORSConfiguration
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	rules := make([]storage.CORSRule, 0, len(result.CORSRules))
	for _, rule := range result.CORSRules {
		var maxAge *int
		if rule.MaxAgeSeconds != nil {
			maxAgeValue := int(*rule.MaxAgeSeconds)
			maxAge = &maxAgeValue
		}
		rules = append(rules, storage.CORSRule{
			ID:             rule.ID,
			AllowedOrigins: rule.AllowedOrigins,
			AllowedMethods: rule.AllowedMethods,
			AllowedHeaders: rule.AllowedHeaders,
			ExposeHeaders:  rule.ExposeHeaders,
			MaxAgeSeconds:  maxAge,
		})
	}

	return &storage.BucketCORSConfiguration{Rules: rules}, nil
}

func (rs *s3ClientStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutBucketCORSConfiguration")
	defer span.End()

	rules := make([]types.CORSRule, 0, len(config.Rules))
	for _, rule := range config.Rules {
		var maxAge *int32
		if rule.MaxAgeSeconds != nil {
			maxAgeValue := int32(*rule.MaxAgeSeconds)
			maxAge = &maxAgeValue
		}
		rules = append(rules, types.CORSRule{
			ID:             rule.ID,
			AllowedOrigins: rule.AllowedOrigins,
			AllowedMethods: rule.AllowedMethods,
			AllowedHeaders: rule.AllowedHeaders,
			ExposeHeaders:  rule.ExposeHeaders,
			MaxAgeSeconds:  maxAge,
		})
	}

	_, err := rs.s3Client.PutBucketCors(ctx, &s3.PutBucketCorsInput{
		Bucket: aws.String(bucketName.String()),
		CORSConfiguration: &types.CORSConfiguration{
			CORSRules: rules,
		},
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteBucketCORSConfiguration")
	defer span.End()

	_, err := rs.s3Client.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func convertLifecycleTagFromSdk(tag *types.Tag) *storage.LifecycleTag {
	if tag == nil {
		return nil
	}
	return &storage.LifecycleTag{
		Key:   aws.ToString(tag.Key),
		Value: aws.ToString(tag.Value),
	}
}

func convertLifecycleTagToSdk(tag *storage.LifecycleTag) *types.Tag {
	if tag == nil {
		return nil
	}
	return &types.Tag{
		Key:   aws.String(tag.Key),
		Value: aws.String(tag.Value),
	}
}

func convertLifecycleTransitionsFromSdk(transitions []types.Transition) []storage.LifecycleTransition {
	if len(transitions) == 0 {
		return nil
	}

	converted := make([]storage.LifecycleTransition, 0, len(transitions))
	for _, transition := range transitions {
		converted = append(converted, storage.LifecycleTransition{
			Days:         transition.Days,
			Date:         transition.Date,
			StorageClass: string(transition.StorageClass),
		})
	}
	return converted
}

func convertLifecycleTransitionsToSdk(transitions []storage.LifecycleTransition) []types.Transition {
	if len(transitions) == 0 {
		return nil
	}

	converted := make([]types.Transition, 0, len(transitions))
	for _, transition := range transitions {
		converted = append(converted, types.Transition{
			Days:         transition.Days,
			Date:         transition.Date,
			StorageClass: types.TransitionStorageClass(transition.StorageClass),
		})
	}
	return converted
}

func convertLifecycleNoncurrentVersionTransitionsFromSdk(transitions []types.NoncurrentVersionTransition) []storage.LifecycleNoncurrentVersionTransition {
	if len(transitions) == 0 {
		return nil
	}

	converted := make([]storage.LifecycleNoncurrentVersionTransition, 0, len(transitions))
	for _, transition := range transitions {
		converted = append(converted, storage.LifecycleNoncurrentVersionTransition{
			NoncurrentDays:          transition.NoncurrentDays,
			NewerNoncurrentVersions: transition.NewerNoncurrentVersions,
			StorageClass:            string(transition.StorageClass),
		})
	}
	return converted
}

func convertLifecycleNoncurrentVersionTransitionsToSdk(transitions []storage.LifecycleNoncurrentVersionTransition) []types.NoncurrentVersionTransition {
	if len(transitions) == 0 {
		return nil
	}

	converted := make([]types.NoncurrentVersionTransition, 0, len(transitions))
	for _, transition := range transitions {
		converted = append(converted, types.NoncurrentVersionTransition{
			NoncurrentDays:          transition.NoncurrentDays,
			NewerNoncurrentVersions: transition.NewerNoncurrentVersions,
			StorageClass:            types.TransitionStorageClass(transition.StorageClass),
		})
	}
	return converted
}

func convertLifecycleRuleFromSdk(rule types.LifecycleRule) storage.LifecycleRule {
	converted := storage.LifecycleRule{
		ID:                           rule.ID,
		Status:                       string(rule.Status),
		Prefix:                       rule.Prefix,
		Transitions:                  convertLifecycleTransitionsFromSdk(rule.Transitions),
		NoncurrentVersionTransitions: convertLifecycleNoncurrentVersionTransitionsFromSdk(rule.NoncurrentVersionTransitions),
	}
	if rule.Filter != nil {
		filter := &storage.LifecycleFilter{
			Prefix:                rule.Filter.Prefix,
			Tag:                   convertLifecycleTagFromSdk(rule.Filter.Tag),
			ObjectSizeGreaterThan: rule.Filter.ObjectSizeGreaterThan,
			ObjectSizeLessThan:    rule.Filter.ObjectSizeLessThan,
		}
		if rule.Filter.And != nil {
			and := &storage.LifecycleFilterAnd{
				Prefix:                rule.Filter.And.Prefix,
				ObjectSizeGreaterThan: rule.Filter.And.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    rule.Filter.And.ObjectSizeLessThan,
			}
			for _, tag := range rule.Filter.And.Tags {
				and.Tags = append(and.Tags, *convertLifecycleTagFromSdk(&tag))
			}
			filter.And = and
		}
		converted.Filter = filter
	}
	if rule.Expiration != nil {
		converted.Expiration = &storage.LifecycleExpiration{
			Days:                      rule.Expiration.Days,
			Date:                      rule.Expiration.Date,
			ExpiredObjectDeleteMarker: rule.Expiration.ExpiredObjectDeleteMarker,
		}
	}
	if rule.AbortIncompleteMultipartUpload != nil {
		converted.AbortIncompleteMultipartUpload = &storage.LifecycleAbortIncompleteMultipartUpload{
			DaysAfterInitiation: rule.AbortIncompleteMultipartUpload.DaysAfterInitiation,
		}
	}
	if rule.NoncurrentVersionExpiration != nil {
		converted.NoncurrentVersionExpiration = &storage.LifecycleNoncurrentVersionExpiration{
			NoncurrentDays:          rule.NoncurrentVersionExpiration.NoncurrentDays,
			NewerNoncurrentVersions: rule.NoncurrentVersionExpiration.NewerNoncurrentVersions,
		}
	}
	return converted
}

func convertLifecycleRuleToSdk(rule storage.LifecycleRule) types.LifecycleRule {
	converted := types.LifecycleRule{
		ID:                           rule.ID,
		Status:                       types.ExpirationStatus(rule.Status),
		Prefix:                       rule.Prefix,
		Transitions:                  convertLifecycleTransitionsToSdk(rule.Transitions),
		NoncurrentVersionTransitions: convertLifecycleNoncurrentVersionTransitionsToSdk(rule.NoncurrentVersionTransitions),
	}
	if rule.Filter != nil {
		filter := &types.LifecycleRuleFilter{
			Prefix:                rule.Filter.Prefix,
			Tag:                   convertLifecycleTagToSdk(rule.Filter.Tag),
			ObjectSizeGreaterThan: rule.Filter.ObjectSizeGreaterThan,
			ObjectSizeLessThan:    rule.Filter.ObjectSizeLessThan,
		}
		if rule.Filter.And != nil {
			and := &types.LifecycleRuleAndOperator{
				Prefix:                rule.Filter.And.Prefix,
				ObjectSizeGreaterThan: rule.Filter.And.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    rule.Filter.And.ObjectSizeLessThan,
			}
			for _, tag := range rule.Filter.And.Tags {
				and.Tags = append(and.Tags, *convertLifecycleTagToSdk(&tag))
			}
			filter.And = and
		}
		converted.Filter = filter
	}
	if rule.Expiration != nil {
		converted.Expiration = &types.LifecycleExpiration{
			Days:                      rule.Expiration.Days,
			Date:                      rule.Expiration.Date,
			ExpiredObjectDeleteMarker: rule.Expiration.ExpiredObjectDeleteMarker,
		}
	}
	if rule.AbortIncompleteMultipartUpload != nil {
		converted.AbortIncompleteMultipartUpload = &types.AbortIncompleteMultipartUpload{
			DaysAfterInitiation: rule.AbortIncompleteMultipartUpload.DaysAfterInitiation,
		}
	}
	if rule.NoncurrentVersionExpiration != nil {
		converted.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{
			NoncurrentDays:          rule.NoncurrentVersionExpiration.NoncurrentDays,
			NewerNoncurrentVersions: rule.NoncurrentVersionExpiration.NewerNoncurrentVersions,
		}
	}
	return converted
}

func (rs *s3ClientStorage) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetBucketLifecycleConfiguration")
	defer span.End()

	result, err := rs.s3Client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchLifecycleConfiguration" {
		return nil, storage.ErrNoSuchLifecycleConfiguration
	}
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return nil, storage.ErrNoSuchBucket
	}
	if err != nil {
		return nil, err
	}

	rules := make([]storage.LifecycleRule, 0, len(result.Rules))
	for _, rule := range result.Rules {
		rules = append(rules, convertLifecycleRuleFromSdk(rule))
	}

	return &storage.BucketLifecycleConfiguration{Rules: rules}, nil
}

func (rs *s3ClientStorage) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutBucketLifecycleConfiguration")
	defer span.End()

	rules := make([]types.LifecycleRule, 0, len(config.Rules))
	for _, rule := range config.Rules {
		rules = append(rules, convertLifecycleRuleToSdk(rule))
	}

	_, err := rs.s3Client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName.String()),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteBucketLifecycleConfiguration")
	defer span.End()

	_, err := rs.s3Client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucketName.String()),
	})
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchBucket" {
		return storage.ErrNoSuchBucket
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.GetObjectTagging")
	defer span.End()

	input := &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
	}
	if opts != nil {
		input.VersionId = opts.VersionID
	}
	result, err := rs.s3Client.GetObjectTagging(ctx, input)
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchKey" {
		return nil, storage.ErrNoSuchKey
	}
	if err != nil {
		return nil, err
	}

	tags := map[string]string{}
	for _, t := range result.TagSet {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return tags, nil
}

func (rs *s3ClientStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.PutObjectTagging")
	defer span.End()

	tagSet := make([]types.Tag, 0, len(tags))
	for k, v := range tags {
		tagSet = append(tagSet, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}

	input := &s3.PutObjectTaggingInput{
		Bucket:  aws.String(bucketName.String()),
		Key:     aws.String(key.String()),
		Tagging: &types.Tagging{TagSet: tagSet},
	}
	if opts != nil {
		input.VersionId = opts.VersionID
	}
	_, err := rs.s3Client.PutObjectTagging(ctx, input)
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchKey" {
		return storage.ErrNoSuchKey
	}
	if err != nil {
		return err
	}
	return nil
}

func (rs *s3ClientStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	ctx, span := rs.tracer.Start(ctx, "S3ClientStorage.DeleteObjectTagging")
	defer span.End()

	input := &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucketName.String()),
		Key:    aws.String(key.String()),
	}
	if opts != nil {
		input.VersionId = opts.VersionID
	}
	_, err := rs.s3Client.DeleteObjectTagging(ctx, input)
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) && ae.ErrorCode() == "NoSuchKey" {
		return storage.ErrNoSuchKey
	}
	if err != nil {
		return err
	}
	return nil
}
