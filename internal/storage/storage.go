package storage

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

type Bucket struct {
	Name         BucketName
	CreationDate time.Time
}

type BucketVersioningStatus string

const (
	BucketVersioningStatusEnabled   BucketVersioningStatus = "Enabled"
	BucketVersioningStatusSuspended BucketVersioningStatus = "Suspended"
)

type BucketVersioningConfiguration struct {
	Status *BucketVersioningStatus
}

// ObjectMetadata holds the user-controllable object metadata: the
// user-modifiable system metadata headers and the user-defined x-amz-meta-*
// key/value pairs. Content-Type is tracked separately.
type ObjectMetadata = metadatastore.ObjectMetadata

type Object struct {
	Key               ObjectKey
	ContentType       *string
	LastModified      time.Time
	VersionID         *string
	IsDeleteMarker    bool
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
	Size              int64
	// StorageClass is the object's storage class label; nil means STANDARD.
	StorageClass *string
	// Tags holds the object's tag set as key/value pairs.
	Tags map[string]string
	// Metadata holds the user-controllable object metadata. Populated by
	// HeadObject and GetObject.
	Metadata ObjectMetadata
}

// ByteRange represents a byte range for GetObject operations.
// Start and End use exclusive end indexing (End points one past the last byte).
// Examples:
//   - Start=0, End=10: bytes 0-9 (10 bytes)
//   - Start=nil, End=nil: entire object
//   - Start=nil, End=500: suffix range - last 500 bytes (will be converted to absolute range)
//
// Suffix ranges (Start=nil, End=N) are converted to absolute ranges by the storage layer.
type ByteRange struct {
	Start *int64
	End   *int64
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type PutObjectResult struct {
	VersionID         *string
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

type PutObjectOptions struct {
	IfNoneMatchStar bool
	IfMatchETag     *string
	// Tags is the object's tag set, supplied via the x-amz-tagging header. It
	// replaces any previous tags on overwrite. Nil/empty means no tags.
	Tags map[string]string
	// Metadata is the object's user-controllable metadata, supplied via the
	// request headers. It replaces any previous metadata on overwrite. Nil
	// means no metadata.
	Metadata *ObjectMetadata
	// StorageClass is the object's storage class, supplied via the
	// x-amz-storage-class header. Nil means STANDARD.
	StorageClass *string
}

// CreateMultipartUploadOptions holds options for a CreateMultipartUpload
// operation. A nil options pointer is valid and means all defaults.
type CreateMultipartUploadOptions struct {
	// Tags is the object's tag set, supplied via the x-amz-tagging header. It is
	// applied to the object when the upload completes. Nil/empty means no tags.
	Tags map[string]string
	// Metadata is the object's user-controllable metadata, supplied via the
	// request headers. It is applied to the object when the upload completes.
	// Nil means no metadata.
	Metadata *ObjectMetadata
	// StorageClass is the storage class for the upload and the resulting
	// object, supplied via the x-amz-storage-class header. Nil means STANDARD.
	StorageClass *string
}

// AppendObjectOptions holds options for an AppendObject operation.
type AppendObjectOptions struct {
	// WriteOffset, when non-nil, specifies the expected current size of the
	// object in bytes. The append is only performed if the actual object size
	// matches this value; otherwise ErrInvalidWriteOffset is returned.
	// Set to 0 to create a new object (equivalent to x-amz-write-offset-bytes: 0).
	WriteOffset *int64
}

// AppendObjectResult is returned by a successful AppendObject operation.
type AppendObjectResult struct {
	ETag string
	// Size is the total size of the object after the append.
	Size int64
}

type CompleteMultipartUploadOptions = metadatastore.CompleteMultipartUploadOptions
type CompleteMultipartUploadPart = metadatastore.CompleteMultipartUploadPart

const ChecksumTypeFullObject = metadatastore.ChecksumTypeFullObject
const ChecksumTypeComposite = metadatastore.ChecksumTypeComposite

type DeleteObjectOptions struct {
	VersionID *string
	// IfMatchETag, when non-nil, requires the stored object's ETag to equal this
	// value before deleting; otherwise ErrPreconditionFailed is returned.
	// The special value "*" matches any existing object (i.e. HTTP If-Match: *),
	// returning ErrPreconditionFailed only when the object does not exist.
	IfMatchETag *string
}

type DeleteObjectResult struct {
	VersionID      *string
	IsDeleteMarker bool
}

// HeadObjectOptions holds conditional request options for HeadObject.
// ETag values must be bare hex strings (without surrounding quotes).
type HeadObjectOptions struct {
	VersionID *string
	// IfMatchETag, when non-nil, requires the stored object's ETag to match;
	// otherwise ErrPreconditionFailed is returned.
	// The special value "*" matches any existing object.
	IfMatchETag *string
	// IfNoneMatchETag, when non-nil, requires the stored object's ETag to differ;
	// otherwise ErrNotModified is returned.
	// The special value "*" matches any existing object.
	IfNoneMatchETag *string
}

// GetObjectOptions holds conditional request options for GetObject.
// ETag values must be bare hex strings (without surrounding quotes).
type GetObjectOptions struct {
	VersionID *string
	// IfMatchETag, when non-nil, requires the stored object's ETag to match;
	// otherwise ErrPreconditionFailed is returned.
	// The special value "*" matches any existing object.
	IfMatchETag *string
	// IfNoneMatchETag, when non-nil, requires the stored object's ETag to differ;
	// otherwise ErrNotModified is returned.
	// The special value "*" matches any existing object.
	IfNoneMatchETag *string
}

// CopySourceConditions holds the x-amz-copy-source-if-* preconditions that are
// evaluated against the source object of a server-side copy. A nil field means
// the corresponding condition is absent. A failed precondition surfaces as
// ErrPreconditionFailed.
type CopySourceConditions struct {
	// IfMatch requires the source ETag to equal this value (or "*" to match any
	// existing object).
	IfMatch *string
	// IfNoneMatch requires the source ETag to differ from this value ("*" matches
	// any existing object and therefore always fails).
	IfNoneMatch *string
	// IfModifiedSince requires the source to have been modified after this time.
	IfModifiedSince *time.Time
	// IfUnmodifiedSince requires the source to be unmodified since this time.
	IfUnmodifiedSince *time.Time
}

// CopyObjectOptions holds options for a server-side CopyObject. A nil pointer is
// equivalent to a plain COPY of the whole object with no preconditions.
type CopyObjectOptions struct {
	// ReplaceMetadata corresponds to x-amz-metadata-directive: REPLACE. When true,
	// ContentType and Metadata are used for the destination instead of the
	// source's content type and metadata.
	ReplaceMetadata bool
	// ContentType is the destination content type used when ReplaceMetadata is true.
	ContentType *string
	// Metadata is the destination object metadata used when ReplaceMetadata is
	// true. Its WebsiteRedirectLocation is applied even with the COPY directive,
	// because S3 never carries the redirect location over from the source.
	Metadata *ObjectMetadata
	// Range, when non-nil, copies only the given byte range of the source into a
	// single-part destination object.
	Range *ByteRange
	// CopySourceConditions holds preconditions evaluated against the source object.
	CopySourceConditions CopySourceConditions
	// ReplaceTags corresponds to x-amz-tagging-directive: REPLACE. When true, the
	// destination object's tags are set from Tags instead of being copied from the
	// source object.
	ReplaceTags bool
	// Tags is the destination tag set used when ReplaceTags is true.
	Tags map[string]string
	// StorageClass is the destination object's storage class, supplied via the
	// x-amz-storage-class header on the copy request. Nil means STANDARD; the
	// source object's class is never carried over (matching AWS, where the
	// storage class is not governed by x-amz-metadata-directive).
	StorageClass *string
}

type CopyObjectResult struct {
	ETag         string
	LastModified time.Time
	// VersionID is the version id of the newly created destination object when
	// the destination bucket has versioning enabled.
	VersionID *string
}

// UploadPartCopyOptions holds options for a server-side UploadPartCopy.
type UploadPartCopyOptions struct {
	// Range, when non-nil, copies only the given byte range of the source into the part.
	Range *ByteRange
	// CopySourceConditions holds preconditions evaluated against the source object.
	CopySourceConditions CopySourceConditions
}

type UploadPartCopyResult struct {
	ETag         string
	LastModified time.Time
}

type InitiateMultipartUploadResult struct {
	UploadId UploadId
}

type UploadPartResult struct {
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

type CompleteMultipartUploadResult struct {
	Location          string
	VersionID         *string
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
}

type Upload struct {
	Key       ObjectKey
	UploadId  UploadId
	Initiated time.Time
	// StorageClass is the class chosen at CreateMultipartUpload; nil means
	// STANDARD.
	StorageClass *string
}

type ListMultipartUploadsResult struct {
	BucketName         BucketName
	KeyMarker          string
	UploadIdMarker     string
	NextKeyMarker      string
	Prefix             string
	Delimiter          string
	NextUploadIdMarker string
	MaxUploads         int32
	CommonPrefixes     []string
	Uploads            []Upload
	IsTruncated        bool
}

type MultipartPart struct {
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	LastModified      time.Time
	PartNumber        int32
	Size              int64
}

type ListPartsResult struct {
	BucketName           BucketName
	Key                  ObjectKey
	UploadId             UploadId
	PartNumberMarker     string
	NextPartNumberMarker *string
	MaxParts             int32
	IsTruncated          bool
	Parts                []*MultipartPart
	// StorageClass is the class chosen at CreateMultipartUpload; nil means
	// STANDARD.
	StorageClass *string
}

// DeleteObjectsEntry represents the result for a single key in a DeleteObjects operation.
type DeleteObjectsEntry struct {
	VersionID             *string
	DeleteMarker          *bool
	DeleteMarkerVersionID *string
	Key                   ObjectKey
	Deleted               bool
	ErrCode               string
	ErrMsg                string
}

// DeleteObjectsResult is the per-key result of a bulk DeleteObjects operation.
type DeleteObjectsResult struct {
	Entries []DeleteObjectsEntry
}

// DeleteObjectsInputEntry represents a single entry in a bulk DeleteObjects request, optionally with a conditional ETag.
type DeleteObjectsInputEntry struct {
	Key         ObjectKey
	VersionID   *string
	IfMatchETag *string
}

type ObjectVersion struct {
	Key            ObjectKey
	VersionID      string
	IsDeleteMarker bool
	IsLatest       bool
	LastModified   time.Time
	Size           int64
	ETag           *string
	// StorageClass is the version's storage class label; nil means STANDARD.
	StorageClass *string
}

type ListObjectVersionsOptions struct {
	Prefix          *string
	Delimiter       *string
	KeyMarker       *string
	VersionIDMarker *string
	MaxKeys         int32
}

type ListObjectVersionsResult struct {
	Versions            []ObjectVersion
	CommonPrefixes      []string
	IsTruncated         bool
	NextKeyMarker       *string
	NextVersionIDMarker *string
}

type ChecksumInput = metadatastore.ChecksumInput
type ChecksumValues = metadatastore.ChecksumValues
type BucketName = metadatastore.BucketName
type ObjectKey = metadatastore.ObjectKey
type UploadId = metadatastore.UploadId

const ETagWildcard = metadatastore.ETagWildcard

const StorageClassStandard = metadatastore.StorageClassStandard

var IsValidStorageClass = metadatastore.IsValidStorageClass
var EffectiveStorageClass = metadatastore.EffectiveStorageClass

var NewBucketName = metadatastore.NewBucketName
var MustNewBucketName = metadatastore.MustNewBucketName

var NewObjectKey = metadatastore.NewObjectKey
var MustNewObjectKey = metadatastore.MustNewObjectKey

var NewUploadId = metadatastore.NewUploadId
var MustNewUploadId = metadatastore.MustNewUploadId

var ValidateChecksums = metadatastore.ValidateChecksums

var ErrNoSuchBucket error = metadatastore.ErrNoSuchBucket
var ErrBucketAlreadyExists error = metadatastore.ErrBucketAlreadyExists
var ErrBucketNotEmpty error = metadatastore.ErrBucketNotEmpty
var ErrNoSuchKey error = metadatastore.ErrNoSuchKey
var ErrBadDigest error = metadatastore.ErrBadDigest
var ErrInvalidPart error = metadatastore.ErrInvalidPart
var ErrInvalidPartOrder error = metadatastore.ErrInvalidPartOrder
var ErrNotImplemented error = metadatastore.ErrNotImplemented
var ErrEntityTooLarge error = metadatastore.ErrEntityTooLarge
var ErrPreconditionFailed error = metadatastore.ErrPreconditionFailed
var ErrNotModified error = metadatastore.ErrNotModified
var ErrInvalidBucketName error = metadatastore.ErrInvalidBucketName
var ErrInvalidObjectKey error = metadatastore.ErrInvalidObjectKey
var ErrInvalidUploadId error = metadatastore.ErrInvalidUploadId
var ErrInvalidRange error = errors.New("InvalidRange")
var ErrNoSuchWebsiteConfiguration error = metadatastore.ErrNoSuchWebsiteConfiguration
var ErrNoSuchCORSConfiguration error = metadatastore.ErrNoSuchCORSConfiguration
var ErrNoSuchLifecycleConfiguration error = metadatastore.ErrNoSuchLifecycleConfiguration
var ErrTooManyParts error = metadatastore.ErrTooManyParts
var ErrInvalidWriteOffset error = metadatastore.ErrInvalidWriteOffset
var ErrInvalidStorageClass error = metadatastore.ErrInvalidStorageClass
var ErrCASFailure error = metadatastore.ErrCASFailure

type CurrentDeleteMarkerError struct {
	VersionID string
}

func (e *CurrentDeleteMarkerError) Error() string {
	return ErrNoSuchKey.Error()
}

type VersionDeleteMarkerMethodNotAllowedError struct {
	VersionID    string
	LastModified time.Time
}

func (e *VersionDeleteMarkerMethodNotAllowedError) Error() string {
	return "MethodNotAllowed"
}

var MaxEntitySize int64 = 5 * 1000 * 1000 * 1000 // 5 GB

// ListObjectsOptions defines options for listing objects
type ListObjectsOptions struct {
	// Prefix limits the response to keys that begin with the specified prefix.
	Prefix *string
	// Delimiter is a character you use to group keys.
	Delimiter *string
	// StartAfter is where you want Amazon S3 to start the listing from. Amazon S3 starts listing after this specified key.
	StartAfter *string
	// MaxKeys sets the maximum number of keys returned in the response.
	MaxKeys int32
}

// ListMultipartUploadsOptions defines options for listing multipart uploads
type ListMultipartUploadsOptions struct {
	// Prefix limits the response to keys that begin with the specified prefix.
	Prefix *string
	// Delimiter is a character you use to group keys.
	Delimiter *string
	// KeyMarker specifies the key-marker query parameter in the request.
	KeyMarker *string
	// UploadIdMarker specifies the upload-id-marker query parameter in the request.
	UploadIdMarker *string
	// MaxUploads sets the maximum number of multipart uploads returned in the response.
	MaxUploads int32
}

// ListPartsOptions defines options for listing parts
type ListPartsOptions struct {
	// PartNumberMarker specifies the part-number-marker query parameter in the request.
	PartNumberMarker *string
	// MaxParts sets the maximum number of parts returned in the response.
	MaxParts int32
}

// BucketManager manages bucket operations
type BucketManager interface {
	CreateBucket(ctx context.Context, bucketName BucketName) error
	DeleteBucket(ctx context.Context, bucketName BucketName) error
	ListBuckets(ctx context.Context) ([]Bucket, error)
	HeadBucket(ctx context.Context, bucketName BucketName) (*Bucket, error)
	GetBucketVersioningConfiguration(ctx context.Context, bucketName BucketName) (*BucketVersioningConfiguration, error)
	PutBucketVersioningConfiguration(ctx context.Context, bucketName BucketName, config *BucketVersioningConfiguration) error
}

type WebsiteConfiguration = metadatastore.WebsiteConfiguration
type WebsiteRedirectAllRequestsTo = metadatastore.WebsiteRedirectAllRequestsTo
type WebsiteRoutingRule = metadatastore.WebsiteRoutingRule
type WebsiteRoutingRuleCondition = metadatastore.WebsiteRoutingRuleCondition
type WebsiteRedirect = metadatastore.WebsiteRedirect

type CORSRule = metadatastore.CORSRule

type BucketCORSConfiguration = metadatastore.BucketCORSConfiguration

const LifecycleRuleStatusEnabled = metadatastore.LifecycleRuleStatusEnabled
const LifecycleRuleStatusDisabled = metadatastore.LifecycleRuleStatusDisabled

type LifecycleTag = metadatastore.LifecycleTag
type LifecycleFilterAnd = metadatastore.LifecycleFilterAnd
type LifecycleFilter = metadatastore.LifecycleFilter
type LifecycleExpiration = metadatastore.LifecycleExpiration
type LifecycleAbortIncompleteMultipartUpload = metadatastore.LifecycleAbortIncompleteMultipartUpload
type LifecycleTransition = metadatastore.LifecycleTransition
type LifecycleRule = metadatastore.LifecycleRule
type BucketLifecycleConfiguration = metadatastore.BucketLifecycleConfiguration

type BucketWebsiteManager interface {
	GetBucketWebsiteConfiguration(ctx context.Context, bucketName BucketName) (*WebsiteConfiguration, error)
	PutBucketWebsiteConfiguration(ctx context.Context, bucketName BucketName, config *WebsiteConfiguration) error
	DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName BucketName) error
}

type BucketCORSManager interface {
	GetBucketCORSConfiguration(ctx context.Context, bucketName BucketName) (*BucketCORSConfiguration, error)
	PutBucketCORSConfiguration(ctx context.Context, bucketName BucketName, config *BucketCORSConfiguration) error
	DeleteBucketCORSConfiguration(ctx context.Context, bucketName BucketName) error
}

type BucketLifecycleManager interface {
	GetBucketLifecycleConfiguration(ctx context.Context, bucketName BucketName) (*BucketLifecycleConfiguration, error)
	PutBucketLifecycleConfiguration(ctx context.Context, bucketName BucketName, config *BucketLifecycleConfiguration) error
	DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName BucketName) error
}

// TaggingManager manages object tagging operations.
type TaggingManager interface {
	// GetObjectTagging returns the tag set of the object at key. Returns
	// ErrNoSuchKey if the object does not exist.
	GetObjectTagging(ctx context.Context, bucketName BucketName, key ObjectKey) (map[string]string, error)
	// PutObjectTagging replaces the entire tag set of the object at key. Returns
	// ErrNoSuchKey if the object does not exist.
	PutObjectTagging(ctx context.Context, bucketName BucketName, key ObjectKey, tags map[string]string) error
	// DeleteObjectTagging removes the entire tag set of the object at key.
	// Returns ErrNoSuchKey if the object does not exist.
	DeleteObjectTagging(ctx context.Context, bucketName BucketName, key ObjectKey) error
}

// ObjectManager manages object operations
type ObjectManager interface {
	ListObjects(ctx context.Context, bucketName BucketName, opts ListObjectsOptions) (*ListBucketResult, error)
	ListObjectVersions(ctx context.Context, bucketName BucketName, opts ListObjectVersionsOptions) (*ListObjectVersionsResult, error)
	HeadObject(ctx context.Context, bucketName BucketName, key ObjectKey, opts *HeadObjectOptions) (*Object, error)
	// GetObject retrieves an object with optional byte ranges.
	// If ranges is empty or nil, returns the entire object as a single reader.
	// All operations are performed in a single transaction to ensure consistency.
	// Returns the object metadata, a list of readers (one per range), and an error.
	GetObject(ctx context.Context, bucketName BucketName, key ObjectKey, ranges []ByteRange, opts *GetObjectOptions) (*Object, []io.ReadCloser, error)
	PutObject(ctx context.Context, bucketName BucketName, key ObjectKey, contentType *string, data io.Reader, checksumInput *ChecksumInput, opts *PutObjectOptions) (*PutObjectResult, error)
	// CopyObject performs a server-side copy of srcBucket/srcKey to dstBucket/dstKey.
	// The whole copy is performed in a single transaction; for a full (non-ranged)
	// copy the destination preserves the source's part structure and therefore its
	// exact ETag. opts may be nil.
	CopyObject(ctx context.Context, srcBucket BucketName, srcKey ObjectKey, dstBucket BucketName, dstKey ObjectKey, opts *CopyObjectOptions) (*CopyObjectResult, error)
	// AppendObject appends data to an existing object. If the object does not exist,
	// it behaves like PutObject and creates a new object with the provided data.
	// The ETag in the result reflects the new whole-object ETag after the append.
	AppendObject(ctx context.Context, bucketName BucketName, key ObjectKey, data io.Reader, checksumInput *ChecksumInput, opts *AppendObjectOptions) (*AppendObjectResult, error)
	DeleteObject(ctx context.Context, bucketName BucketName, key ObjectKey, opts *DeleteObjectOptions) (*DeleteObjectResult, error)
	DeleteObjects(ctx context.Context, bucketName BucketName, entries []DeleteObjectsInputEntry) (*DeleteObjectsResult, error)
	// TransitionObjectStorageClass changes the storage class of the current
	// object version at key, moving its part data to the part store mapped to
	// the target class. The object version and its metadata are preserved; the
	// object stays immediately readable (storage classes are labels, no
	// archive/restore). opts.IfMatchETag, when set, guards against a concurrent
	// replacement (ErrPreconditionFailed on mismatch). Returns ErrNoSuchKey
	// when no current object exists.
	TransitionObjectStorageClass(ctx context.Context, bucketName BucketName, key ObjectKey, targetStorageClass string, opts *TransitionObjectStorageClassOptions) error
}

// TransitionObjectStorageClassOptions holds options for
// TransitionObjectStorageClass. A nil pointer is equivalent to an
// unconditional transition.
type TransitionObjectStorageClassOptions struct {
	// IfMatchETag, when non-nil, requires the current object's ETag to equal
	// this value; otherwise ErrPreconditionFailed is returned.
	IfMatchETag *string
}

// MultipartUploadManager manages multipart upload operations
type MultipartUploadManager interface {
	CreateMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, contentType *string, checksumType *string, opts *CreateMultipartUploadOptions) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, partNumber int32, data io.Reader, checksumInput *ChecksumInput) (*UploadPartResult, error)
	// UploadPartCopy uploads a part of a multipart upload by copying data (optionally
	// a byte range) from an existing source object, server-side. opts may be nil.
	UploadPartCopy(ctx context.Context, srcBucket BucketName, srcKey ObjectKey, dstBucket BucketName, dstKey ObjectKey, uploadId UploadId, partNumber int32, opts *UploadPartCopyOptions) (*UploadPartCopyResult, error)
	CompleteMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, checksumInput *ChecksumInput, opts *CompleteMultipartUploadOptions) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId) error
	ListMultipartUploads(ctx context.Context, bucketName BucketName, opts ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, bucketName BucketName, key ObjectKey, uploadId UploadId, opts ListPartsOptions) (*ListPartsResult, error)
}

// Storage is a composite interface that combines all storage operations
type Storage interface {
	lifecycle.Manager
	BucketManager
	BucketWebsiteManager
	BucketCORSManager
	BucketLifecycleManager
	ObjectManager
	MultipartUploadManager
	TaggingManager
}

type TransactionalStorage interface {
	Storage
	WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage Storage) error) error
}

func ListAllObjectsOfBucket(ctx context.Context, storage Storage, bucketName BucketName) ([]Object, error) {
	allObjects := []Object{}
	var startAfter *string
	for {
		listBucketResult, err := storage.ListObjects(ctx, bucketName, ListObjectsOptions{
			StartAfter: startAfter,
			MaxKeys:    1000,
		})
		if err != nil {
			return nil, err
		}
		allObjects = append(allObjects, listBucketResult.Objects...)
		if !listBucketResult.IsTruncated {
			break
		}
		startAfter = ptrutils.ToPtr(listBucketResult.Objects[len(listBucketResult.Objects)-1].Key.String())
	}
	return allObjects, nil
}

func Tester(storage Storage, bucketNames []BucketName, content []byte) error {
	ctx := context.Background()
	err := storage.Start(ctx)
	if err != nil {
		return err
	}
	defer storage.Stop(ctx)

	for _, bucketName := range bucketNames {
		key := MustNewObjectKey("test")
		data := ioutils.NewByteReadSeekCloser(content)

		err = storage.CreateBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		bucket, err := storage.HeadBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		if !bucketName.Equals(bucket.Name) {
			return errors.New("invalid bucketName")
		}

		buckets, err := storage.ListBuckets(ctx)
		if err != nil {
			return err
		}

		if len(buckets) != 1 {
			return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
		}

		if !bucketName.Equals(buckets[0].Name) {
			return errors.New("invalid bucketName")
		}

		_, err = storage.PutObject(ctx, bucketName, key, nil, data, nil, nil)
		if err != nil {
			return err
		}

		object, err := storage.HeadObject(ctx, bucketName, key, nil)
		if err != nil {
			return err
		}

		if object.Size != int64(len(content)) {
			return errors.New("invalid part length")
		}

		listBucketResult, err := storage.ListObjects(ctx, bucketName, ListObjectsOptions{
			MaxKeys: 1000,
		})
		if err != nil {
			return err
		}

		if len(listBucketResult.Objects) != 1 {
			return errors.New("invalid objects length")
		}

		if !key.Equals(listBucketResult.Objects[0].Key) {
			return errors.New("invalid object key")
		}

		_, err = storage.DeleteObject(ctx, bucketName, key, nil)
		if err != nil {
			return err
		}

		initiateMultipartUploadResult, err := storage.CreateMultipartUpload(ctx, bucketName, key, nil, nil, nil)
		if err != nil {
			return err
		}

		_, err = data.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, nil)
		if err != nil {
			return err
		}

		_, err = storage.CompleteMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, nil, nil)
		if err != nil {
			return err
		}

		_, err = storage.DeleteObject(ctx, bucketName, key, nil)
		if err != nil {
			return err
		}

		initiateMultipartUploadResult, err = storage.CreateMultipartUpload(ctx, bucketName, key, nil, nil, nil)
		if err != nil {
			return err
		}

		_, err = data.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data, nil)
		if err != nil {
			return err
		}

		err = storage.AbortMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId)
		if err != nil {
			return err
		}

		err = storage.DeleteBucket(ctx, bucketName)
		if err != nil {
			return err
		}

		buckets, err = storage.ListBuckets(ctx)
		if err != nil {
			return err
		}

		if len(buckets) != 0 {
			return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
		}
	}

	return nil
}
