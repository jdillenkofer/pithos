package metadatastore

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type Bucket struct {
	Name         BucketName
	CreationDate time.Time
}

// ObjectMetadata holds the user-controllable object metadata: the
// user-modifiable system metadata headers and the user-defined x-amz-meta-*
// key/value pairs. Content-Type is tracked separately on Object.
type ObjectMetadata struct {
	CacheControl       *string
	ContentDisposition *string
	ContentEncoding    *string
	ContentLanguage    *string
	// Expires is stored as the raw header value (an HTTP date) and returned
	// verbatim, matching S3 behaviour.
	Expires                 *string
	WebsiteRedirectLocation *string
	// UserMetadata holds the x-amz-meta-* pairs; keys are stored lowercase
	// without the "x-amz-meta-" prefix.
	UserMetadata map[string]string
}

type Object struct {
	Key               ObjectKey
	ContentType       *string // only set in HeadObject and PutObject
	LastModified      time.Time
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
	Size              int64
	Parts             []Part
	// Tags holds the object's tag set as key/value pairs. It is populated by
	// HeadObject and ListObjects and applied (replacing any existing tags) by
	// PutObject.
	Tags map[string]string
	// Metadata holds the user-controllable object metadata. It is populated by
	// HeadObject and applied (replacing any existing metadata) by PutObject.
	Metadata ObjectMetadata
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type Part struct {
	Id                partstore.PartId
	Size              int64
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

type InitiateMultipartUploadResult struct {
	UploadId UploadId
}

type CompleteMultipartUploadResult struct {
	DeletedParts      []Part
	Location          string
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
}

type AbortMultipartResult struct {
	DeletedParts []Part
}

type Upload struct {
	Key       ObjectKey
	UploadId  UploadId
	Initiated time.Time
}

type ListMultipartUploadsResult struct {
	Bucket             BucketName
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
}

const ChecksumTypeFullObject = "FULL_OBJECT"
const ChecksumTypeComposite = "COMPOSITE"

type ChecksumInput struct {
	ChecksumType      *string
	ChecksumAlgorithm *string
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

type ChecksumValues = checksumutils.ChecksumValues

func ValidateChecksums(checksumInput *ChecksumInput, calculatedChecksums ChecksumValues) error {
	if checksumInput == nil {
		return nil
	}
	if checksumInput.ETag != nil && calculatedChecksums.ETag != nil {
		if *checksumInput.ETag != *calculatedChecksums.ETag {
			return ErrBadDigest
		}
	}
	if checksumInput.ChecksumCRC32 != nil && calculatedChecksums.ChecksumCRC32 != nil {
		if *checksumInput.ChecksumCRC32 != *calculatedChecksums.ChecksumCRC32 {
			return ErrBadDigest
		}
	}
	if checksumInput.ChecksumCRC32C != nil && calculatedChecksums.ChecksumCRC32C != nil {
		if *checksumInput.ChecksumCRC32C != *calculatedChecksums.ChecksumCRC32C {
			return ErrBadDigest
		}
	}
	if checksumInput.ChecksumCRC64NVME != nil && calculatedChecksums.ChecksumCRC64NVME != nil {
		if *checksumInput.ChecksumCRC64NVME != *calculatedChecksums.ChecksumCRC64NVME {
			return ErrBadDigest
		}
	}
	if checksumInput.ChecksumSHA1 != nil && calculatedChecksums.ChecksumSHA1 != nil {
		if *checksumInput.ChecksumSHA1 != *calculatedChecksums.ChecksumSHA1 {
			return ErrBadDigest
		}
	}
	if checksumInput.ChecksumSHA256 != nil && calculatedChecksums.ChecksumSHA256 != nil {
		if *checksumInput.ChecksumSHA256 != *calculatedChecksums.ChecksumSHA256 {
			return ErrBadDigest
		}
	}
	return nil
}

var ErrNoSuchBucket error = errors.New("NoSuchBucket")
var ErrBucketAlreadyExists error = errors.New("BucketAlreadyExists")
var ErrBucketNotEmpty error = errors.New("BucketNotEmpty")
var ErrNoSuchKey error = errors.New("NoSuchKey")
var ErrNoSuchUpload error = errors.New("NoSuchUpload")
var ErrBadDigest error = errors.New("BadDigest")
var ErrUploadWithInvalidSequenceNumber error = errors.New("UploadWithInvalidSequenceNumber")
var ErrNotImplemented error = errors.New("not implemented")
var ErrEntityTooLarge error = errors.New("EntityTooLarge")
var ErrPreconditionFailed error = errors.New("PreconditionFailed")
var ErrNotModified error = errors.New("NotModified")
var ErrNoSuchWebsiteConfiguration error = errors.New("NoSuchWebsiteConfiguration")
var ErrNoSuchCORSConfiguration error = errors.New("NoSuchCORSConfiguration")
var ErrNoSuchLifecycleConfiguration error = errors.New("NoSuchLifecycleConfiguration")
var ErrTooManyParts error = errors.New("TooManyParts")
var ErrInvalidWriteOffset error = errors.New("InvalidWriteOffset")

// ErrCASFailure is returned by the storage layer when a compare-and-swap
// (optimistic lock) operation fails because a concurrent writer modified the
// object between our read and our update. It is an internal error and should
// be mapped to the appropriate public API error by the caller.
var ErrCASFailure error = errors.New("CASFailure")

type ListObjectsOptions struct {
	Prefix        *string
	Delimiter     *string
	StartAfter    *string
	MaxKeys       int32
	SkipPartFetch bool
}

type ListMultipartUploadsOptions struct {
	Prefix         *string
	Delimiter      *string
	KeyMarker      *string
	UploadIdMarker *string
	MaxUploads     int32
}

type ListPartsOptions struct {
	PartNumberMarker *string
	MaxParts         int32
}

// ETagWildcard is the special If-Match / If-None-Match value that matches any
// object, as defined by the HTTP spec and used by S3.
const ETagWildcard = "*"

type PutObjectOptions struct {
	IfNoneMatchStar bool
	IfMatchETag     *string
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
}

// AppendObjectOptions holds options for an AppendObject operation.
type AppendObjectOptions struct {
	// WriteOffset, when non-nil, specifies the expected current size of the
	// object in bytes. The append is only performed if the actual object size
	// matches this value; otherwise ErrInvalidWriteOffset is returned.
	// Set to 0 to create a new object (equivalent to x-amz-write-offset-bytes: 0).
	WriteOffset *int64
}

type DeleteObjectOptions struct {
	// IfMatchETag, when non-nil, requires the stored object's ETag to equal this
	// value before deleting; otherwise ErrPreconditionFailed is returned.
	// The special value "*" matches any existing object (i.e. HTTP If-Match: *),
	// returning ErrPreconditionFailed only when the object does not exist.
	IfMatchETag *string
}

// CompleteMultipartUploadOptions holds conditional request options for CompleteMultipartUpload.
// AWS S3 behaviour: If-Match is checked against the ETag of the *current* object at the key
// (before it is replaced). If-None-Match "*" prevents the upload from completing when any
// object already exists at the key.
type CompleteMultipartUploadOptions struct {
	// IfMatchETag, when non-nil, requires the currently stored object's ETag to equal
	// this value; otherwise ErrPreconditionFailed is returned.
	// The special value "*" matches any existing object.
	IfMatchETag *string
	// IfNoneMatchStar, when true, requires that no object currently exists at the key;
	// otherwise ErrPreconditionFailed is returned (HTTP 412).
	IfNoneMatchStar bool
}

type MaintenanceStore interface {
	GetInUsePartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error)
}

type BucketStore interface {
	CreateBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
	DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
	ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error)
	HeadBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*Bucket, error)
}

type WebsiteConfiguration struct {
	IndexDocumentSuffix string
	ErrorDocumentKey    *string
}

type CORSRule struct {
	ID             *string
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	ExposeHeaders  []string
	MaxAgeSeconds  *int
}

type BucketCORSConfiguration struct {
	Rules []CORSRule
}

// LifecycleRuleStatus values as defined by the S3 API.
const (
	LifecycleRuleStatusEnabled  = "Enabled"
	LifecycleRuleStatusDisabled = "Disabled"
)

// LifecycleTag is a single tag predicate of a lifecycle rule filter.
type LifecycleTag struct {
	Key   string
	Value string
}

// LifecycleFilterAnd combines multiple lifecycle filter predicates; an object
// must satisfy all of them for the rule to apply.
type LifecycleFilterAnd struct {
	Prefix                *string
	Tags                  []LifecycleTag
	ObjectSizeGreaterThan *int64
	ObjectSizeLessThan    *int64
}

// LifecycleFilter selects the objects a lifecycle rule applies to. At most one
// of the fields may be set; combinations must be expressed via And. An empty
// filter applies the rule to all objects in the bucket.
type LifecycleFilter struct {
	Prefix                *string
	Tag                   *LifecycleTag
	ObjectSizeGreaterThan *int64
	ObjectSizeLessThan    *int64
	And                   *LifecycleFilterAnd
}

// LifecycleExpiration describes when objects expire. Exactly one of Days, Date
// or ExpiredObjectDeleteMarker is set.
type LifecycleExpiration struct {
	// Days is the number of days after object creation when the object expires.
	// Following S3 semantics, the effective expiry is rounded to the next
	// midnight UTC after creation + Days.
	Days *int32
	// Date is the absolute expiry time; it must be midnight UTC.
	Date *time.Time
	// ExpiredObjectDeleteMarker only has an effect on versioned buckets and is
	// accepted for compatibility; without versioning it is a no-op.
	ExpiredObjectDeleteMarker *bool
}

// LifecycleAbortIncompleteMultipartUpload aborts multipart uploads that were
// initiated more than DaysAfterInitiation days ago and never completed.
type LifecycleAbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int32
}

// LifecycleRule is a single rule of a bucket lifecycle configuration.
type LifecycleRule struct {
	ID     *string
	Status string
	// Prefix is the legacy top-level rule prefix (pre-Filter API). New
	// configurations use Filter instead; exactly one of the two is set.
	Prefix                         *string
	Filter                         *LifecycleFilter
	Expiration                     *LifecycleExpiration
	AbortIncompleteMultipartUpload *LifecycleAbortIncompleteMultipartUpload
}

type BucketLifecycleConfiguration struct {
	Rules []LifecycleRule
}

type BucketWebsiteStore interface {
	GetBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*WebsiteConfiguration, error)
	PutBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName, config *WebsiteConfiguration) error
	DeleteBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
}

type BucketCORSStore interface {
	GetBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*BucketCORSConfiguration, error)
	PutBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName, config *BucketCORSConfiguration) error
	DeleteBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
}

type BucketLifecycleStore interface {
	GetBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*BucketLifecycleConfiguration, error)
	PutBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName, config *BucketLifecycleConfiguration) error
	DeleteBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
}

type ObjectStore interface {
	ListObjects(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListObjectsOptions) (*ListBucketResult, error)
	HeadObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) (*Object, error)
	PutObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, object *Object, opts *PutObjectOptions) error
	// AppendObject appends a new part to an existing object's part list. The caller
	// must supply the updated object metadata (including new ETag, size, and the
	// full ordered part list). If no object exists at the key yet, a new object is
	// created. If WriteOffset is set in opts and does not match the current object
	// size, ErrInvalidWriteOffset is returned.
	AppendObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, object *Object, opts *AppendObjectOptions) error
	DeleteObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, opts *DeleteObjectOptions) error
	// GetObjectTagging returns the tag set of the object at key. Returns
	// ErrNoSuchKey if the object does not exist.
	GetObjectTagging(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) (map[string]string, error)
	// PutObjectTagging replaces the tag set of the object at key. Returns
	// ErrNoSuchKey if the object does not exist.
	PutObjectTagging(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, tags map[string]string) error
	// DeleteObjectTagging removes the entire tag set of the object at key.
	// Returns ErrNoSuchKey if the object does not exist.
	DeleteObjectTagging(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) error
}

type MultipartStore interface {
	CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, contentType *string, checksumType *string, opts *CreateMultipartUploadOptions) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, partNumber int32, part Part) error
	CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, checksumInput *ChecksumInput, opts *CompleteMultipartUploadOptions) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId) (*AbortMultipartResult, error)
	ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, opts ListPartsOptions) (*ListPartsResult, error)
}

type MetadataStore interface {
	lifecycle.Manager
	MaintenanceStore
	BucketStore
	BucketWebsiteStore
	BucketCORSStore
	BucketLifecycleStore
	ObjectStore
	MultipartStore
}

func runTesterTx(ctx context.Context, db database.Database, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	return database.WithTx(ctx, db, opts, func(ctx context.Context, tx database.Tx) error {
		return fn(tx.SqlTx())
	})
}

func Tester(metadataStore MetadataStore, db database.Database) error {
	ctx := context.Background()
	err := metadataStore.Start(ctx)
	if err != nil {
		return err
	}
	defer metadataStore.Stop(ctx)

	bucketName := MustNewBucketName("bucket")
	key := MustNewObjectKey("test")

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.CreateBucket(ctx, tx, bucketName)
	})
	if err != nil {
		return err
	}

	var bucket *Bucket
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		bucket, err = metadataStore.HeadBucket(ctx, tx, bucketName)
		return err
	})
	if err != nil {
		return err
	}

	if !bucketName.Equals(bucket.Name) {
		return errors.New("invalid bucketName")
	}

	var buckets []Bucket
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		buckets, err = metadataStore.ListBuckets(ctx, tx)
		return err
	})
	if err != nil {
		return err
	}

	if len(buckets) != 1 {
		return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
	}

	if !bucketName.Equals(buckets[0].Name) {
		return errors.New("invalid bucketName")
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.PutObject(ctx, tx, bucketName, &Object{
			Key:          key,
			LastModified: time.Now(),
			ETag:         "",
			Size:         0,
			Parts:        []Part{},
		}, nil)
	})
	if err != nil {
		return err
	}

	var object *Object
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		object, err = metadataStore.HeadObject(ctx, tx, bucketName, key)
		return err
	})
	if err != nil {
		return err
	}

	if len(object.Parts) != 0 {
		return errors.New("invalid part length")
	}

	var listBucketResult *ListBucketResult
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		listBucketResult, err = metadataStore.ListObjects(ctx, tx, bucketName, ListObjectsOptions{
			MaxKeys:       1000,
			SkipPartFetch: true,
		})
		return err
	})
	if err != nil {
		return err
	}

	if len(listBucketResult.Objects) != 1 {
		return errors.New("invalid objects length")
	}

	if key != listBucketResult.Objects[0].Key {
		return errors.New("invalid object key")
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.DeleteObject(ctx, tx, bucketName, key, nil)
	})
	if err != nil {
		return err
	}

	var initiateMultipartUploadResult *InitiateMultipartUploadResult
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		var err error
		initiateMultipartUploadResult, err = metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, nil, nil, nil)
		return err
	})
	if err != nil {
		return err
	}

	partId, err := partstore.NewRandomPartId()
	if err != nil {
		return err
	}
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Part{
			Id:   *partId,
			Size: 0,
			ETag: "",
		})
	})
	if err != nil {
		return err
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		_, err := metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, nil, nil)
		return err
	})
	if err != nil {
		return err
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.DeleteObject(ctx, tx, bucketName, key, nil)
	})
	if err != nil {
		return err
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		var err error
		initiateMultipartUploadResult, err = metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, nil, nil, nil)
		return err
	})
	if err != nil {
		return err
	}

	partId, err = partstore.NewRandomPartId()
	if err != nil {
		return err
	}
	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Part{
			Id:   *partId,
			Size: 0,
			ETag: "",
		})
	})
	if err != nil {
		return err
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		_, err := metadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId)
		return err
	})
	if err != nil {
		return err
	}

	// Multiple pending uploads can share the same key, so the
	// (keyMarker, uploadIdMarker) cursor must resume mid-key and return the
	// remaining uploads of the marker key before moving to greater keys.
	uploadKeys := []ObjectKey{
		MustNewObjectKey("upload-a"),
		MustNewObjectKey("upload-a"),
		MustNewObjectKey("upload-a"),
		MustNewObjectKey("upload-b"),
		MustNewObjectKey("upload-c"),
	}
	expectedUploads := []Upload{}
	for _, uploadKey := range uploadKeys {
		err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
			initiateResult, err := metadataStore.CreateMultipartUpload(ctx, tx, bucketName, uploadKey, nil, nil, nil)
			if err != nil {
				return err
			}
			expectedUploads = append(expectedUploads, Upload{Key: uploadKey, UploadId: initiateResult.UploadId})
			return nil
		})
		if err != nil {
			return err
		}
	}
	slices.SortFunc(expectedUploads, func(a, b Upload) int {
		if c := strings.Compare(a.Key.String(), b.Key.String()); c != 0 {
			return c
		}
		return strings.Compare(a.UploadId.String(), b.UploadId.String())
	})

	collectedUploads := []Upload{}
	keyMarker := ""
	uploadIdMarker := ""
	for page := 0; ; page++ {
		if page > len(expectedUploads) {
			return errors.New("multipart upload pagination did not terminate")
		}
		var listMultipartUploadsResult *ListMultipartUploadsResult
		err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
			var err error
			listMultipartUploadsResult, err = metadataStore.ListMultipartUploads(ctx, tx, bucketName, ListMultipartUploadsOptions{
				KeyMarker:      &keyMarker,
				UploadIdMarker: &uploadIdMarker,
				MaxUploads:     2,
			})
			return err
		})
		if err != nil {
			return err
		}
		if len(listMultipartUploadsResult.Uploads) > 2 {
			return errors.New("expected at most 2 uploads per page got " + strconv.Itoa(len(listMultipartUploadsResult.Uploads)))
		}
		collectedUploads = append(collectedUploads, listMultipartUploadsResult.Uploads...)
		if !listMultipartUploadsResult.IsTruncated {
			break
		}
		keyMarker = listMultipartUploadsResult.NextKeyMarker
		uploadIdMarker = listMultipartUploadsResult.NextUploadIdMarker
	}
	if len(collectedUploads) != len(expectedUploads) {
		return errors.New("expected " + strconv.Itoa(len(expectedUploads)) + " paginated uploads got " + strconv.Itoa(len(collectedUploads)))
	}
	for idx, expectedUpload := range expectedUploads {
		if !expectedUpload.Key.Equals(collectedUploads[idx].Key) || !expectedUpload.UploadId.Equals(collectedUploads[idx].UploadId) {
			return errors.New("unexpected upload at index " + strconv.Itoa(idx))
		}
	}

	for _, upload := range expectedUploads {
		err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
			_, err := metadataStore.AbortMultipartUpload(ctx, tx, bucketName, upload.Key, upload.UploadId)
			return err
		})
		if err != nil {
			return err
		}
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(tx *sql.Tx) error {
		return metadataStore.DeleteBucket(ctx, tx, bucketName)
	})
	if err != nil {
		return err
	}

	err = runTesterTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(tx *sql.Tx) error {
		var err error
		buckets, err = metadataStore.ListBuckets(ctx, tx)
		return err
	})
	if err != nil {
		return err
	}

	if len(buckets) != 0 {
		return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
	}

	return nil
}
