package metadatastore

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
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

type BucketVersioningStatus string

const (
	BucketVersioningStatusEnabled   BucketVersioningStatus = "Enabled"
	BucketVersioningStatusSuspended BucketVersioningStatus = "Suspended"
)

type BucketVersioningConfiguration struct {
	Status *BucketVersioningStatus
}

type Object struct {
	Key               ObjectKey
	ContentType       *string // only set in HeadObject and PutObject
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
	Parts             []Part
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

// AppendObjectOptions holds options for an AppendObject operation.
type AppendObjectOptions struct {
	// WriteOffset, when non-nil, specifies the expected current size of the
	// object in bytes. The append is only performed if the actual object size
	// matches this value; otherwise ErrInvalidWriteOffset is returned.
	// Set to 0 to create a new object (equivalent to x-amz-write-offset-bytes: 0).
	WriteOffset *int64
}

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

type ObjectVersion struct {
	Key            ObjectKey
	VersionID      string
	IsDeleteMarker bool
	IsLatest       bool
	LastModified   time.Time
	Size           int64
	ETag           *string
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
	GetBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*BucketVersioningConfiguration, error)
	PutBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName, config *BucketVersioningConfiguration) error
}

type WebsiteConfiguration struct {
	IndexDocumentSuffix string
	ErrorDocumentKey    *string
}

type BucketWebsiteStore interface {
	GetBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*WebsiteConfiguration, error)
	PutBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName, config *WebsiteConfiguration) error
	DeleteBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
}

type ObjectStore interface {
	ListObjects(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListObjectsOptions) (*ListBucketResult, error)
	ListObjectVersions(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListObjectVersionsOptions) (*ListObjectVersionsResult, error)
	HeadObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) (*Object, error)
	HeadObjectVersion(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, versionID string) (*Object, error)
	PutObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, object *Object, opts *PutObjectOptions) error
	// AppendObject appends a new part to an existing object's part list. The caller
	// must supply the updated object metadata (including new ETag, size, and the
	// full ordered part list). If no object exists at the key yet, a new object is
	// created. If WriteOffset is set in opts and does not match the current object
	// size, ErrInvalidWriteOffset is returned.
	AppendObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, object *Object, opts *AppendObjectOptions) error
	DeleteObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, opts *DeleteObjectOptions) (*DeleteObjectResult, error)
}

type MultipartStore interface {
	CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, contentType *string, checksumType *string) (*InitiateMultipartUploadResult, error)
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
	ObjectStore
	MultipartStore
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

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.CreateBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	bucket, err := metadataStore.HeadBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if !bucketName.Equals(bucket.Name) {
		return errors.New("invalid bucketName")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	buckets, err := metadataStore.ListBuckets(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(buckets) != 1 {
		return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
	}

	if !bucketName.Equals(buckets[0].Name) {
		return errors.New("invalid bucketName")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.PutObject(ctx, tx, bucketName, &Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         "",
		Size:         0,
		Parts:        []Part{},
	}, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	object, err := metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(object.Parts) != 0 {
		return errors.New("invalid part length")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	listBucketResult, err := metadataStore.ListObjects(ctx, tx, bucketName, ListObjectsOptions{
		MaxKeys:       1000,
		SkipPartFetch: true,
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(listBucketResult.Objects) != 1 {
		return errors.New("invalid objects length")
	}

	if key != listBucketResult.Objects[0].Key {
		return errors.New("invalid object key")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = metadataStore.DeleteObject(ctx, tx, bucketName, key, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	initiateMultipartUploadResult, err := metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, nil, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	partId, err := partstore.NewRandomPartId()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Part{
		Id:   *partId,
		Size: 0,
		ETag: "",
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, nil, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = metadataStore.DeleteObject(ctx, tx, bucketName, key, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	initiateMultipartUploadResult, err = metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, nil, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	partId, err = partstore.NewRandomPartId()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Part{
		Id:   *partId,
		Size: 0,
		ETag: "",
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	_, err = metadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.DeleteBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	buckets, err = metadataStore.ListBuckets(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(buckets) != 0 {
		return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
	}

	return nil
}
