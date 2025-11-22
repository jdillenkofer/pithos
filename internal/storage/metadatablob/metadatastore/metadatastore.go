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
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type Bucket struct {
	Name         BucketName
	CreationDate time.Time
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
	Blobs             []Blob
}

type ListBucketResult struct {
	Objects        []Object
	CommonPrefixes []string
	IsTruncated    bool
}

type Blob struct {
	Id                blobstore.BlobId
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
	DeletedBlobs      []Blob
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
	DeletedBlobs []Blob
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

type Part struct {
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
	Parts                []*Part
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

type ListObjectsOptions struct {
	Prefix     string
	Delimiter  string
	StartAfter string
	MaxKeys    int32
}

type ListMultipartUploadsOptions struct {
	Prefix         string
	Delimiter      string
	KeyMarker      string
	UploadIdMarker string
	MaxUploads     int32
}

type ListPartsOptions struct {
	PartNumberMarker string
	MaxParts         int32
}

type MaintenanceStore interface {
	GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error)
}

type BucketStore interface {
	CreateBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
	DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) error
	ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error)
	HeadBucket(ctx context.Context, tx *sql.Tx, bucketName BucketName) (*Bucket, error)
}

type ObjectStore interface {
	ListObjects(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListObjectsOptions) (*ListBucketResult, error)
	HeadObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) (*Object, error)
	PutObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, object *Object) error
	DeleteObject(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey) error
}

type MultipartStore interface {
	CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, contentType *string, checksumType *string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, partNumber int32, blob Blob) error
	CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, checksumInput *ChecksumInput) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId) (*AbortMultipartResult, error)
	ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName BucketName, opts ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, tx *sql.Tx, bucketName BucketName, key ObjectKey, uploadId UploadId, opts ListPartsOptions) (*ListPartsResult, error)
}

type MetadataStore interface {
	lifecycle.Manager
	MaintenanceStore
	BucketStore
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
		Blobs:        []Blob{},
	})
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

	if len(object.Blobs) != 0 {
		return errors.New("invalid blob length")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	listBucketResult, err := metadataStore.ListObjects(ctx, tx, bucketName, ListObjectsOptions{
		MaxKeys: 1000,
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
	err = metadataStore.DeleteObject(ctx, tx, bucketName, key)
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
	blobId, err := blobstore.NewRandomBlobId()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   *blobId,
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
	_, err = metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, nil)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.DeleteObject(ctx, tx, bucketName, key)
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
	blobId, err = blobstore.NewRandomBlobId()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   *blobId,
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
