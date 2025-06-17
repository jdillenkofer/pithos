package metadatastore

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
)

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type Object struct {
	Key          string
	ContentType  string // only set in HeadObject and PutObject
	LastModified time.Time
	ETag         string
	Size         int64
	Blobs        []Blob
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
	UploadId string
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
}

type AbortMultipartResult struct {
	DeletedBlobs []Blob
}

type Upload struct {
	Key       string
	UploadId  string
	Initiated time.Time
}

type ListMultipartUploadsResult struct {
	Bucket             string
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
	ETag         string
	LastModified time.Time
	PartNumber   int32
	Size         int64
}

type ListPartsResult struct {
	Bucket               string
	Key                  string
	UploadId             string
	PartNumberMarker     string
	NextPartNumberMarker *string
	MaxParts             int32
	IsTruncated          bool
	Parts                []*Part
}

var ErrNoSuchBucket error = errors.New("NoSuchBucket")
var ErrBucketAlreadyExists error = errors.New("BucketAlreadyExists")
var ErrBucketNotEmpty error = errors.New("BucketNotEmpty")
var ErrNoSuchKey error = errors.New("NoSuchKey")
var ErrUploadWithInvalidSequenceNumber error = errors.New("UploadWithInvalidSequenceNumber")
var ErrNotImplemented error = errors.New("not implemented")

type MetadataStore interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	GetInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error)
	CreateBucket(ctx context.Context, tx *sql.Tx, bucketName string) error
	DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName string) error
	ListBuckets(ctx context.Context, tx *sql.Tx) ([]Bucket, error)
	HeadBucket(ctx context.Context, tx *sql.Tx, bucketName string) (*Bucket, error)
	ListObjects(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, delimiter string, startAfter string, maxKeys int32) (*ListBucketResult, error)
	HeadObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*Object, error)
	PutObject(ctx context.Context, tx *sql.Tx, bucketName string, object *Object) error
	DeleteObject(ctx context.Context, tx *sql.Tx, bucketName string, key string) error
	CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, contentType string) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumber int32, blob Blob) error
	CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*AbortMultipartResult, error)
	ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucket string, prefix string, delimiter string, keyMarker string, uploadIdMarker string, maxUploads int32) (*ListMultipartUploadsResult, error)
	ListParts(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string, partNumberMarker string, maxParts int32) (*ListPartsResult, error)
}

func Tester(metadataStore MetadataStore, db database.Database) error {
	ctx := context.Background()
	err := metadataStore.Start(ctx)
	if err != nil {
		return err
	}
	defer metadataStore.Stop(ctx)

	bucketName := "bucket"
	key := "test"

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

	if bucketName != bucket.Name {
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

	if bucketName != buckets[0].Name {
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
	listBucketResult, err := metadataStore.ListObjects(ctx, tx, bucketName, "", "", "", 1000)
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
	initiateMultipartUploadResult, err := metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, "")
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   blobstore.BlobId(ulid.Make()),
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
	_, err = metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId)
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
	initiateMultipartUploadResult, err = metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key, "")
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   blobstore.BlobId(ulid.Make()),
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
