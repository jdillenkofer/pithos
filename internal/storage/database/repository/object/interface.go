package object

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	SaveObject(ctx context.Context, tx *sql.Tx, object *Entity) error
	ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bool, error)
	FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, startAfter string) ([]Entity, error)
	FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, uploadIdMarker string) ([]Entity, error)
	FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey, uploadId string) (*Entity, error)
	FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, key storage.ObjectKey) (*Entity, error)
	CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, startAfter string) (*int, error)
	CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, prefix string, keyMarker string, uploadIdMarker string) (*int, error)
	DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type Entity struct {
	Id                *ulid.ULID
	BucketName        storage.BucketName
	Key               storage.ObjectKey
	ContentType       *string
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumType      *string
	Size              int64
	UploadStatus      string
	UploadId          *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

const (
	UploadStatusPending   = "PENDING"
	UploadStatusCompleted = "COMPLETED"
)
