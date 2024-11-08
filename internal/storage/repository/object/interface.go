package object

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type ObjectRepository interface {
	SaveObject(ctx context.Context, tx *sql.Tx, object *ObjectEntity) error
	ContainsBucketObjectsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error)
	FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) ([]ObjectEntity, error)
	FindObjectByBucketNameAndKeyAndUploadId(ctx context.Context, tx *sql.Tx, bucketName string, key string, uploadId string) (*ObjectEntity, error)
	FindObjectByBucketNameAndKey(ctx context.Context, tx *sql.Tx, bucketName string, key string) (*ObjectEntity, error)
	CountObjectsByBucketNameAndPrefixAndStartAfter(ctx context.Context, tx *sql.Tx, bucketName string, prefix string, startAfter string) (*int, error)
	DeleteObjectById(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error
}

type ObjectEntity struct {
	Id           *ulid.ULID
	BucketName   string
	Key          string
	ETag         string
	Size         int64
	UploadStatus string
	UploadId     string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

const (
	UploadStatusPending   = "PENDING"
	UploadStatusCompleted = "COMPLETED"
)
