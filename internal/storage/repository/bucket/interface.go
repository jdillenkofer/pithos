package bucket

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BucketRepository interface {
	FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]BucketEntity, error)
	FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*BucketEntity, error)
	SaveBucket(ctx context.Context, tx *sql.Tx, bucket *BucketEntity) error
	ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error)
	DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) error
}

type BucketEntity struct {
	Id        *ulid.ULID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}
