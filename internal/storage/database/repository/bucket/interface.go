package bucket

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*Entity, error)
	SaveBucket(ctx context.Context, tx *sql.Tx, bucket *Entity) error
	ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bool, error)
	DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) error
}

type Entity struct {
	Id        *ulid.ULID
	Name      storage.BucketName
	CreatedAt time.Time
	UpdatedAt time.Time
}
