package bucket

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type Repository interface {
	FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]Entity, error)
	FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*Entity, error)
	SaveBucket(ctx context.Context, tx *sql.Tx, bucket *Entity) error
	ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error)
	DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) error
}

type Entity struct {
	Id        *ulid.ULID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}
