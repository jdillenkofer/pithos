package buckettag

import (
	"context"
	"database/sql"
)

type Entity struct {
	BucketName string
	Key        string
	Value      string
}

type Repository interface {
	FindTagsByBucketNameOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string) ([]Entity, error)
	SaveTag(ctx context.Context, tx *sql.Tx, tag Entity) error
	DeleteTagsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) error
}
