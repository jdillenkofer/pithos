package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BucketRepository struct {
	db *sql.DB
}

func NewBucketRepository(db *sql.DB) BucketRepository {
	return BucketRepository{
		db: db,
	}
}

type BucketEntity struct {
	Id        *ulid.ULID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*BucketEntity, error) {
	var id string
	var name string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := BucketEntity{
		Id:        &ulidId,
		Name:      name,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return &bucketEntity, nil
}

func (br *BucketRepository) FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]BucketEntity, error) {
	bucketRows, err := tx.QueryContext(ctx, "SELECT id, name, created_at, updated_at FROM buckets")
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	buckets := []BucketEntity{}
	for bucketRows.Next() {
		bucketEntity, err := convertRowToBucketEntity(bucketRows)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, *bucketEntity)
	}
	return buckets, nil
}

func (br *BucketRepository) FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*BucketEntity, error) {
	bucketRows, err := tx.QueryContext(ctx, "SELECT id, name, created_at, updated_at FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	if !bucketRows.Next() {
		return nil, nil
	}
	bucketEntity, err := convertRowToBucketEntity(bucketRows)
	if err != nil {
		return nil, err
	}
	return bucketEntity, nil
}

func (br *BucketRepository) SaveBucket(ctx context.Context, tx *sql.Tx, bucket *BucketEntity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.ExecContext(ctx, "INSERT INTO buckets (id, name, created_at, updated_at) VALUES(?, ?, ?, ?)", bucket.Id.String(), bucket.Name, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, "UPDATE buckets SET name = ?, updated_at = ? WHERE id = ?", bucket.Name, bucket.UpdatedAt, bucket.Id.String())
	return err
}

func (br *BucketRepository) ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error) {
	bucketRows, err := tx.QueryContext(ctx, "SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	var exists = bucketRows.Next()
	return &exists, nil
}

func (br *BucketRepository) DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM buckets WHERE name = ?", bucketName)
	return err
}
