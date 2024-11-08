package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository/bucket"
	"github.com/oklog/ulid/v2"
)

type sqliteBucketRepository struct {
	db                             *sql.DB
	findAllBucketsPreparedStmt     *sql.Stmt
	findBucketByNamePreparedStmt   *sql.Stmt
	insertBucketPreparedStmt       *sql.Stmt
	updateBucketByIdPreparedStmt   *sql.Stmt
	existsBucketByNamePreparedStmt *sql.Stmt
	deleteBucketByNamePreparedStmt *sql.Stmt
}

const (
	findAllBucketsStmt     = "SELECT id, name, created_at, updated_at FROM buckets"
	findBucketByNameStmt   = "SELECT id, name, created_at, updated_at FROM buckets WHERE name = ?"
	insertBucketStmt       = "INSERT INTO buckets (id, name, created_at, updated_at) VALUES(?, ?, ?, ?)"
	updateBucketByIdStmt   = "UPDATE buckets SET name = ?, updated_at = ? WHERE id = ?"
	existsBucketByNameStmt = "SELECT id FROM buckets WHERE name = ?"
	deleteBucketByNameStmt = "DELETE FROM buckets WHERE name = ?"
)

func New(db *sql.DB) (bucket.BucketRepository, error) {
	findAllBucketsPreparedStmt, err := db.Prepare(findAllBucketsStmt)
	if err != nil {
		return nil, err
	}
	findBucketByNamePreparedStmt, err := db.Prepare(findBucketByNameStmt)
	if err != nil {
		return nil, err
	}
	insertBucketPreparedStmt, err := db.Prepare(insertBucketStmt)
	if err != nil {
		return nil, err
	}
	updateBucketByIdPreparedStmt, err := db.Prepare(updateBucketByIdStmt)
	if err != nil {
		return nil, err
	}
	existsBucketByNamePreparedStmt, err := db.Prepare(existsBucketByNameStmt)
	if err != nil {
		return nil, err
	}
	deleteBucketByNamePreparedStmt, err := db.Prepare(deleteBucketByNameStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteBucketRepository{
		db:                             db,
		findAllBucketsPreparedStmt:     findAllBucketsPreparedStmt,
		findBucketByNamePreparedStmt:   findBucketByNamePreparedStmt,
		insertBucketPreparedStmt:       insertBucketPreparedStmt,
		updateBucketByIdPreparedStmt:   updateBucketByIdPreparedStmt,
		existsBucketByNamePreparedStmt: existsBucketByNamePreparedStmt,
		deleteBucketByNamePreparedStmt: deleteBucketByNamePreparedStmt,
	}, nil
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*bucket.BucketEntity, error) {
	var id string
	var name string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := bucket.BucketEntity{
		Id:        &ulidId,
		Name:      name,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return &bucketEntity, nil
}

func (br *sqliteBucketRepository) FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]bucket.BucketEntity, error) {
	bucketRows, err := tx.StmtContext(ctx, br.findAllBucketsPreparedStmt).QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	buckets := []bucket.BucketEntity{}
	for bucketRows.Next() {
		bucketEntity, err := convertRowToBucketEntity(bucketRows)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, *bucketEntity)
	}
	return buckets, nil
}

func (br *sqliteBucketRepository) FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*bucket.BucketEntity, error) {
	bucketRows, err := tx.StmtContext(ctx, br.findBucketByNamePreparedStmt).QueryContext(ctx, bucketName)
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

func (br *sqliteBucketRepository) SaveBucket(ctx context.Context, tx *sql.Tx, bucket *bucket.BucketEntity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.StmtContext(ctx, br.insertBucketPreparedStmt).ExecContext(ctx, bucket.Id.String(), bucket.Name, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, br.updateBucketByIdPreparedStmt).ExecContext(ctx, bucket.Name, bucket.UpdatedAt, bucket.Id.String())
	return err
}

func (br *sqliteBucketRepository) ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) (*bool, error) {
	bucketRows, err := tx.StmtContext(ctx, br.existsBucketByNamePreparedStmt).QueryContext(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	var exists = bucketRows.Next()
	return &exists, nil
}

func (br *sqliteBucketRepository) DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName string) error {
	_, err := tx.StmtContext(ctx, br.deleteBucketByNamePreparedStmt).ExecContext(ctx, bucketName)
	return err
}
