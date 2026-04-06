package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/oklog/ulid/v2"
)

type sqliteRepository struct {
}

const (
	findAllBucketsStmt     = "SELECT id, name, website_index_document_suffix, website_error_document_key, cors_configuration_json, created_at, updated_at FROM buckets"
	findBucketByNameStmt   = "SELECT id, name, website_index_document_suffix, website_error_document_key, cors_configuration_json, created_at, updated_at FROM buckets WHERE name = $1"
	insertBucketStmt       = "INSERT INTO buckets (id, name, website_index_document_suffix, website_error_document_key, cors_configuration_json, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7)"
	updateBucketByIdStmt   = "UPDATE buckets SET name = $1, website_index_document_suffix = $2, website_error_document_key = $3, cors_configuration_json = $4, updated_at = $5 WHERE id = $6"
	existsBucketByNameStmt = "SELECT id FROM buckets WHERE name = $1"
	deleteBucketByNameStmt = "DELETE FROM buckets WHERE name = $1"
)

func NewRepository() (bucket.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*bucket.Entity, error) {
	var id string
	var name string
	var websiteIndexDocumentSuffix *string
	var websiteErrorDocumentKey *string
	var corsConfigurationJSON *string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &websiteIndexDocumentSuffix, &websiteErrorDocumentKey, &corsConfigurationJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := bucket.Entity{
		Id:                         &ulidId,
		Name:                       storage.MustNewBucketName(name),
		WebsiteIndexDocumentSuffix: websiteIndexDocumentSuffix,
		WebsiteErrorDocumentKey:    websiteErrorDocumentKey,
		CORSConfigurationJSON:      corsConfigurationJSON,
		CreatedAt:                  createdAt,
		UpdatedAt:                  updatedAt,
	}
	return &bucketEntity, nil
}

func (br *sqliteRepository) FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]bucket.Entity, error) {
	bucketRows, err := tx.QueryContext(ctx, findAllBucketsStmt)
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	buckets := []bucket.Entity{}
	for bucketRows.Next() {
		bucketEntity, err := convertRowToBucketEntity(bucketRows)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, *bucketEntity)
	}
	return buckets, nil
}

func (br *sqliteRepository) FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bucket.Entity, error) {
	bucketRows, err := tx.QueryContext(ctx, findBucketByNameStmt, bucketName.String())
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

func (br *sqliteRepository) SaveBucket(ctx context.Context, tx *sql.Tx, bucket *bucket.Entity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now().UTC()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.ExecContext(ctx, insertBucketStmt, bucket.Id.String(), bucket.Name.String(), bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.CORSConfigurationJSON, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBucketByIdStmt, bucket.Name.String(), bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.CORSConfigurationJSON, bucket.UpdatedAt, bucket.Id.String())
	return err
}

func (br *sqliteRepository) ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bool, error) {
	bucketRows, err := tx.QueryContext(ctx, existsBucketByNameStmt, bucketName.String())
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	var exists = bucketRows.Next()
	return &exists, nil
}

func (br *sqliteRepository) DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) error {
	_, err := tx.ExecContext(ctx, deleteBucketByNameStmt, bucketName.String())
	return err
}
