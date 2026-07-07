package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findAllBucketsStmt     = "SELECT id, name, versioning_status, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, created_at, updated_at FROM buckets"
	findBucketByNameStmt   = "SELECT id, name, versioning_status, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, created_at, updated_at FROM buckets WHERE name = $1"
	insertBucketStmt       = "INSERT INTO buckets (id, name, versioning_status, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
	updateBucketByIdStmt   = "UPDATE buckets SET name = $1, versioning_status = $2, website_index_document_suffix = $3, website_error_document_key = $4, website_redirect_all_host_name = $5, website_redirect_all_protocol = $6, website_routing_rules_json = $7, cors_configuration_json = $8, lifecycle_configuration_json = $9, updated_at = $10 WHERE id = $11"
	existsBucketByNameStmt = "SELECT id FROM buckets WHERE name = $1"
	deleteBucketByNameStmt = "DELETE FROM buckets WHERE name = $1"
)

func NewRepository() (bucket.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*bucket.Entity, error) {
	var id string
	var name string
	var versioningStatus *string
	var websiteIndexDocumentSuffix *string
	var websiteErrorDocumentKey *string
	var websiteRedirectAllHostName *string
	var websiteRedirectAllProtocol *string
	var websiteRoutingRulesJSON *string
	var corsConfigurationJSON *string
	var lifecycleConfigurationJSON *string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &versioningStatus, &websiteIndexDocumentSuffix, &websiteErrorDocumentKey, &websiteRedirectAllHostName, &websiteRedirectAllProtocol, &websiteRoutingRulesJSON, &corsConfigurationJSON, &lifecycleConfigurationJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := bucket.Entity{
		Id:                         &ulidId,
		Name:                       storage.MustNewBucketName(name),
		VersioningStatus:           versioningStatus,
		WebsiteIndexDocumentSuffix: websiteIndexDocumentSuffix,
		WebsiteErrorDocumentKey:    websiteErrorDocumentKey,
		WebsiteRedirectAllHostName: websiteRedirectAllHostName,
		WebsiteRedirectAllProtocol: websiteRedirectAllProtocol,
		WebsiteRoutingRulesJSON:    websiteRoutingRulesJSON,
		CORSConfigurationJSON:      corsConfigurationJSON,
		LifecycleConfigurationJSON: lifecycleConfigurationJSON,
		CreatedAt:                  createdAt,
		UpdatedAt:                  updatedAt,
	}
	return &bucketEntity, nil
}

func (br *pgxRepository) FindAllBuckets(ctx context.Context, tx *sql.Tx) ([]bucket.Entity, error) {
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

func (br *pgxRepository) FindBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bucket.Entity, error) {
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

func (br *pgxRepository) SaveBucket(ctx context.Context, tx *sql.Tx, bucket *bucket.Entity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now().UTC()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.ExecContext(ctx, insertBucketStmt, bucket.Id.String(), bucket.Name.String(), bucket.VersioningStatus, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBucketByIdStmt, bucket.Name.String(), bucket.VersioningStatus, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.UpdatedAt, bucket.Id.String())
	return err
}

func (br *pgxRepository) ExistsBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bool, error) {
	bucketRows, err := tx.QueryContext(ctx, existsBucketByNameStmt, bucketName.String())
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	var exists = bucketRows.Next()
	return &exists, nil
}

func (br *pgxRepository) DeleteBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) error {
	_, err := tx.ExecContext(ctx, deleteBucketByNameStmt, bucketName.String())
	return err
}
