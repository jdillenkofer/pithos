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
	findAllBucketsStmt     = "SELECT id, name, owner_account_id, versioning_status, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at FROM buckets"
	findBucketByNameStmt   = "SELECT id, name, owner_account_id, versioning_status, object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at FROM buckets WHERE name = $1"
	insertBucketStmt       = "INSERT INTO buckets (id, name, owner_account_id, versioning_status, object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"
	updateBucketByIdStmt   = "UPDATE buckets SET name = $1, owner_account_id = $2, versioning_status = $3, object_lock_enabled = $4, default_retention_mode = $5, default_retention_days = $6, default_retention_years = $7, website_index_document_suffix = $8, website_error_document_key = $9, website_redirect_all_host_name = $10, website_redirect_all_protocol = $11, website_routing_rules_json = $12, cors_configuration_json = $13, lifecycle_configuration_json = $14, notification_configuration_json = $15, updated_at = $16 WHERE id = $17"
	existsBucketByNameStmt = "SELECT id FROM buckets WHERE name = $1"
	deleteBucketByNameStmt = "DELETE FROM buckets WHERE name = $1"
)

func NewRepository() (bucket.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToBucketEntity(bucketRows *sql.Rows) (*bucket.Entity, error) {
	var id string
	var name string
	var ownerAccountID string
	var versioningStatus *string
	var websiteIndexDocumentSuffix *string
	var websiteErrorDocumentKey *string
	var websiteRedirectAllHostName *string
	var websiteRedirectAllProtocol *string
	var websiteRoutingRulesJSON *string
	var corsConfigurationJSON *string
	var lifecycleConfigurationJSON *string
	var notificationConfigurationJSON *string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &ownerAccountID, &versioningStatus, &websiteIndexDocumentSuffix, &websiteErrorDocumentKey, &websiteRedirectAllHostName, &websiteRedirectAllProtocol, &websiteRoutingRulesJSON, &corsConfigurationJSON, &lifecycleConfigurationJSON, &notificationConfigurationJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := bucket.Entity{
		Id:                            &ulidId,
		Name:                          storage.MustNewBucketName(name),
		OwnerAccountID:                ownerAccountID,
		VersioningStatus:              versioningStatus,
		WebsiteIndexDocumentSuffix:    websiteIndexDocumentSuffix,
		WebsiteErrorDocumentKey:       websiteErrorDocumentKey,
		WebsiteRedirectAllHostName:    websiteRedirectAllHostName,
		WebsiteRedirectAllProtocol:    websiteRedirectAllProtocol,
		WebsiteRoutingRulesJSON:       websiteRoutingRulesJSON,
		CORSConfigurationJSON:         corsConfigurationJSON,
		LifecycleConfigurationJSON:    lifecycleConfigurationJSON,
		NotificationConfigurationJSON: notificationConfigurationJSON,
		CreatedAt:                     createdAt,
		UpdatedAt:                     updatedAt,
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
	var id, name string
	bucketEntity := &bucket.Entity{}
	err = bucketRows.Scan(&id, &name, &bucketEntity.OwnerAccountID, &bucketEntity.VersioningStatus, &bucketEntity.ObjectLockEnabled, &bucketEntity.DefaultRetentionMode, &bucketEntity.DefaultRetentionDays, &bucketEntity.DefaultRetentionYears, &bucketEntity.WebsiteIndexDocumentSuffix, &bucketEntity.WebsiteErrorDocumentKey, &bucketEntity.WebsiteRedirectAllHostName, &bucketEntity.WebsiteRedirectAllProtocol, &bucketEntity.WebsiteRoutingRulesJSON, &bucketEntity.CORSConfigurationJSON, &bucketEntity.LifecycleConfigurationJSON, &bucketEntity.NotificationConfigurationJSON, &bucketEntity.CreatedAt, &bucketEntity.UpdatedAt)
	if err != nil {
		return nil, err
	}
	parsed := ulid.MustParse(id)
	bucketEntity.Id, bucketEntity.Name = &parsed, storage.MustNewBucketName(name)
	return bucketEntity, nil
}

func (br *sqliteRepository) FindBucketByNameForShare(ctx context.Context, tx *sql.Tx, name storage.BucketName) (*bucket.Entity, error) {
	return br.FindBucketByName(ctx, tx, name)
}
func (br *sqliteRepository) FindBucketByNameForUpdate(ctx context.Context, tx *sql.Tx, name storage.BucketName) (*bucket.Entity, error) {
	return br.FindBucketByName(ctx, tx, name)
}

func (br *sqliteRepository) SaveBucket(ctx context.Context, tx *sql.Tx, bucket *bucket.Entity) error {
	if bucket.Id == nil {
		id := ulid.Make()
		bucket.Id = &id
		bucket.CreatedAt = time.Now().UTC()
		bucket.UpdatedAt = bucket.CreatedAt
		_, err := tx.ExecContext(ctx, insertBucketStmt, bucket.Id.String(), bucket.Name.String(), bucket.OwnerAccountID, bucket.VersioningStatus, bucket.ObjectLockEnabled, bucket.DefaultRetentionMode, bucket.DefaultRetentionDays, bucket.DefaultRetentionYears, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.NotificationConfigurationJSON, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBucketByIdStmt, bucket.Name.String(), bucket.OwnerAccountID, bucket.VersioningStatus, bucket.ObjectLockEnabled, bucket.DefaultRetentionMode, bucket.DefaultRetentionDays, bucket.DefaultRetentionYears, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.NotificationConfigurationJSON, bucket.UpdatedAt, bucket.Id.String())
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
