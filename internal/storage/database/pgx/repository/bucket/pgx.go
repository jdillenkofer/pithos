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
	findAllBucketsStmt     = "SELECT id, name, versioning_status, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at FROM buckets"
	findBucketByNameStmt   = "SELECT id, name, versioning_status, object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at FROM buckets WHERE name = $1"
	insertBucketStmt       = "INSERT INTO buckets (id, name, versioning_status, object_lock_enabled, default_retention_mode, default_retention_days, default_retention_years, website_index_document_suffix, website_error_document_key, website_redirect_all_host_name, website_redirect_all_protocol, website_routing_rules_json, cors_configuration_json, lifecycle_configuration_json, notification_configuration_json, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
	updateBucketByIdStmt   = "UPDATE buckets SET name = $1, versioning_status = $2, object_lock_enabled = $3, default_retention_mode = $4, default_retention_days = $5, default_retention_years = $6, website_index_document_suffix = $7, website_error_document_key = $8, website_redirect_all_host_name = $9, website_redirect_all_protocol = $10, website_routing_rules_json = $11, cors_configuration_json = $12, lifecycle_configuration_json = $13, notification_configuration_json = $14, updated_at = $15 WHERE id = $16"
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
	var notificationConfigurationJSON *string
	var createdAt time.Time
	var updatedAt time.Time
	err := bucketRows.Scan(&id, &name, &versioningStatus, &websiteIndexDocumentSuffix, &websiteErrorDocumentKey, &websiteRedirectAllHostName, &websiteRedirectAllProtocol, &websiteRoutingRulesJSON, &corsConfigurationJSON, &lifecycleConfigurationJSON, &notificationConfigurationJSON, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	bucketEntity := bucket.Entity{
		Id:                            &ulidId,
		Name:                          storage.MustNewBucketName(name),
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

func convertStateRowToBucketEntity(rows *sql.Rows) (*bucket.Entity, error) {
	var id, name string
	var e bucket.Entity
	err := rows.Scan(&id, &name, &e.VersioningStatus, &e.ObjectLockEnabled, &e.DefaultRetentionMode, &e.DefaultRetentionDays, &e.DefaultRetentionYears, &e.WebsiteIndexDocumentSuffix, &e.WebsiteErrorDocumentKey, &e.WebsiteRedirectAllHostName, &e.WebsiteRedirectAllProtocol, &e.WebsiteRoutingRulesJSON, &e.CORSConfigurationJSON, &e.LifecycleConfigurationJSON, &e.NotificationConfigurationJSON, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	parsed := ulid.MustParse(id)
	e.Id, e.Name = &parsed, storage.MustNewBucketName(name)
	return &e, nil
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
	return br.findBucketByName(ctx, tx, bucketName, "")
}
func (br *pgxRepository) FindBucketByNameForShare(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bucket.Entity, error) {
	return br.findBucketByName(ctx, tx, bucketName, " FOR SHARE")
}
func (br *pgxRepository) FindBucketByNameForUpdate(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName) (*bucket.Entity, error) {
	return br.findBucketByName(ctx, tx, bucketName, " FOR UPDATE")
}
func (br *pgxRepository) findBucketByName(ctx context.Context, tx *sql.Tx, bucketName storage.BucketName, suffix string) (*bucket.Entity, error) {
	bucketRows, err := tx.QueryContext(ctx, findBucketByNameStmt+suffix, bucketName.String())
	if err != nil {
		return nil, err
	}
	defer bucketRows.Close()
	if !bucketRows.Next() {
		return nil, nil
	}
	bucketEntity, err := convertStateRowToBucketEntity(bucketRows)
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
		_, err := tx.ExecContext(ctx, insertBucketStmt, bucket.Id.String(), bucket.Name.String(), bucket.VersioningStatus, bucket.ObjectLockEnabled, bucket.DefaultRetentionMode, bucket.DefaultRetentionDays, bucket.DefaultRetentionYears, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.NotificationConfigurationJSON, bucket.CreatedAt, bucket.UpdatedAt)
		return err
	}
	bucket.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBucketByIdStmt, bucket.Name.String(), bucket.VersioningStatus, bucket.ObjectLockEnabled, bucket.DefaultRetentionMode, bucket.DefaultRetentionDays, bucket.DefaultRetentionYears, bucket.WebsiteIndexDocumentSuffix, bucket.WebsiteErrorDocumentKey, bucket.WebsiteRedirectAllHostName, bucket.WebsiteRedirectAllProtocol, bucket.WebsiteRoutingRulesJSON, bucket.CORSConfigurationJSON, bucket.LifecycleConfigurationJSON, bucket.NotificationConfigurationJSON, bucket.UpdatedAt, bucket.Id.String())
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
