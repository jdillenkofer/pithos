package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/tag"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/usermetadata"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// isUniqueConstraintViolation reports whether err is a unique-constraint
// violation from either the PostgreSQL (pgconn.PgError / SQLSTATE 23505) or
// SQLite (sqlite3.ErrConstraintUnique) driver.
func isUniqueConstraintViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgerrcode.UniqueViolation
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique
	}
	return false
}

type sqlMetadataStore struct {
	*lifecycle.ValidatedLifecycle
	bucketRepository       bucket.Repository
	objectRepository       object.Repository
	partRepository         part.Repository
	tagRepository          tag.Repository
	userMetadataRepository usermetadata.Repository
	tracer                 trace.Tracer
}

// Compile-time check to ensure sqlMetadataStore implements metadatastore.MetadataStore
var _ metadatastore.MetadataStore = (*sqlMetadataStore)(nil)

func New(db database.Database, bucketRepository bucket.Repository, objectRepository object.Repository, partRepository part.Repository, tagRepository tag.Repository, userMetadataRepository usermetadata.Repository) (metadatastore.MetadataStore, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("SqlMetadataStore")
	if err != nil {
		return nil, err
	}
	return &sqlMetadataStore{
		ValidatedLifecycle:     lifecycle,
		bucketRepository:       bucketRepository,
		objectRepository:       objectRepository,
		partRepository:         partRepository,
		tagRepository:          tagRepository,
		userMetadataRepository: userMetadataRepository,
		tracer:                 otel.Tracer("internal/storage/metadatapart/metadatastore/sql"),
	}, nil
}

// loadObjectTags loads the tag set for the given object id as a map. An object
// with no tags yields an empty (non-nil) map.
func (sms *sqlMetadataStore) loadObjectTags(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) (map[string]string, error) {
	tagEntities, err := sms.tagRepository.FindTagsByObjectIdOrderByKeyAsc(ctx, tx, objectId)
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	for _, tagEntity := range tagEntities {
		tags[tagEntity.Key] = tagEntity.Value
	}
	return tags, nil
}

// replaceObjectTags removes any existing tags for the object id and inserts the
// supplied tag set.
func (sms *sqlMetadataStore) replaceObjectTags(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, tags map[string]string) error {
	err := sms.tagRepository.DeleteTagsByObjectId(ctx, tx, objectId)
	if err != nil {
		return err
	}
	for key, value := range tags {
		tagEntity := tag.Entity{
			ObjectId: objectId,
			Key:      key,
			Value:    value,
		}
		err = sms.tagRepository.SaveTag(ctx, tx, &tagEntity)
		if err != nil {
			return err
		}
	}
	return nil
}

// loadObjectUserMetadata loads the user-defined metadata for the given object
// id as a map. An object with no user metadata yields an empty (non-nil) map.
func (sms *sqlMetadataStore) loadObjectUserMetadata(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) (map[string]string, error) {
	userMetadataEntities, err := sms.userMetadataRepository.FindUserMetadataByObjectIdOrderByKeyAsc(ctx, tx, objectId)
	if err != nil {
		return nil, err
	}
	userMetadata := map[string]string{}
	for _, userMetadataEntity := range userMetadataEntities {
		userMetadata[userMetadataEntity.Key] = userMetadataEntity.Value
	}
	return userMetadata, nil
}

// replaceObjectUserMetadata removes any existing user-defined metadata for the
// object id and inserts the supplied entries.
func (sms *sqlMetadataStore) replaceObjectUserMetadata(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, userMetadata map[string]string) error {
	err := sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, objectId)
	if err != nil {
		return err
	}
	for key, value := range userMetadata {
		userMetadataEntity := usermetadata.Entity{
			ObjectId: objectId,
			Key:      key,
			Value:    value,
		}
		err = sms.userMetadataRepository.SaveUserMetadata(ctx, tx, &userMetadataEntity)
		if err != nil {
			return err
		}
	}
	return nil
}

// applySystemMetadataToEntity copies the user-modifiable system metadata
// headers onto the object entity's columns.
func applySystemMetadataToEntity(objectEntity *object.Entity, metadata metadatastore.ObjectMetadata) {
	objectEntity.CacheControl = metadata.CacheControl
	objectEntity.ContentDisposition = metadata.ContentDisposition
	objectEntity.ContentEncoding = metadata.ContentEncoding
	objectEntity.ContentLanguage = metadata.ContentLanguage
	objectEntity.Expires = metadata.Expires
	objectEntity.WebsiteRedirectLocation = metadata.WebsiteRedirectLocation
}

// systemMetadataFromEntity reads the user-modifiable system metadata headers
// from the object entity's columns.
func systemMetadataFromEntity(objectEntity *object.Entity) metadatastore.ObjectMetadata {
	return metadatastore.ObjectMetadata{
		CacheControl:            objectEntity.CacheControl,
		ContentDisposition:      objectEntity.ContentDisposition,
		ContentEncoding:         objectEntity.ContentEncoding,
		ContentLanguage:         objectEntity.ContentLanguage,
		Expires:                 objectEntity.Expires,
		WebsiteRedirectLocation: objectEntity.WebsiteRedirectLocation,
	}
}

func (sms *sqlMetadataStore) GetInUsePartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetInUsePartIds")
	defer span.End()

	return sms.partRepository.FindInUsePartIds(ctx, tx)
}

func (sms *sqlMetadataStore) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CreateBucket")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *exists {
		return metadatastore.ErrBucketAlreadyExists
	}

	err = sms.bucketRepository.SaveBucket(ctx, tx, &bucket.Entity{
		Name: bucketName,
	})
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucket")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	containsBucketObjects, err := sms.objectRepository.ContainsBucketObjectsByBucketName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *containsBucketObjects {
		return metadatastore.ErrBucketNotEmpty
	}

	err = sms.bucketRepository.DeleteBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) ListBuckets(ctx context.Context, tx *sql.Tx) ([]metadatastore.Bucket, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListBuckets")
	defer span.End()

	bucketEntities, err := sms.bucketRepository.FindAllBuckets(ctx, tx)
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucketEntity bucket.Entity) metadatastore.Bucket {
		return metadatastore.Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		}
	}, bucketEntities)

	return buckets, nil
}

func (sms *sqlMetadataStore) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.Bucket, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadBucket")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	bucket := metadatastore.Bucket{
		Name:         bucketEntity.Name,
		CreationDate: bucketEntity.CreatedAt,
	}

	return &bucket, nil
}

func (sms *sqlMetadataStore) GetBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.WebsiteConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if bucketEntity.WebsiteIndexDocumentSuffix == nil && bucketEntity.WebsiteRedirectAllHostName == nil && bucketEntity.WebsiteRoutingRulesJSON == nil {
		return nil, metadatastore.ErrNoSuchWebsiteConfiguration
	}

	config := &metadatastore.WebsiteConfiguration{
		ErrorDocumentKey: bucketEntity.WebsiteErrorDocumentKey,
	}
	if bucketEntity.WebsiteIndexDocumentSuffix != nil {
		config.IndexDocumentSuffix = *bucketEntity.WebsiteIndexDocumentSuffix
	}
	if bucketEntity.WebsiteRedirectAllHostName != nil {
		config.RedirectAllRequestsTo = &metadatastore.WebsiteRedirectAllRequestsTo{
			HostName: *bucketEntity.WebsiteRedirectAllHostName,
			Protocol: bucketEntity.WebsiteRedirectAllProtocol,
		}
	}
	if bucketEntity.WebsiteRoutingRulesJSON != nil {
		if err := json.Unmarshal([]byte(*bucketEntity.WebsiteRoutingRulesJSON), &config.RoutingRules); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func (sms *sqlMetadataStore) PutBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.WebsiteConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	if config.IndexDocumentSuffix == "" {
		bucketEntity.WebsiteIndexDocumentSuffix = nil
	} else {
		bucketEntity.WebsiteIndexDocumentSuffix = &config.IndexDocumentSuffix
	}
	bucketEntity.WebsiteErrorDocumentKey = config.ErrorDocumentKey
	bucketEntity.WebsiteRedirectAllHostName = nil
	bucketEntity.WebsiteRedirectAllProtocol = nil
	if config.RedirectAllRequestsTo != nil {
		bucketEntity.WebsiteRedirectAllHostName = &config.RedirectAllRequestsTo.HostName
		bucketEntity.WebsiteRedirectAllProtocol = config.RedirectAllRequestsTo.Protocol
	}
	bucketEntity.WebsiteRoutingRulesJSON = nil
	if len(config.RoutingRules) > 0 {
		routingRulesJSON, err := json.Marshal(config.RoutingRules)
		if err != nil {
			return err
		}
		routingRulesJSONStr := string(routingRulesJSON)
		bucketEntity.WebsiteRoutingRulesJSON = &routingRulesJSONStr
	}

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.WebsiteIndexDocumentSuffix = nil
	bucketEntity.WebsiteErrorDocumentKey = nil
	bucketEntity.WebsiteRedirectAllHostName = nil
	bucketEntity.WebsiteRedirectAllProtocol = nil
	bucketEntity.WebsiteRoutingRulesJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketVersioningConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketVersioningConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if bucketEntity.VersioningStatus == nil {
		return &metadatastore.BucketVersioningConfiguration{}, nil
	}

	status := metadatastore.BucketVersioningStatus(*bucketEntity.VersioningStatus)
	return &metadatastore.BucketVersioningConfiguration{Status: &status}, nil
}

func (sms *sqlMetadataStore) PutBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketVersioningConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketVersioningConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	if config == nil || config.Status == nil {
		bucketEntity.VersioningStatus = nil
	} else {
		status := string(*config.Status)
		bucketEntity.VersioningStatus = &status
	}

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketCORSConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if bucketEntity.CORSConfigurationJSON == nil {
		return nil, metadatastore.ErrNoSuchCORSConfiguration
	}

	var config metadatastore.BucketCORSConfiguration
	err = json.Unmarshal([]byte(*bucketEntity.CORSConfigurationJSON), &config)
	if err != nil {
		return nil, err
	}
	if config.Rules == nil {
		config.Rules = []metadatastore.CORSRule{}
	}
	return &config, nil
}

func (sms *sqlMetadataStore) PutBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketCORSConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}
	jsonString := string(jsonConfig)
	bucketEntity.CORSConfigurationJSON = &jsonString

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.CORSConfigurationJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketLifecycleConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if bucketEntity.LifecycleConfigurationJSON == nil {
		return nil, metadatastore.ErrNoSuchLifecycleConfiguration
	}

	var config metadatastore.BucketLifecycleConfiguration
	err = json.Unmarshal([]byte(*bucketEntity.LifecycleConfigurationJSON), &config)
	if err != nil {
		return nil, err
	}
	if config.Rules == nil {
		config.Rules = []metadatastore.LifecycleRule{}
	}
	return &config, nil
}

func (sms *sqlMetadataStore) PutBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketLifecycleConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}
	jsonString := string(jsonConfig)
	bucketEntity.LifecycleConfigurationJSON = &jsonString

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.LifecycleConfigurationJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func determineCommonPrefix(prefix, key, delimiter string) *string {
	prefixSegments := strings.Split(prefix, delimiter)
	keySegments := strings.Split(key, delimiter)
	if len(prefixSegments) >= len(keySegments) {
		return nil
	}
	commonPrefix := ""
	for idx := range prefixSegments {
		commonPrefix += keySegments[idx] + delimiter
	}
	return &commonPrefix
}

func (sms *sqlMetadataStore) listObjects(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectsOptions) (*metadatastore.ListBucketResult, error) {
	prefix := ""
	if opts.Prefix != nil {
		prefix = *opts.Prefix
	}
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}
	startAfter := ""
	if opts.StartAfter != nil {
		startAfter = *opts.StartAfter
	}
	maxKeys := opts.MaxKeys

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	objects := []metadatastore.Object{}
	listedObjectIds := []ulid.ULID{}
	var err error
	isTruncated := false
	var objectEntities []object.Entity
	if delimiter == "" {
		objectEntities, err = sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAscWithLimit(ctx, tx, bucketName, prefix, startAfter, maxKeys+1)
		if err != nil {
			return nil, err
		}
		if int32(len(objectEntities)) > maxKeys {
			isTruncated = true
			objectEntities = objectEntities[:maxKeys]
		}
	} else {
		keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(ctx, tx, bucketName, prefix, startAfter)
		if err != nil {
			return nil, err
		}
		isTruncated = int32(*keyCount) > maxKeys
		objectEntities, err = sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx, tx, bucketName, prefix, startAfter)
		if err != nil {
			return nil, err
		}
	}

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key.String(), delimiter)
			if commonPrefix != nil {
				if _, seen := commonPrefixSet[*commonPrefix]; !seen {
					commonPrefixSet[*commonPrefix] = struct{}{}
					commonPrefixes = append(commonPrefixes, *commonPrefix)
				}
			}
		}
		if int32(len(objects)) < maxKeys {
			var parts []metadatastore.Part = nil
			if !opts.SkipPartFetch {
				// @Perf: Consider optimizing part fetch to reduce database calls
				partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
				if err != nil {
					return nil, err
				}
				for _, partEntity := range partEntities {
					partStruc := metadatastore.Part{
						Id:                partEntity.PartId,
						ETag:              partEntity.ETag,
						ChecksumCRC32:     partEntity.ChecksumCRC32,
						ChecksumCRC32C:    partEntity.ChecksumCRC32C,
						ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
						ChecksumSHA1:      partEntity.ChecksumSHA1,
						ChecksumSHA256:    partEntity.ChecksumSHA256,
						Size:              partEntity.Size,
						StoreName:         partEntity.PartStoreName,
					}
					parts = append(parts, partStruc)
				}
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, metadatastore.Object{
					Key:               objectEntity.Key,
					LastModified:      objectEntity.UpdatedAt,
					VersionID:         objectEntity.VersionID,
					IsDeleteMarker:    objectEntity.IsDeleteMarker,
					ETag:              objectEntity.ETag,
					ChecksumCRC32:     objectEntity.ChecksumCRC32,
					ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
					ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
					ChecksumSHA1:      objectEntity.ChecksumSHA1,
					ChecksumSHA256:    objectEntity.ChecksumSHA256,
					ChecksumType:      objectEntity.ChecksumType,
					Size:              objectEntity.Size,
					StorageClass:      objectEntity.StorageClass,
					Parts:             parts,
				})
				listedObjectIds = append(listedObjectIds, *objectEntity.Id)
			}
		}
	}

	// Load the tag sets of the whole page in one query, so per-object consumers
	// (e.g. tag-based list filtering in the authorizer) don't need a storage
	// lookup per key. Every listed object gets a non-nil map.
	tagEntities, err := sms.tagRepository.FindTagsByObjectIdsOrderByObjectIdAndKeyAsc(ctx, tx, listedObjectIds)
	if err != nil {
		return nil, err
	}
	tagsByObjectId := map[ulid.ULID]map[string]string{}
	for _, tagEntity := range tagEntities {
		if tagsByObjectId[tagEntity.ObjectId] == nil {
			tagsByObjectId[tagEntity.ObjectId] = map[string]string{}
		}
		tagsByObjectId[tagEntity.ObjectId][tagEntity.Key] = tagEntity.Value
	}
	for i, objectId := range listedObjectIds {
		tags := tagsByObjectId[objectId]
		if tags == nil {
			tags = map[string]string{}
		}
		objects[i].Tags = tags
	}

	listBucketResult := metadatastore.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}
	return &listBucketResult, nil
}

func (sms *sqlMetadataStore) ListObjects(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectsOptions) (*metadatastore.ListBucketResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListObjects")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	return sms.listObjects(ctx, tx, bucketName, opts)
}

func (sms *sqlMetadataStore) ListObjectVersions(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListObjectVersionsOptions) (*metadatastore.ListObjectVersionsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListObjectVersions")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	prefix := ""
	if opts.Prefix != nil {
		prefix = *opts.Prefix
	}
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}
	keyMarker := ""
	if opts.KeyMarker != nil {
		keyMarker = *opts.KeyMarker
	}
	versionIDMarker := ""
	if opts.VersionIDMarker != nil {
		versionIDMarker = *opts.VersionIDMarker
	}

	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	var entities []object.Entity
	if delimiter == "" {
		// Without a delimiter every row is emitted, so one extra row is enough
		// to detect truncation and the fetch can be bounded in SQL.
		entities, err = sms.objectRepository.FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDescWithLimit(ctx, tx, bucketName, prefix, keyMarker, versionIDMarker, maxKeys+1)
	} else {
		entities, err = sms.objectRepository.FindObjectVersionsByBucketNameAndPrefixAndKeyMarkerAndVersionIDMarkerOrderByKeyAscAndVersionIDDesc(ctx, tx, bucketName, prefix, keyMarker, versionIDMarker)
	}
	if err != nil {
		return nil, err
	}

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	versions := []metadatastore.ObjectVersion{}
	isTruncated := false
	emittedCount := int32(0)
	var lastReturnedKey *string
	var lastReturnedVersionID *string
	var nextKeyMarker *string
	var nextVersionIDMarker *string

	for _, entity := range entities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, entity.Key.String(), delimiter)
			if commonPrefix != nil {
				if _, exists := commonPrefixSet[*commonPrefix]; exists {
					// The entity is represented by an already-emitted common
					// prefix; advance the continuation markers past it so the
					// next page does not re-emit the same prefix.
					k := entity.Key.String()
					lastReturnedKey = &k
					lastReturnedVersionID = entity.VersionID
					continue
				}
				if emittedCount >= maxKeys {
					isTruncated = true
					break
				}
				commonPrefixSet[*commonPrefix] = struct{}{}
				commonPrefixes = append(commonPrefixes, *commonPrefix)
				emittedCount++
				k := entity.Key.String()
				lastReturnedKey = &k
				lastReturnedVersionID = entity.VersionID
				continue
			}
		}

		if emittedCount >= maxKeys {
			isTruncated = true
			break
		}

		keyWithoutPrefix := strings.TrimPrefix(entity.Key.String(), prefix)
		if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
			versionID := ""
			if entity.VersionID != nil {
				versionID = *entity.VersionID
			}
			versions = append(versions, metadatastore.ObjectVersion{
				Key:            entity.Key,
				VersionID:      versionID,
				IsDeleteMarker: entity.IsDeleteMarker,
				IsLatest:       entity.IsLatest,
				LastModified:   entity.UpdatedAt,
				Size:           entity.Size,
				ETag:           &entity.ETag,
				StorageClass:   entity.StorageClass,
			})
			emittedCount++
			nextKey := entity.Key.String()
			lastReturnedKey = &nextKey
			lastReturnedVersionID = entity.VersionID
		}
	}

	if isTruncated {
		nextKeyMarker = lastReturnedKey
		nextVersionIDMarker = lastReturnedVersionID
	}

	return &metadatastore.ListObjectVersionsResult{
		Versions:            versions,
		CommonPrefixes:      commonPrefixes,
		IsTruncated:         isTruncated,
		NextKeyMarker:       nextKeyMarker,
		NextVersionIDMarker: nextVersionIDMarker,
	}, nil
}

func (sms *sqlMetadataStore) HeadObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) (*metadatastore.Object, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadObject")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}
	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}
	parts := sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
		return metadatastore.Part{
			Id:                partEntity.PartId,
			ETag:              partEntity.ETag,
			ChecksumCRC32:     partEntity.ChecksumCRC32,
			ChecksumCRC32C:    partEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      partEntity.ChecksumSHA1,
			ChecksumSHA256:    partEntity.ChecksumSHA256,
			Size:              partEntity.Size,
			StoreName:         partEntity.PartStoreName,
		}
	}, partEntities)

	tags, err := sms.loadObjectTags(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	metadata := systemMetadataFromEntity(objectEntity)
	metadata.UserMetadata, err = sms.loadObjectUserMetadata(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.Object{
		Key:               key,
		ContentType:       objectEntity.ContentType,
		LastModified:      objectEntity.UpdatedAt,
		VersionID:         objectEntity.VersionID,
		IsDeleteMarker:    objectEntity.IsDeleteMarker,
		ETag:              objectEntity.ETag,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
		Size:              objectEntity.Size,
		StorageClass:      objectEntity.StorageClass,
		Parts:             parts,
		Tags:              tags,
		Metadata:          metadata,
	}, nil
}

func (sms *sqlMetadataStore) HeadObjectVersion(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, versionID string) (*metadatastore.Object, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadObjectVersion")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, versionID)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	parts := []metadatastore.Part{}
	if !objectEntity.IsDeleteMarker {
		partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
		if err != nil {
			return nil, err
		}
		parts = sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
			return metadatastore.Part{Id: partEntity.PartId, ETag: partEntity.ETag, ChecksumCRC32: partEntity.ChecksumCRC32, ChecksumCRC32C: partEntity.ChecksumCRC32C, ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME, ChecksumSHA1: partEntity.ChecksumSHA1, ChecksumSHA256: partEntity.ChecksumSHA256, Size: partEntity.Size, StoreName: partEntity.PartStoreName}
		}, partEntities)
	}

	tags, err := sms.loadObjectTags(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	metadata := systemMetadataFromEntity(objectEntity)
	metadata.UserMetadata, err = sms.loadObjectUserMetadata(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.Object{Key: key, ContentType: objectEntity.ContentType, LastModified: objectEntity.UpdatedAt, VersionID: objectEntity.VersionID, IsDeleteMarker: objectEntity.IsDeleteMarker, ETag: objectEntity.ETag, ChecksumCRC32: objectEntity.ChecksumCRC32, ChecksumCRC32C: objectEntity.ChecksumCRC32C, ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME, ChecksumSHA1: objectEntity.ChecksumSHA1, ChecksumSHA256: objectEntity.ChecksumSHA256, ChecksumType: objectEntity.ChecksumType, Size: objectEntity.Size, StorageClass: objectEntity.StorageClass, Parts: parts, Tags: tags, Metadata: metadata}, nil
}

func (sms *sqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)

	latestObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	objectExists := latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker
	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			if !objectExists {
				return metadatastore.ErrPreconditionFailed
			}
		} else if !objectExists || latestObjectEntity.ETag != *opts.IfMatchETag {
			return metadatastore.ErrPreconditionFailed
		}
	}
	if opts != nil && opts.IfNoneMatchStar && objectExists {
		return metadatastore.ErrPreconditionFailed
	}
	if opts != nil && opts.IfNoneMatchStar {
		latestObjectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
		if err != nil {
			return err
		}
		if latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker {
			return metadatastore.ErrPreconditionFailed
		}
	}

	if opts != nil && (opts.IfMatchETag != nil || opts.IfNoneMatchStar) && latestObjectEntity != nil {
		lockedObjectEntity := *latestObjectEntity
		locked, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, latestObjectEntity.OptimisticLockVersion)
		if err != nil {
			return err
		}
		if !*locked {
			return metadatastore.ErrPreconditionFailed
		}
		latestObjectEntity = &lockedObjectEntity
	}

	objectEntity := object.Entity{
		BucketName:        bucketName,
		Key:               obj.Key,
		ContentType:       obj.ContentType,
		ETag:              obj.ETag,
		ChecksumCRC32:     obj.ChecksumCRC32,
		ChecksumCRC32C:    obj.ChecksumCRC32C,
		ChecksumCRC64NVME: obj.ChecksumCRC64NVME,
		ChecksumSHA1:      obj.ChecksumSHA1,
		ChecksumSHA256:    obj.ChecksumSHA256,
		ChecksumType:      obj.ChecksumType,
		Size:              obj.Size,
		StorageClass:      obj.StorageClass,
		IsDeleteMarker:    false,
		IsLatest:          true,
		UploadStatus:      object.UploadStatusCompleted,
	}
	applySystemMetadataToEntity(&objectEntity, obj.Metadata)

	if versioningEnabled {
		versionID := metadatastore.NewRandomUploadId().String()
		objectEntity.VersionID = &versionID
		if latestObjectEntity != nil {
			latestObjectEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity); err != nil {
				return err
			}
		}
		if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
			if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
				return metadatastore.ErrPreconditionFailed
			}
			return err
		}
	} else {
		nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
		if err != nil {
			return err
		}
		if opts != nil && opts.IfNoneMatchStar && nullVersionEntity != nil {
			return metadatastore.ErrPreconditionFailed
		}
		nullVersion := "null"
		objectEntity.VersionID = &nullVersion

		if latestObjectEntity != nil {
			latestObjectEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity); err != nil {
				return err
			}
		}

		if nullVersionEntity != nil {
			objectEntity.Id = nullVersionEntity.Id
			objectEntity.OptimisticLockVersion = nullVersionEntity.OptimisticLockVersion
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return metadatastore.ErrPreconditionFailed
				}
				return err
			}
			err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *objectEntity.Id)
			if err != nil {
				return err
			}
		} else {
			if err := sms.objectRepository.SaveObject(ctx, tx, &objectEntity); err != nil {
				if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
					return metadatastore.ErrPreconditionFailed
				}
				return err
			}
		}
	}

	objectId := objectEntity.Id
	sequenceNumber := 0
	for _, partStruc := range obj.Parts {
		partEntity := part.Entity{
			PartId:            partStruc.Id,
			ObjectId:          *objectId,
			ETag:              partStruc.ETag,
			ChecksumCRC32:     partStruc.ChecksumCRC32,
			ChecksumCRC32C:    partStruc.ChecksumCRC32C,
			ChecksumCRC64NVME: partStruc.ChecksumCRC64NVME,
			ChecksumSHA1:      partStruc.ChecksumSHA1,
			ChecksumSHA256:    partStruc.ChecksumSHA256,
			Size:              partStruc.Size,
			SequenceNumber:    sequenceNumber,
			PartStoreName:     partStruc.StoreName,
		}
		err = sms.partRepository.SavePart(ctx, tx, &partEntity)
		if err != nil {
			return err
		}
		sequenceNumber += 1
	}

	obj.VersionID = objectEntity.VersionID

	// PutObject replaces the object entirely, so its tag set and user metadata
	// are replaced with the values supplied on the new object (empty when none
	// were provided).
	if err := sms.replaceObjectTags(ctx, tx, *objectId, obj.Tags); err != nil {
		return err
	}
	if err := sms.replaceObjectUserMetadata(ctx, tx, *objectId, obj.Metadata.UserMetadata); err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) AppendObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.AppendObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AppendObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)
	if versioningEnabled {
		return sms.PutObject(ctx, tx, bucketName, obj, nil)
	}

	// Check whether an object already exists at this key.
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	if oldObjectEntity != nil {
		updatedEntity := object.Entity{
			Id:             oldObjectEntity.Id,
			BucketName:     bucketName,
			Key:            obj.Key,
			ContentType:    oldObjectEntity.ContentType,
			ETag:           obj.ETag,
			ChecksumType:   obj.ChecksumType,
			Size:           obj.Size,
			StorageClass:   oldObjectEntity.StorageClass,
			VersionID:      oldObjectEntity.VersionID,
			IsLatest:       true,
			IsDeleteMarker: false,
			UploadStatus:   object.UploadStatusCompleted,
		}
		// Appends preserve the object's existing metadata (like its content type).
		applySystemMetadataToEntity(&updatedEntity, systemMetadataFromEntity(oldObjectEntity))
		updated, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &updatedEntity, oldObjectEntity.OptimisticLockVersion)
		if err != nil {
			return err
		}
		if !*updated {
			return metadatastore.ErrCASFailure
		}

		err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return err
		}

		sequenceNumber := 0
		for _, partStruc := range obj.Parts {
			partEntity := part.Entity{
				PartId:            partStruc.Id,
				ObjectId:          *updatedEntity.Id,
				ETag:              partStruc.ETag,
				ChecksumCRC32:     partStruc.ChecksumCRC32,
				ChecksumCRC32C:    partStruc.ChecksumCRC32C,
				ChecksumCRC64NVME: partStruc.ChecksumCRC64NVME,
				ChecksumSHA1:      partStruc.ChecksumSHA1,
				ChecksumSHA256:    partStruc.ChecksumSHA256,
				Size:              partStruc.Size,
				SequenceNumber:    sequenceNumber,
				PartStoreName:     partStruc.StoreName,
			}
			err = sms.partRepository.SavePart(ctx, tx, &partEntity)
			if err != nil {
				return err
			}
			sequenceNumber++
		}
		return nil
	}

	// No existing object — create a new one (same semantics as PutObject).
	newEntity := object.Entity{
		BucketName:     bucketName,
		Key:            obj.Key,
		ContentType:    obj.ContentType,
		ETag:           obj.ETag,
		ChecksumType:   obj.ChecksumType,
		Size:           obj.Size,
		StorageClass:   obj.StorageClass,
		VersionID:      ptrutils.ToPtr("null"),
		IsLatest:       true,
		IsDeleteMarker: false,
		UploadStatus:   object.UploadStatusCompleted,
	}
	applySystemMetadataToEntity(&newEntity, obj.Metadata)
	err = sms.objectRepository.SaveObject(ctx, tx, &newEntity)
	if err != nil {
		return err
	}

	sequenceNumber := 0
	for _, partStruc := range obj.Parts {
		partEntity := part.Entity{
			PartId:            partStruc.Id,
			ObjectId:          *newEntity.Id,
			ETag:              partStruc.ETag,
			ChecksumCRC32:     partStruc.ChecksumCRC32,
			ChecksumCRC32C:    partStruc.ChecksumCRC32C,
			ChecksumCRC64NVME: partStruc.ChecksumCRC64NVME,
			ChecksumSHA1:      partStruc.ChecksumSHA1,
			ChecksumSHA256:    partStruc.ChecksumSHA256,
			Size:              partStruc.Size,
			SequenceNumber:    sequenceNumber,
			PartStoreName:     partStruc.StoreName,
		}
		err = sms.partRepository.SavePart(ctx, tx, &partEntity)
		if err != nil {
			return err
		}
		sequenceNumber++
	}
	return nil
}

func (sms *sqlMetadataStore) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.DeleteObjectOptions) (*metadatastore.DeleteObjectResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteObject")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	versioningStatus := ""
	if bucketEntity.VersioningStatus != nil {
		versioningStatus = *bucketEntity.VersioningStatus
	}

	currentEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}

	if opts != nil && opts.VersionID != nil {
		versionEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, *opts.VersionID)
		if err != nil {
			return nil, err
		}
		if versionEntity == nil {
			return &metadatastore.DeleteObjectResult{VersionID: opts.VersionID}, nil
		}

		if opts.IfMatchETag != nil {
			if *opts.IfMatchETag == metadatastore.ETagWildcard {
				// any existing version matches
			} else if versionEntity.IsDeleteMarker || versionEntity.ETag != *opts.IfMatchETag {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}

		if !versionEntity.IsDeleteMarker {
			err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *versionEntity.Id)
			if err != nil {
				return nil, err
			}
		}

		err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *versionEntity.Id)
		if err != nil {
			return nil, err
		}

		if versionEntity.IsLatest {
			nextLatest, err := sms.objectRepository.FindLatestObjectByBucketNameAndKeyExcludingID(ctx, tx, bucketName, key, *versionEntity.Id)
			if err != nil {
				return nil, err
			}
			if nextLatest != nil {
				nextLatest.IsLatest = true
				if err := sms.objectRepository.SaveObject(ctx, tx, nextLatest); err != nil {
					return nil, err
				}
			}
		}

		return &metadatastore.DeleteObjectResult{VersionID: versionEntity.VersionID, IsDeleteMarker: versionEntity.IsDeleteMarker}, nil
	}

	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			if currentEntity == nil || currentEntity.IsDeleteMarker {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else {
			if currentEntity == nil || currentEntity.IsDeleteMarker || currentEntity.ETag != *opts.IfMatchETag {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}
	}

	if versioningStatus == string(metadatastore.BucketVersioningStatusEnabled) || versioningStatus == string(metadatastore.BucketVersioningStatusSuspended) {
		if versioningStatus == string(metadatastore.BucketVersioningStatusSuspended) {
			nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, key)
			if err != nil {
				return nil, err
			}
			if nullVersionEntity != nil {
				err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}

				err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}

				_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}
			}
		}

		if currentEntity != nil {
			currentEntity.IsLatest = false
			if err := sms.objectRepository.SaveObject(ctx, tx, currentEntity); err != nil {
				return nil, err
			}
		}
		deleteMarkerVersionID := metadatastore.NewRandomUploadId().String()
		deleteMarker := object.Entity{
			BucketName:     bucketName,
			Key:            key,
			ETag:           "",
			Size:           0,
			VersionID:      &deleteMarkerVersionID,
			IsDeleteMarker: true,
			IsLatest:       true,
			UploadStatus:   object.UploadStatusCompleted,
		}
		if err := sms.objectRepository.SaveObject(ctx, tx, &deleteMarker); err != nil {
			return nil, err
		}
		return &metadatastore.DeleteObjectResult{VersionID: deleteMarker.VersionID, IsDeleteMarker: true}, nil
	}

	if currentEntity != nil {
		if opts != nil && opts.IfMatchETag != nil {
			lockedObjectEntity := *currentEntity
			locked, lockErr := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, currentEntity.OptimisticLockVersion)
			if lockErr != nil {
				return nil, lockErr
			}
			if !*locked {
				return nil, metadatastore.ErrPreconditionFailed
			}

			if !lockedObjectEntity.IsDeleteMarker {
				err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *lockedObjectEntity.Id)
				if err != nil {
					return nil, err
				}
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *lockedObjectEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *lockedObjectEntity.Id)
			if err != nil {
				return nil, err
			}

			deleted, deleteErr := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *lockedObjectEntity.Id, lockedObjectEntity.OptimisticLockVersion)
			if deleteErr != nil {
				return nil, deleteErr
			}
			if !*deleted {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else {
			if !currentEntity.IsDeleteMarker {
				err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *currentEntity.Id)
				if err != nil {
					return nil, err
				}
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}

			_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *currentEntity.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	return &metadatastore.DeleteObjectResult{}, nil
}

func (sms *sqlMetadataStore) GetObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) (map[string]string, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetObjectTagging")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	return sms.loadObjectTags(ctx, tx, *objectEntity.Id)
}

func (sms *sqlMetadataStore) PutObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, tags map[string]string) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObjectTagging")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	if err := sms.replaceObjectTags(ctx, tx, *objectEntity.Id, tags); err != nil {
		return err
	}

	// Bump the object's updated_at / optimistic lock version so the tag change
	// is reflected in the object metadata and concurrent writers are detected.
	return sms.objectRepository.SaveObject(ctx, tx, objectEntity)
}

func (sms *sqlMetadataStore) DeleteObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteObjectTagging")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	if err := sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *objectEntity.Id); err != nil {
		return err
	}

	return sms.objectRepository.SaveObject(ctx, tx, objectEntity)
}

func (sms *sqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, contentType *string, checksumType *string, opts *metadatastore.CreateMultipartUploadOptions) (*metadatastore.InitiateMultipartUploadResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CreateMultipartUpload")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if checksumType == nil {
		checksumType = ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject)
	}

	objectEntity := object.Entity{
		BucketName:     bucketName,
		Key:            key,
		ContentType:    contentType,
		ETag:           "",
		ChecksumType:   checksumType,
		Size:           -1,
		IsLatest:       false,
		IsDeleteMarker: false,
		UploadId:       ptrutils.ToPtr(metadatastore.NewRandomUploadId()),
		UploadStatus:   object.UploadStatusPending,
	}
	if opts != nil {
		// The class chosen at CreateMultipartUpload is carried to the final
		// object because CompleteMultipartUpload reuses this row.
		objectEntity.StorageClass = opts.StorageClass
	}
	if opts != nil && opts.Metadata != nil {
		applySystemMetadataToEntity(&objectEntity, *opts.Metadata)
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
	}

	// Persist any tags supplied via x-amz-tagging and any user-defined metadata
	// on the pending object. They are carried over when the upload is completed
	// (the object row is reused).
	if opts != nil && len(opts.Tags) > 0 {
		if err := sms.replaceObjectTags(ctx, tx, *objectEntity.Id, opts.Tags); err != nil {
			return nil, err
		}
	}
	if opts != nil && opts.Metadata != nil && len(opts.Metadata.UserMetadata) > 0 {
		if err := sms.replaceObjectUserMetadata(ctx, tx, *objectEntity.Id, opts.Metadata.UserMetadata); err != nil {
			return nil, err
		}
	}

	return &metadatastore.InitiateMultipartUploadResult{
		UploadId: *objectEntity.UploadId,
	}, nil
}

func (sms *sqlMetadataStore) UploadPart(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadID metadatastore.UploadId, partNumber int32, blb metadatastore.Part) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.UploadPart")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadID)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	partEntity := part.Entity{
		PartId:            blb.Id,
		ObjectId:          *objectEntity.Id,
		ETag:              blb.ETag,
		ChecksumCRC32:     blb.ChecksumCRC32,
		ChecksumCRC32C:    blb.ChecksumCRC32C,
		ChecksumCRC64NVME: blb.ChecksumCRC64NVME,
		ChecksumSHA1:      blb.ChecksumSHA1,
		ChecksumSHA256:    blb.ChecksumSHA256,
		Size:              blb.Size,
		SequenceNumber:    int(partNumber),
		PartStoreName:     blb.StoreName,
	}
	err = sms.partRepository.SavePart(ctx, tx, &partEntity)
	if err != nil {
		return err
	}

	return nil
}

func trimETagQuotes(etag string) string {
	return strings.Trim(etag, "\"")
}

func declaredPartChecksumMatches(declared *string, stored *string) bool {
	if declared == nil || stored == nil {
		return true
	}
	return *declared == *stored
}

// validateCompleteMultipartUploadParts checks the client-declared completion
// manifest against the uploaded parts loaded in this transaction.
func validateCompleteMultipartUploadParts(declaredParts []metadatastore.CompleteMultipartUploadPart, storedParts []part.Entity) error {
	if len(declaredParts) == 0 {
		return nil
	}

	storedPartsByNumber := map[int32]part.Entity{}
	for _, storedPart := range storedParts {
		storedPartsByNumber[int32(storedPart.SequenceNumber)] = storedPart
	}

	previousPartNumber := int32(0)
	for _, declaredPart := range declaredParts {
		if declaredPart.PartNumber <= previousPartNumber {
			return metadatastore.ErrInvalidPartOrder
		}
		previousPartNumber = declaredPart.PartNumber

		storedPart, ok := storedPartsByNumber[declaredPart.PartNumber]
		if !ok {
			return metadatastore.ErrInvalidPart
		}
		if declaredPart.ETag != "" && trimETagQuotes(declaredPart.ETag) != trimETagQuotes(storedPart.ETag) {
			return metadatastore.ErrInvalidPart
		}
		if !declaredPartChecksumMatches(declaredPart.ChecksumCRC32, storedPart.ChecksumCRC32) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumCRC32C, storedPart.ChecksumCRC32C) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumCRC64NVME, storedPart.ChecksumCRC64NVME) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumSHA1, storedPart.ChecksumSHA1) ||
			!declaredPartChecksumMatches(declaredPart.ChecksumSHA256, storedPart.ChecksumSHA256) {
			return metadatastore.ErrInvalidPart
		}
	}

	if len(declaredParts) != len(storedPartsByNumber) {
		return metadatastore.ErrInvalidPart
	}

	return nil
}

func (sms *sqlMetadataStore) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, checksumInput *metadatastore.ChecksumInput, opts *metadatastore.CompleteMultipartUploadOptions) (*metadatastore.CompleteMultipartUploadResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CompleteMultipartUpload")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	checksumType := metadatastore.ChecksumTypeFullObject
	if objectEntity.ChecksumType != nil {
		checksumType = *objectEntity.ChecksumType
	}

	// Validate SequenceNumbers and calculate totalSize
	var totalSize int64 = 0
	parts := make([]checksumutils.PartChecksums, len(partEntities))

	for i, partEntity := range partEntities {
		if i+1 != partEntity.SequenceNumber {
			return nil, metadatastore.ErrUploadWithInvalidSequenceNumber
		}
		totalSize += partEntity.Size

		parts[i] = checksumutils.PartChecksums{
			ETag:              partEntity.ETag,
			ChecksumCRC32:     partEntity.ChecksumCRC32,
			ChecksumCRC32C:    partEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      partEntity.ChecksumSHA1,
			ChecksumSHA256:    partEntity.ChecksumSHA256,
			Size:              partEntity.Size,
		}
	}

	if opts != nil {
		if err = validateCompleteMultipartUploadParts(opts.Parts, partEntities); err != nil {
			return nil, err
		}
	}

	calculatedChecksums, err := checksumutils.CalculateMultipartChecksums(parts, checksumType)
	if err != nil {
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, calculatedChecksums)
	if err != nil {
		return nil, err
	}

	deletedParts := []metadatastore.Part{}

	versioningEnabled := bucketEntity.VersioningStatus != nil && *bucketEntity.VersioningStatus == string(metadatastore.BucketVersioningStatusEnabled)

	latestObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}
	objectExists := latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker

	// Evaluate conditional headers (AWS S3 compatible behaviour):
	//   If-Match:      the current object's ETag must match the supplied value.
	//   If-None-Match: "*" means the operation must fail when any object exists.
	if opts != nil {
		if opts.IfMatchETag != nil {
			// If-Match: * — any existing non-delete-marker object satisfies the condition; fail when absent.
			// If-Match: <etag> — existing ETag must match exactly; fail otherwise.
			if *opts.IfMatchETag == metadatastore.ETagWildcard {
				if !objectExists {
					return nil, metadatastore.ErrPreconditionFailed
				}
			} else {
				if !objectExists || latestObjectEntity.ETag != *opts.IfMatchETag {
					return nil, metadatastore.ErrPreconditionFailed
				}
			}
		}
		if opts.IfNoneMatchStar {
			// If-None-Match: * — fail when any existing non-delete-marker object currently exists at the key.
			if objectExists {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}
	}
	if opts != nil && opts.IfNoneMatchStar {
		latestObjectEntity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
		if err != nil {
			return nil, err
		}
		if latestObjectEntity != nil && !latestObjectEntity.IsDeleteMarker {
			return nil, metadatastore.ErrPreconditionFailed
		}
	}

	if opts != nil && (opts.IfMatchETag != nil || opts.IfNoneMatchStar) && latestObjectEntity != nil {
		lockedObjectEntity := *latestObjectEntity
		locked, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, latestObjectEntity.OptimisticLockVersion)
		if err != nil {
			return nil, err
		}
		if !*locked {
			return nil, metadatastore.ErrPreconditionFailed
		}
		latestObjectEntity = &lockedObjectEntity
	}

	if versioningEnabled {
		newVersionID := metadatastore.NewRandomUploadId().String()
		objectEntity.VersionID = &newVersionID
	} else {
		nullVersionEntity, err := sms.objectRepository.FindNullObjectVersionByBucketNameAndKey(ctx, tx, bucketName, key)
		if err != nil {
			return nil, err
		}
		if opts != nil && opts.IfNoneMatchStar && nullVersionEntity != nil {
			return nil, metadatastore.ErrPreconditionFailed
		}
		if nullVersionEntity != nil {
			oldPartEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *nullVersionEntity.Id)
			if err != nil {
				return nil, err
			}

			deletedParts = append(deletedParts, sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
				return metadatastore.Part{Id: partEntity.PartId, ETag: partEntity.ETag, ChecksumCRC32: partEntity.ChecksumCRC32, ChecksumCRC32C: partEntity.ChecksumCRC32C, ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME, ChecksumSHA1: partEntity.ChecksumSHA1, ChecksumSHA256: partEntity.ChecksumSHA256, Size: partEntity.Size, StoreName: partEntity.PartStoreName}
			}, oldPartEntities)...)

			if opts != nil && opts.IfMatchETag != nil && latestObjectEntity != nil && latestObjectEntity.Id != nil && nullVersionEntity.Id != nil && *latestObjectEntity.Id == *nullVersionEntity.Id {
				deleted, deleteErr := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *nullVersionEntity.Id, latestObjectEntity.OptimisticLockVersion)
				if deleteErr != nil {
					return nil, deleteErr
				}
				if !*deleted {
					return nil, metadatastore.ErrPreconditionFailed
				}
			} else {
				_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *nullVersionEntity.Id)
				if err != nil {
					return nil, err
				}
			}
		}

		nullVersion := "null"
		objectEntity.VersionID = &nullVersion
	}

	if latestObjectEntity != nil {
		latestObjectEntity.IsLatest = false
		err = sms.objectRepository.SaveObject(ctx, tx, latestObjectEntity)
		if err != nil {
			return nil, err
		}
	}

	objectEntity.UploadStatus = object.UploadStatusCompleted
	objectEntity.UploadId = nil
	objectEntity.IsDeleteMarker = false
	objectEntity.IsLatest = true
	objectEntity.Size = totalSize
	objectEntity.ETag = *calculatedChecksums.ETag
	objectEntity.ChecksumCRC32 = calculatedChecksums.ChecksumCRC32
	objectEntity.ChecksumCRC32C = calculatedChecksums.ChecksumCRC32C
	objectEntity.ChecksumCRC64NVME = calculatedChecksums.ChecksumCRC64NVME
	objectEntity.ChecksumSHA1 = calculatedChecksums.ChecksumSHA1
	objectEntity.ChecksumSHA256 = calculatedChecksums.ChecksumSHA256
	objectEntity.ChecksumType = ptrutils.ToPtr(checksumType)

	err = sms.objectRepository.SaveObject(ctx, tx, objectEntity)
	if err != nil {
		if opts != nil && opts.IfNoneMatchStar && isUniqueConstraintViolation(err) {
			return nil, metadatastore.ErrPreconditionFailed
		}
		return nil, err
	}

	return &metadatastore.CompleteMultipartUploadResult{
		DeletedParts:      deletedParts,
		ETag:              objectEntity.ETag,
		VersionID:         objectEntity.VersionID,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
	}, nil
}

func (sms *sqlMetadataStore) AbortMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId) (*metadatastore.AbortMultipartResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AbortMultipartUpload")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	err = sms.userMetadataRepository.DeleteUserMetadataByObjectId(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	parts := sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
		return metadatastore.Part{
			Id:                partEntity.PartId,
			ETag:              partEntity.ETag,
			ChecksumCRC32:     partEntity.ChecksumCRC32,
			ChecksumCRC32C:    partEntity.ChecksumCRC32C,
			ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
			ChecksumSHA1:      partEntity.ChecksumSHA1,
			ChecksumSHA256:    partEntity.ChecksumSHA256,
			Size:              partEntity.Size,
			StoreName:         partEntity.PartStoreName,
		}
	}, partEntities)

	_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	return &metadatastore.AbortMultipartResult{
		DeletedParts: parts,
	}, nil
}

func (sms *sqlMetadataStore) ListMultipartUploads(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, opts metadatastore.ListMultipartUploadsOptions) (*metadatastore.ListMultipartUploadsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListMultipartUploads")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	prefix := ""
	if opts.Prefix != nil {
		prefix = *opts.Prefix
	}
	keyMarker := ""
	if opts.KeyMarker != nil {
		keyMarker = *opts.KeyMarker
	}
	uploadIdMarker := ""
	if opts.UploadIdMarker != nil {
		uploadIdMarker = *opts.UploadIdMarker
	}
	delimiter := ""
	if opts.Delimiter != nil {
		delimiter = *opts.Delimiter
	}

	commonPrefixes := []string{}
	commonPrefixSet := map[string]struct{}{}
	uploads := []metadatastore.Upload{}
	isTruncated := false
	var objectEntities []object.Entity
	if delimiter == "" {
		objectEntities, err = sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAscWithLimit(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker, opts.MaxUploads+1)
		if err != nil {
			return nil, err
		}
		if int32(len(objectEntities)) > opts.MaxUploads {
			isTruncated = true
			objectEntities = objectEntities[:opts.MaxUploads]
		}
	} else {
		keyCount, err := sms.objectRepository.CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
		if err != nil {
			return nil, err
		}
		isTruncated = int32(*keyCount) > opts.MaxUploads
		objectEntities, err = sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
		if err != nil {
			return nil, err
		}
	}

	nextKeyMarker := ""
	nextUploadIdMarker := ""

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key.String(), delimiter)
			if commonPrefix != nil {
				if _, seen := commonPrefixSet[*commonPrefix]; !seen {
					commonPrefixSet[*commonPrefix] = struct{}{}
					commonPrefixes = append(commonPrefixes, *commonPrefix)
				}
			}
		}
		if int32(len(uploads)) < opts.MaxUploads {
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				uploads = append(uploads, metadatastore.Upload{
					Key:          objectEntity.Key,
					UploadId:     *objectEntity.UploadId,
					Initiated:    objectEntity.CreatedAt,
					StorageClass: objectEntity.StorageClass,
				})
			}
			nextKeyMarker = objectEntity.Key.String()
			nextUploadIdMarker = objectEntity.UploadId.String()
		}
	}

	listMultipartUploadsResult := metadatastore.ListMultipartUploadsResult{
		Bucket:             bucketName,
		KeyMarker:          keyMarker,
		UploadIdMarker:     uploadIdMarker,
		Prefix:             prefix,
		Delimiter:          delimiter,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIdMarker,
		MaxUploads:         opts.MaxUploads,
		CommonPrefixes:     commonPrefixes,
		Uploads:            uploads,
		IsTruncated:        isTruncated,
	}
	return &listMultipartUploadsResult, nil
}

func (sms *sqlMetadataStore) ListParts(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, opts metadatastore.ListPartsOptions) (*metadatastore.ListPartsResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListParts")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}

	objectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKeyAndUploadId(ctx, tx, bucketName, key, uploadId)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil || objectEntity.UploadStatus != object.UploadStatusPending {
		return nil, metadatastore.ErrNoSuchKey
	}

	partEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objectEntity.Id)
	if err != nil {
		return nil, err
	}

	var partNumberMarkerI32 int32 = 0
	partNumberMarker := ""
	if opts.PartNumberMarker != nil {
		partNumberMarker = *opts.PartNumberMarker
	}
	if partNumberMarker != "" {
		partNumberMarkerI64, err := strconv.ParseInt(partNumberMarker, 10, 32)
		if err != nil {
			return nil, err
		}
		partNumberMarkerI32 = int32(partNumberMarkerI64)
	}

	parts := []*metadatastore.MultipartPart{}
	var nextPartNumberMarker *string = nil
	isTruncated := false
	for idx, part := range partEntities {
		sequenceNumberI32 := int32(part.SequenceNumber)
		if sequenceNumberI32 <= partNumberMarkerI32 {
			continue
		}
		parts = append(parts, &metadatastore.MultipartPart{
			ETag:              part.ETag,
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			LastModified:      part.UpdatedAt,
			PartNumber:        sequenceNumberI32,
			Size:              part.Size,
		})
		if len(parts) >= int(opts.MaxParts) {
			isTruncated = idx < len(partEntities)-1
			lastPartNumberMarker := strconv.Itoa(part.SequenceNumber)
			nextPartNumberMarker = &lastPartNumberMarker
			break
		}
	}

	return &metadatastore.ListPartsResult{
		BucketName:           bucketName,
		Key:                  key,
		UploadId:             uploadId,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             opts.MaxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
		StorageClass:         objectEntity.StorageClass,
	}, nil
}
