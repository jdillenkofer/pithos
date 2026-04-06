package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
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
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	sqlite3 "github.com/mattn/go-sqlite3"
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
	bucketRepository bucket.Repository
	objectRepository object.Repository
	partRepository   part.Repository
	tracer           trace.Tracer
}

// Compile-time check to ensure sqlMetadataStore implements metadatastore.MetadataStore
var _ metadatastore.MetadataStore = (*sqlMetadataStore)(nil)

func New(db database.Database, bucketRepository bucket.Repository, objectRepository object.Repository, partRepository part.Repository) (metadatastore.MetadataStore, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("SqlMetadataStore")
	if err != nil {
		return nil, err
	}
	return &sqlMetadataStore{
		ValidatedLifecycle: lifecycle,
		bucketRepository:   bucketRepository,
		objectRepository:   objectRepository,
		partRepository:     partRepository,
		tracer:             otel.Tracer("internal/storage/metadatapart/metadatastore/sql"),
	}, nil
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

	if bucketEntity.WebsiteIndexDocumentSuffix == nil {
		return nil, metadatastore.ErrNoSuchWebsiteConfiguration
	}

	return &metadatastore.WebsiteConfiguration{
		IndexDocumentSuffix: *bucketEntity.WebsiteIndexDocumentSuffix,
		ErrorDocumentKey:    bucketEntity.WebsiteErrorDocumentKey,
	}, nil
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

	bucketEntity.WebsiteIndexDocumentSuffix = &config.IndexDocumentSuffix
	bucketEntity.WebsiteErrorDocumentKey = config.ErrorDocumentKey

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

	keyCount, err := sms.objectRepository.CountObjectsByBucketNameAndPrefixAndStartAfter(ctx, tx, bucketName, prefix, startAfter)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	objects := []metadatastore.Object{}
	objectEntities, err := sms.objectRepository.FindObjectsByBucketNameAndPrefixAndStartAfterOrderByKeyAsc(ctx, tx, bucketName, prefix, startAfter)
	if err != nil {
		return nil, err
	}

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key.String(), delimiter)
			if commonPrefix != nil && !slices.Contains(commonPrefixes, *commonPrefix) {
				commonPrefixes = append(commonPrefixes, *commonPrefix)
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
					}
					parts = append(parts, partStruc)
				}
			}
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				objects = append(objects, metadatastore.Object{
					Key:               objectEntity.Key,
					LastModified:      objectEntity.UpdatedAt,
					ETag:              objectEntity.ETag,
					ChecksumCRC32:     objectEntity.ChecksumCRC32,
					ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
					ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
					ChecksumSHA1:      objectEntity.ChecksumSHA1,
					ChecksumSHA256:    objectEntity.ChecksumSHA256,
					ChecksumType:      objectEntity.ChecksumType,
					Size:              objectEntity.Size,
					Parts:             parts,
				})
			}
		}
	}

	listBucketResult := metadatastore.ListBucketResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    int32(*keyCount) > maxKeys,
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
		}
	}, partEntities)

	return &metadatastore.Object{
		Key:               key,
		ContentType:       objectEntity.ContentType,
		LastModified:      objectEntity.UpdatedAt,
		ETag:              objectEntity.ETag,
		ChecksumCRC32:     objectEntity.ChecksumCRC32,
		ChecksumCRC32C:    objectEntity.ChecksumCRC32C,
		ChecksumCRC64NVME: objectEntity.ChecksumCRC64NVME,
		ChecksumSHA1:      objectEntity.ChecksumSHA1,
		ChecksumSHA256:    objectEntity.ChecksumSHA256,
		ChecksumType:      objectEntity.ChecksumType,
		Size:              objectEntity.Size,
		Parts:             parts,
	}, nil
}

func (sms *sqlMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObject")
	defer span.End()

	existsBucket, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*existsBucket {
		return metadatastore.ErrNoSuchBucket
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
		UploadStatus:      object.UploadStatusCompleted,
	}

	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	if opts != nil && opts.IfMatchETag != nil {
		if oldObjectEntity == nil || oldObjectEntity.ETag != *opts.IfMatchETag {
			return metadatastore.ErrPreconditionFailed
		}

		objectEntity.Id = oldObjectEntity.Id
		updated, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &objectEntity, oldObjectEntity.OptimisticLockVersion)
		if err != nil {
			return err
		}
		if !*updated {
			return metadatastore.ErrPreconditionFailed
		}

		err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *objectEntity.Id)
		if err != nil {
			return err
		}
	} else if opts != nil && opts.IfNoneMatchStar {
		inserted, err := sms.objectRepository.InsertObjectIfAbsent(ctx, tx, &objectEntity)
		if err != nil {
			return err
		}
		if !*inserted {
			return metadatastore.ErrPreconditionFailed
		}
	} else if oldObjectEntity != nil {
		objectEntity.Id = oldObjectEntity.Id
		objectEntity.OptimisticLockVersion = oldObjectEntity.OptimisticLockVersion
		err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
		if err != nil {
			return err
		}

		err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *objectEntity.Id)
		if err != nil {
			return err
		}
	} else {
		err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
		if err != nil {
			return err
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
		}
		err = sms.partRepository.SavePart(ctx, tx, &partEntity)
		if err != nil {
			return err
		}
		sequenceNumber += 1
	}

	return nil
}

func (sms *sqlMetadataStore) AppendObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.AppendObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.AppendObject")
	defer span.End()

	existsBucket, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*existsBucket {
		return metadatastore.ErrNoSuchBucket
	}

	// Check whether an object already exists at this key.
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, obj.Key)
	if err != nil {
		return err
	}

	if oldObjectEntity != nil {
		updatedEntity := object.Entity{
			Id:           oldObjectEntity.Id,
			BucketName:   bucketName,
			Key:          obj.Key,
			ContentType:  oldObjectEntity.ContentType,
			ETag:         obj.ETag,
			ChecksumType: obj.ChecksumType,
			Size:         obj.Size,
			UploadStatus: object.UploadStatusCompleted,
		}
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
		BucketName:   bucketName,
		Key:          obj.Key,
		ContentType:  obj.ContentType,
		ETag:         obj.ETag,
		ChecksumType: obj.ChecksumType,
		Size:         obj.Size,
		UploadStatus: object.UploadStatusCompleted,
	}
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
		}
		err = sms.partRepository.SavePart(ctx, tx, &partEntity)
		if err != nil {
			return err
		}
		sequenceNumber++
	}
	return nil
}

func (sms *sqlMetadataStore) DeleteObject(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.DeleteObjectOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteObject")
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

	if opts != nil && opts.IfMatchETag != nil {
		if *opts.IfMatchETag == metadatastore.ETagWildcard {
			// If-Match: * — object must exist (any ETag); 412 if absent.
			if objectEntity == nil {
				return metadatastore.ErrPreconditionFailed
			}
		} else {
			// Conditional delete: object must exist and ETag must match exactly.
			if objectEntity == nil || objectEntity.ETag != *opts.IfMatchETag {
				return metadatastore.ErrPreconditionFailed
			}
		}
	}

	if objectEntity != nil {
		if opts != nil && opts.IfMatchETag != nil {
			lockedObjectEntity := *objectEntity
			locked, err := sms.objectRepository.UpdateObjectByIdAndOptimisticLockVersion(ctx, tx, &lockedObjectEntity, objectEntity.OptimisticLockVersion)
			if err != nil {
				return err
			}
			if !*locked {
				return metadatastore.ErrPreconditionFailed
			}

			err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *lockedObjectEntity.Id)
			if err != nil {
				return err
			}

			deleted, err := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *lockedObjectEntity.Id, lockedObjectEntity.OptimisticLockVersion)
			if err != nil {
				return err
			}
			if !*deleted {
				return metadatastore.ErrPreconditionFailed
			}
		} else {
			err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *objectEntity.Id)
			if err != nil {
				return err
			}

			_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *objectEntity.Id)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sms *sqlMetadataStore) CreateMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, contentType *string, checksumType *string) (*metadatastore.InitiateMultipartUploadResult, error) {
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
		BucketName:   bucketName,
		Key:          key,
		ContentType:  contentType,
		ETag:         "",
		ChecksumType: checksumType,
		Size:         -1,
		UploadId:     ptrutils.ToPtr(metadatastore.NewRandomUploadId()),
		UploadStatus: object.UploadStatusPending,
	}
	err = sms.objectRepository.SaveObject(ctx, tx, &objectEntity)
	if err != nil {
		return nil, err
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
	}
	err = sms.partRepository.SavePart(ctx, tx, &partEntity)
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) CompleteMultipartUpload(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, uploadId metadatastore.UploadId, checksumInput *metadatastore.ChecksumInput, opts *metadatastore.CompleteMultipartUploadOptions) (*metadatastore.CompleteMultipartUploadResult, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CompleteMultipartUpload")
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

	calculatedChecksums, err := checksumutils.CalculateMultipartChecksums(parts, checksumType)
	if err != nil {
		return nil, err
	}

	err = metadatastore.ValidateChecksums(checksumInput, calculatedChecksums)
	if err != nil {
		return nil, err
	}

	deletedParts := []metadatastore.Part{}

	// Remove old objects
	oldObjectEntity, err := sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
	if err != nil {
		return nil, err
	}

	// Evaluate conditional headers (AWS S3 compatible behaviour):
	//   If-Match:      the current object's ETag must match the supplied value.
	//   If-None-Match: "*" means the operation must fail when any object exists.
	if opts != nil {
		if opts.IfMatchETag != nil {
			// If-Match: * — any existing object satisfies the condition; fail when absent.
			// If-Match: <etag> — existing ETag must match exactly; fail otherwise.
			if *opts.IfMatchETag == metadatastore.ETagWildcard {
				if oldObjectEntity == nil {
					return nil, metadatastore.ErrPreconditionFailed
				}
			} else {
				if oldObjectEntity == nil || oldObjectEntity.ETag != *opts.IfMatchETag {
					return nil, metadatastore.ErrPreconditionFailed
				}
			}
		}
		if opts.IfNoneMatchStar {
			// If-None-Match: * — fail when any object currently exists at the key.
			if oldObjectEntity != nil {
				return nil, metadatastore.ErrPreconditionFailed
			}
		}
	}

	if oldObjectEntity != nil {
		oldPartEntities, err := sms.partRepository.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}

		err = sms.partRepository.DeletePartsByObjectId(ctx, tx, *oldObjectEntity.Id)
		if err != nil {
			return nil, err
		}

		deletedParts = sliceutils.Map(func(partEntity part.Entity) metadatastore.Part {
			return metadatastore.Part{
				Id:                partEntity.PartId,
				ETag:              partEntity.ETag,
				ChecksumCRC32:     partEntity.ChecksumCRC32,
				ChecksumCRC32C:    partEntity.ChecksumCRC32C,
				ChecksumCRC64NVME: partEntity.ChecksumCRC64NVME,
				ChecksumSHA1:      partEntity.ChecksumSHA1,
				ChecksumSHA256:    partEntity.ChecksumSHA256,
				Size:              partEntity.Size,
			}
		}, oldPartEntities)

		if opts != nil && opts.IfMatchETag != nil {
			deleted, err := sms.objectRepository.DeleteObjectByIdAndOptimisticLockVersion(ctx, tx, *oldObjectEntity.Id, oldObjectEntity.OptimisticLockVersion)
			if err != nil {
				return nil, err
			}
			if !*deleted {
				return nil, metadatastore.ErrPreconditionFailed
			}
		} else {
			_, err = sms.objectRepository.DeleteObjectById(ctx, tx, *oldObjectEntity.Id)
			if err != nil {
				return nil, err
			}
		}
	}

	objectEntity.UploadStatus = object.UploadStatusCompleted
	objectEntity.UploadId = nil
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

	keyCount, err := sms.objectRepository.CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
	if err != nil {
		return nil, err
	}
	commonPrefixes := []string{}
	uploads := []metadatastore.Upload{}
	objectEntities, err := sms.objectRepository.FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(ctx, tx, bucketName, prefix, keyMarker, uploadIdMarker)
	if err != nil {
		return nil, err
	}

	nextKeyMarker := ""
	nextUploadIdMarker := ""

	for _, objectEntity := range objectEntities {
		if delimiter != "" {
			commonPrefix := determineCommonPrefix(prefix, objectEntity.Key.String(), delimiter)
			if commonPrefix != nil && !slices.Contains(commonPrefixes, *commonPrefix) {
				commonPrefixes = append(commonPrefixes, *commonPrefix)
			}
		}
		if int32(len(uploads)) < opts.MaxUploads {
			keyWithoutPrefix := strings.TrimPrefix(objectEntity.Key.String(), prefix)
			if delimiter == "" || !strings.Contains(keyWithoutPrefix, delimiter) {
				uploads = append(uploads, metadatastore.Upload{
					Key:       objectEntity.Key,
					UploadId:  *objectEntity.UploadId,
					Initiated: objectEntity.CreatedAt,
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
		IsTruncated:        int32(*keyCount) > opts.MaxUploads,
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
	}, nil
}
