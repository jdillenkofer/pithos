package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
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
	partRegistryRepository partregistry.Repository
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
	partRegistryRepository, err := repositoryfactory.NewPartRegistryRepository(db)
	if err != nil {
		return nil, err
	}
	return &sqlMetadataStore{
		ValidatedLifecycle:     lifecycle,
		bucketRepository:       bucketRepository,
		objectRepository:       objectRepository,
		partRepository:         partRepository,
		partRegistryRepository: partRegistryRepository,
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

func (sms *sqlMetadataStore) GetInUsePartIdCounts(ctx context.Context, tx *sql.Tx) (map[partstore.PartId]int64, error) {
	return sms.partRepository.FindInUsePartIdCounts(ctx, tx)
}

func (sms *sqlMetadataStore) TryAddPartReferences(ctx context.Context, tx *sql.Tx, partIds []partstore.PartId) (bool, error) {
	counts := map[partstore.PartId]int64{}
	for _, id := range partIds {
		counts[id]++
	}
	refs := make([]partregistry.Ref, 0, len(counts))
	for id, count := range counts {
		refs = append(refs, partregistry.Ref{PartId: id, Delta: count})
	}
	return sms.partRegistryRepository.TryAddReferences(ctx, tx, refs)
}
