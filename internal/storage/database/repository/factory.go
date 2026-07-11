package repository

import (
	"errors"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	postgresBucket "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bucket"
	postgresObject "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/object"
	postgresPart "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/part"
	postgresPartContent "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partcontent"
	postgresPartDedupIndex "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partdedupindex"
	postgresPartOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partoutboxentry"
	postgresPartRegistry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partregistry"
	postgresStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/storageoutboxentry"
	postgresTag "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/tag"
	postgresUserMetadata "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/usermetadata"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partdedupindex"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/tag"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/usermetadata"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/bucket"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/object"
	sqlitePart "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/part"
	sqlitePartContent "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partcontent"
	sqlitePartDedupIndex "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partdedupindex"
	sqlitePartOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partoutboxentry"
	sqlitePartRegistry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partregistry"
	sqliteStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/storageoutboxentry"
	sqliteTag "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/tag"
	sqliteUserMetadata "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/usermetadata"
)

var errUnknownDatabaseType = errors.New("unknown database type")

func NewStorageOutboxEntryRepository(db database.Database) (storageoutboxentry.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresStorageOutboxEntry.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteStorageOutboxEntry.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewPartOutboxEntryRepository(db database.Database) (partoutboxentry.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresPartOutboxEntry.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqlitePartOutboxEntry.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewBucketRepository(db database.Database) (bucket.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresBucket.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteBucket.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewObjectRepository(db database.Database) (object.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresObject.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteObject.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewPartRepository(db database.Database) (part.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresPart.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqlitePart.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewPartRegistryRepository(db database.Database) (partregistry.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresPartRegistry.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqlitePartRegistry.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewPartDedupIndexRepository(db database.Database) (partdedupindex.Repository, error) {
	switch db.GetDatabaseType() {
	case database.DB_TYPE_POSTGRES:
		return postgresPartDedupIndex.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqlitePartDedupIndex.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewTagRepository(db database.Database) (tag.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresTag.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteTag.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewUserMetadataRepository(db database.Database) (usermetadata.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresUserMetadata.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteUserMetadata.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewPartContentRepository(db database.Database) (partcontent.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresPartContent.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqlitePartContent.NewRepository()
	}
	return nil, errUnknownDatabaseType
}
