package repository

import (
	"errors"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	postgresPart "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/part"
	postgresPartContent "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partcontent"
	postgresPartOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partoutboxentry"
	postgresBucket "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bucket"
	postgresObject "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/object"
	postgresStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	sqlitePart "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/part"
	sqlitePartContent "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partcontent"
	sqlitePartOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/partoutboxentry"
	sqliteBucket "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/bucket"
	sqliteObject "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/object"
	sqliteStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/storageoutboxentry"
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
