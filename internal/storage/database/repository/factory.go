package repository

import (
	"errors"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	postgresBlob "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/blob"
	postgresBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/blobcontent"
	postgresBlobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bloboutboxentry"
	postgresBucket "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/bucket"
	postgresObject "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/object"
	postgresStorageOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blob"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	sqliteBlob "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/blob"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/blobcontent"
	sqliteBlobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/bloboutboxentry"
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

func NewBlobOutboxEntryRepository(db database.Database) (bloboutboxentry.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresBlobOutboxEntry.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteBlobOutboxEntry.NewRepository()
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

func NewBlobRepository(db database.Database) (blob.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresBlob.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteBlob.NewRepository()
	}
	return nil, errUnknownDatabaseType
}

func NewBlobContentRepository(db database.Database) (blobcontent.Repository, error) {
	dbType := db.GetDatabaseType()
	switch dbType {
	case database.DB_TYPE_POSTGRES:
		return postgresBlobContent.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteBlobContent.NewRepository()
	}
	return nil, errUnknownDatabaseType
}
