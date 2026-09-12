package repository

import (
	"github.com/jdillenkofer/pithos/internal/storage/database"
	postgresObjectLock "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/objectlock"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/objectlock"
	sqliteObjectLock "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/objectlock"
)

func NewObjectLockRepository(db database.Database) (objectlock.Repository, error) {
	switch db.GetDatabaseType() {
	case database.DB_TYPE_POSTGRES:
		return postgresObjectLock.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sqliteObjectLock.NewRepository()
	}
	return nil, errUnknownDatabaseType
}
