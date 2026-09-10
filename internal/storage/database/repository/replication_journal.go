package repository

import (
	"github.com/jdillenkofer/pithos/internal/storage/database"
	pg "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/replicationjournal"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/replicationjournal"
	sq "github.com/jdillenkofer/pithos/internal/storage/database/sqlite/repository/replicationjournal"
)

func NewReplicationJournalRepository(db database.Database) (replicationjournal.Repository, error) {
	switch db.GetDatabaseType() {
	case database.DB_TYPE_POSTGRES:
		return pg.NewRepository()
	case database.DB_TYPE_SQLITE:
		return sq.NewRepository()
	}
	return nil, errUnknownDatabaseType
}
