package config

import (
	"database/sql"
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

const (
	SqliteDatabaseType = "SqliteDatabase"
)

type DatabaseInstantiator = internalConfig.DynamicJsonInstantiator[*sql.DB]

type SqliteDatabaseConfiguration struct {
	StoragePath string `json:"storagePath"`
	internalConfig.DynamicJsonType
}

func (s *SqliteDatabaseConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	return database.OpenDatabase(s.StoragePath)
}

func CreateDatabaseInstantiatorFromJson(b []byte) (DatabaseInstantiator, error) {
	var dc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &dc)
	if err != nil {
		return nil, err
	}

	var di DatabaseInstantiator
	switch dc.Type {
	case SqliteDatabaseType:
		di = &SqliteDatabaseConfiguration{}
	default:
		return nil, errors.New("unknown database type")
	}
	err = json.Unmarshal(b, &di)
	if err != nil {
		return nil, err
	}
	return di, nil
}
