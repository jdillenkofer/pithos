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
	SqliteDatabaseType            = "SqliteDatabase"
	DatabaseReferenceType         = "DatabaseReference"
	RegisterDatabaseReferenceType = "RegisterDatabaseReference"
)

type DatabaseInstantiator = internalConfig.DynamicJsonInstantiator[*sql.DB]

type SqliteDatabaseConfiguration struct {
	StoragePath string `json:"storagePath"`
	internalConfig.DynamicJsonType
}

func (s *SqliteDatabaseConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *SqliteDatabaseConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	return database.OpenDatabase(s.StoragePath)
}

type DatabaseReferenceConfiguration struct {
	RefName string `json:"refName"`
	internalConfig.DynamicJsonType
}

func (ref *DatabaseReferenceConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (ref *DatabaseReferenceConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	di, err := diProvider.LookupByName(ref.RefName)
	if err != nil {
		return nil, err
	}
	databaseInstantiator := di.(DatabaseInstantiator)
	db, err := databaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type RegisterDatabaseReferenceConfiguration struct {
	RefName              string               `json:"refName"`
	DatabaseInstantiator DatabaseInstantiator `json:"-"`
	RawDatabase          json.RawMessage      `json:"db"`
	internalConfig.DynamicJsonType
}

func (regRef *RegisterDatabaseReferenceConfiguration) UnmarshalJSON(b []byte) error {
	type registerDatabaseReferenceConfiguration RegisterDatabaseReferenceConfiguration
	err := json.Unmarshal(b, (*registerDatabaseReferenceConfiguration)(regRef))
	if err != nil {
		return err
	}
	regRef.DatabaseInstantiator, err = CreateDatabaseInstantiatorFromJson(regRef.RawDatabase)
	if err != nil {
		return err
	}
	return nil
}

func (regRef *RegisterDatabaseReferenceConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := diCollection.RegisterSingletonByName(regRef.RefName, regRef.DatabaseInstantiator)
	if err != nil {
		return err
	}
	err = regRef.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (regRef *RegisterDatabaseReferenceConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	db, err := regRef.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return db, nil
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
	case DatabaseReferenceType:
		di = &DatabaseReferenceConfiguration{}
	case RegisterDatabaseReferenceType:
		di = &RegisterDatabaseReferenceConfiguration{}
	default:
		return nil, errors.New("unknown database type")
	}
	err = json.Unmarshal(b, &di)
	if err != nil {
		return nil, err
	}
	return di, nil
}
