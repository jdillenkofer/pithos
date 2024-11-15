package config

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

const (
	sqliteDatabaseType            = "SqliteDatabase"
	databaseReferenceType         = "DatabaseReference"
	registerDatabaseReferenceType = "RegisterDatabaseReference"
)

type DatabaseInstantiator = internalConfig.DynamicJsonInstantiator[*sql.DB]

type SqliteDatabaseConfiguration struct {
	dbInstance *sql.DB
	DbPath     internalConfig.StringProvider `json:"dbPath"`
	internalConfig.DynamicJsonType
}

func (s *SqliteDatabaseConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *SqliteDatabaseConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	if s.dbInstance == nil {
		dbInstance, err := database.OpenDatabase(s.DbPath.Value())
		if err != nil {
			return nil, err
		}
		s.dbInstance = dbInstance

		dc, err := diProvider.LookupByType(reflect.TypeOf((*internalConfig.DbContainer)(nil)))
		if err != nil {
			return nil, err
		}
		dbContainer := dc.(*internalConfig.DbContainer)
		dbContainer.AddDb(dbInstance)
	}
	return s.dbInstance, nil
}

type DatabaseReferenceConfiguration struct {
	RefName internalConfig.StringProvider `json:"refName"`
	internalConfig.DynamicJsonType
}

func (ref *DatabaseReferenceConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (ref *DatabaseReferenceConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*sql.DB, error) {
	di, err := diProvider.LookupByName(ref.RefName.Value())
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
	RefName              internalConfig.StringProvider `json:"refName"`
	DatabaseInstantiator DatabaseInstantiator          `json:"-"`
	RawDatabase          json.RawMessage               `json:"db"`
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
	err := diCollection.RegisterSingletonByName(regRef.RefName.Value(), regRef.DatabaseInstantiator)
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
	case sqliteDatabaseType:
		di = &SqliteDatabaseConfiguration{}
	case databaseReferenceType:
		di = &DatabaseReferenceConfiguration{}
	case registerDatabaseReferenceType:
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
