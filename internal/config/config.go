package config

import (
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
)

type DynamicJsonType struct {
	Type string `json:"type"`
}

type DynamicJsonInstantiator[T any] interface {
	RegisterReferences(diCollection dependencyinjection.DICollection) error
	Instantiate(diProvider dependencyinjection.DIProvider) (T, error)
}

type DbContainer struct {
	dbs []*sql.DB
}

func NewDbContainer() *DbContainer {
	return &DbContainer{}
}

func (dbContainer *DbContainer) AddDb(db *sql.DB) {
	dbContainer.dbs = append(dbContainer.dbs, db)
}

func (dbContainer *DbContainer) Dbs() []*sql.DB {
	return dbContainer.dbs
}
