package config

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strconv"

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

func CreateTempDir() (tempDir *string, cleanup func(), err error) {
	d, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		return nil, nil, err
	}
	tempDir = &d
	cleanup = func() {
		_ = os.RemoveAll(*tempDir)
	}
	return
}

const (
	envKeyType = "EnvKey"
)

type envKeyProvider struct {
	EnvKey string `json:"envKey"`
	DynamicJsonType
}

type StringProvider struct {
	value string `json:"-"`
}

func (s *StringProvider) Value() string {
	return s.value
}

func (s *StringProvider) UnmarshalJSON(b []byte) error {
	var rawString string
	err := json.Unmarshal(b, &rawString)
	if err == nil {
		s.value = rawString
		return nil
	}
	ekp := envKeyProvider{}
	err = json.Unmarshal(b, &ekp)
	if err != nil {
		return err
	}
	if ekp.Type != envKeyType {
		return errors.New("invalid stringProvider type")
	}
	s.value = os.Getenv(ekp.EnvKey)
	return nil
}

type Int64Provider struct {
	value int64
}

func (i *Int64Provider) Value() int64 {
	return i.value
}

func (i *Int64Provider) UnmarshalJSON(b []byte) error {
	var rawInt64 int64
	err := json.Unmarshal(b, &rawInt64)
	if err == nil {
		i.value = rawInt64
		return nil
	}
	ekp := envKeyProvider{}
	err = json.Unmarshal(b, &ekp)
	if err != nil {
		return err
	}
	if ekp.Type != envKeyType {
		return errors.New("invalid int64Provider type")
	}
	envInt64 := os.Getenv(ekp.EnvKey)
	i.value, err = strconv.ParseInt(envInt64, 10, 64)
	if err != nil {
		return err
	}
	return nil
}
