package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
)

const (
	sqlMetadataStoreType = "SqlMetadataStore"
)

type MetadataStoreInstantiator = internalConfig.DynamicJsonInstantiator[metadatastore.MetadataStore]

type SqlMetadataStoreConfiguration struct {
	DatabaseInstantiator databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase          json.RawMessage                     `json:"db"`
	internalConfig.DynamicJsonType
}

func (s *SqlMetadataStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sqlMetadataStoreConfiguration SqlMetadataStoreConfiguration
	err := json.Unmarshal(b, (*sqlMetadataStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(s.RawDatabase)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlMetadataStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlMetadataStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (metadatastore.MetadataStore, error) {
	db, err := s.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		return nil, err
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		return nil, err
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		return nil, err
	}
	return sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
}

func CreateMetadataStoreInstantiatorFromJson(b []byte) (MetadataStoreInstantiator, error) {
	var mc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &mc)
	if err != nil {
		return nil, err
	}

	var mi MetadataStoreInstantiator
	switch mc.Type {
	case sqlMetadataStoreType:
		mi = &SqlMetadataStoreConfiguration{}
	default:
		return nil, errors.New("unknown metadataStore type")
	}
	err = json.Unmarshal(b, &mi)
	if err != nil {
		return nil, err
	}
	return mi, nil
}
