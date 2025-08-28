package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/middlewares/tracing"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
)

const (
	tracingMetadataStoreMiddlewareType = "TracingMetadataStoreMiddleware"
	sqlMetadataStoreType               = "SqlMetadataStore"
)

type MetadataStoreInstantiator = internalConfig.DynamicJsonInstantiator[metadatastore.MetadataStore]

type TracingMetadataStoreMiddlewareConfiguration struct {
	RegionName                     internalConfig.StringProvider `json:"regionName"`
	InnerMetadataStoreInstantiator MetadataStoreInstantiator     `json:"-"`
	RawInnerMetadataStore          json.RawMessage               `json:"innerMetadataStore"`
	internalConfig.DynamicJsonType
}

func (t *TracingMetadataStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tracingMetadataStoreMiddlewareConfiguration TracingMetadataStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tracingMetadataStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerMetadataStoreInstantiator, err = CreateMetadataStoreInstantiatorFromJson(t.RawInnerMetadataStore)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingMetadataStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := t.InnerMetadataStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingMetadataStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (metadatastore.MetadataStore, error) {
	innerMetadataStore, err := t.InnerMetadataStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName.Value(), innerMetadataStore)
}

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
	blobRepository, err := repositoryFactory.NewBlobRepository(db)
	if err != nil {
		return nil, err
	}
	return sqlMetadataStore.New(db, bucketRepository, objectRepository, blobRepository)
}

func CreateMetadataStoreInstantiatorFromJson(b []byte) (MetadataStoreInstantiator, error) {
	var mc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &mc)
	if err != nil {
		return nil, err
	}

	var mi MetadataStoreInstantiator
	switch mc.Type {
	case tracingMetadataStoreMiddlewareType:
		mi = &TracingMetadataStoreMiddlewareConfiguration{}
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
