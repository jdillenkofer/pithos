package config

import (
	"database/sql"
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blob"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/middlewares/tracing"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/sql"
)

const (
	TracingMetadataStoreMiddlewareType = "TracingMetadataStoreMiddleware"
	SqlMetadataStoreType               = "SqlMetadataStore"
)

type MetadataStoreInstantiator = internalConfig.DynamicJsonInstantiator[metadatastore.MetadataStore]

type TracingMetadataStoreMiddlewareConfiguration struct {
	RegionName                     string                    `json:"regionName"`
	InnerMetadataStoreInstantiator MetadataStoreInstantiator `json:"-"`
	RawInnerMetadataStore          json.RawMessage           `json:"innerMetadataStore"`
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

func (t *TracingMetadataStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (metadatastore.MetadataStore, error) {
	innerMetadataStore, err := t.InnerMetadataStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName, innerMetadataStore)
}

type SqlMetadataStoreConfiguration struct {
	internalConfig.DynamicJsonType
}

func (s *SqlMetadataStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (metadatastore.MetadataStore, error) {
	// @TODO: use real db
	var db *sql.DB = nil
	// @TODO: use real repository
	var bucketRepository bucket.Repository = nil
	/*
		bucketRepository, err := sqliteBucket.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
	// @TODO: use real repository
	var objectRepository object.Repository = nil
	/*
		objectRepository, err := sqliteObject.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
	// @TODO: use real repository
	var blobRepository blob.Repository = nil
	/*
		blobRepository, err := sqliteBlob.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
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
	case TracingMetadataStoreMiddlewareType:
		mi = &TracingMetadataStoreMiddlewareConfiguration{}
	case SqlMetadataStoreType:
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
