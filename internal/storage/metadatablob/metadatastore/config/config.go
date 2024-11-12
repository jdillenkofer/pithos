package config

import (
	"database/sql"
	"encoding/json"
	"errors"

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

type MetadataStoreInstantiator interface {
	Instantiate() (metadatastore.MetadataStore, error)
}

type MetadataStoreConfiguration struct {
	Type string `json:"type"`
}

type TracingMetadataStoreMiddlewareConfiguration struct {
	RegionName                     string                    `json:"regionName"`
	InnerMetadataStoreInstantiator MetadataStoreInstantiator `json:"-"`
	RawInnerMetadataStore          json.RawMessage           `json:"innerMetadataStore"`
	MetadataStoreConfiguration
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

func (t *TracingMetadataStoreMiddlewareConfiguration) Instantiate() (metadatastore.MetadataStore, error) {
	innerMetadataStore, err := t.InnerMetadataStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName, innerMetadataStore)
}

type SqlMetadataStoreConfiguration struct {
	MetadataStoreConfiguration
}

func (s *SqlMetadataStoreConfiguration) Instantiate() (metadatastore.MetadataStore, error) {
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
	var mc MetadataStoreConfiguration
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
