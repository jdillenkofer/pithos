package config

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	cacheConfig "github.com/jdillenkofer/pithos/internal/storage/cache/config"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	blobStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/config"
	metadataStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore/config"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/tracing"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
)

const (
	CacheStorageType                = "CacheStorage"
	MetadataBlobStorageType         = "MetadataBlobStorage"
	PrometheusStorageMiddlewareType = "PrometheusStorageMiddleware"
	TracingStorageMiddlewareType    = "TracingStorageMiddleware"
	OutboxStorageType               = "OutboxStorage"
	ReplicationStorageType          = "ReplicationStorage"
	S3ClientStorageType             = "S3ClientStorage"
)

type StorageInstantiator = internalConfig.DynamicJsonInstantiator[storage.Storage]

type CacheStorageConfiguration struct {
	CacheInstantiator        cacheConfig.CacheInstantiator `json:"-"`
	RawCache                 json.RawMessage               `json:"cache"`
	InnerStorageInstantiator StorageInstantiator           `json:"-"`
	RawInnerStorage          json.RawMessage               `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (c *CacheStorageConfiguration) UnmarshalJSON(b []byte) error {
	type cacheStorageConfiguration CacheStorageConfiguration
	err := json.Unmarshal(b, (*cacheStorageConfiguration)(c))
	if err != nil {
		return err
	}
	c.CacheInstantiator, err = cacheConfig.CreateCacheInstantiatorFromJson(c.RawCache)
	if err != nil {
		return err
	}
	c.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(c.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (c *CacheStorageConfiguration) Instantiate() (storage.Storage, error) {
	cacheImpl, err := c.CacheInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	innerStorage, err := c.InnerStorageInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return cache.New(cacheImpl, innerStorage)
}

type MetadataBlobStorageConfiguration struct {
	MetadataStoreInstantiator metadataStoreConfig.MetadataStoreInstantiator `json:"-"`
	RawMetadataStore          json.RawMessage                               `json:"metadataStore"`
	BlobStoreInstantiator     blobStoreConfig.BlobStoreInstantiator         `json:"-"`
	RawBlobStore              json.RawMessage                               `json:"blobStore"`
	internalConfig.DynamicJsonType
}

func (m *MetadataBlobStorageConfiguration) UnmarshalJSON(b []byte) error {
	type metadataBlobStorageConfiguration MetadataBlobStorageConfiguration
	err := json.Unmarshal(b, (*metadataBlobStorageConfiguration)(m))
	if err != nil {
		return err
	}
	m.MetadataStoreInstantiator, err = metadataStoreConfig.CreateMetadataStoreInstantiatorFromJson(m.RawMetadataStore)
	if err != nil {
		return err
	}
	m.BlobStoreInstantiator, err = blobStoreConfig.CreateBlobStoreInstantiatorFromJson(m.RawBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (m *MetadataBlobStorageConfiguration) Instantiate() (storage.Storage, error) {
	// @TODO: use real db
	var db *sql.DB = nil
	metadataStore, err := m.MetadataStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	blobStore, err := m.BlobStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return metadatablob.NewStorage(db, metadataStore, blobStore)
}

type PrometheusStorageMiddlewareConfiguration struct {
	InnerStorageInstantiator StorageInstantiator `json:"-"`
	RawInnerStorage          json.RawMessage     `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (p *PrometheusStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type prometheusStorageMiddlewareConfiguration PrometheusStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*prometheusStorageMiddlewareConfiguration)(p))
	if err != nil {
		return err
	}
	p.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(p.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (p *PrometheusStorageMiddlewareConfiguration) Instantiate() (storage.Storage, error) {
	innerStorage, err := p.InnerStorageInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	// @TODO: prometheus registerer
	return prometheus.NewStorageMiddleware(innerStorage, nil)
}

type TracingStorageMiddlewareConfiguration struct {
	RegionName               string              `json:"regionName"`
	InnerStorageInstantiator StorageInstantiator `json:"-"`
	RawInnerStorage          json.RawMessage     `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (t *TracingStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tracingStorageMiddlewareConfiguration TracingStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*tracingStorageMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(t.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingStorageMiddlewareConfiguration) Instantiate() (storage.Storage, error) {
	innerStorage, err := t.InnerStorageInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return tracing.NewStorageMiddleware(t.RegionName, innerStorage)
}

type OutboxStorageConfiguration struct {
	InnerStorageInstantiator StorageInstantiator `json:"-"`
	RawInnerStorage          json.RawMessage     `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (o *OutboxStorageConfiguration) UnmarshalJSON(b []byte) error {
	type outboxStorageConfiguration OutboxStorageConfiguration
	err := json.Unmarshal(b, (*outboxStorageConfiguration)(o))
	if err != nil {
		return err
	}
	o.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(o.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxStorageConfiguration) Instantiate() (storage.Storage, error) {
	// @TODO: use real db
	var db *sql.DB = nil
	innerStorage, err := o.InnerStorageInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	// @TODO: use real repository
	var storageOutboxEntryRepository storageoutboxentry.Repository = nil
	/*
		storageOutboxEntryRepository, err := sqliteStorageOutboxEntry.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
	return outbox.NewStorage(db, innerStorage, storageOutboxEntryRepository)
}

type ReplicationStorageConfiguration struct {
	PrimaryStorageInstantiator    StorageInstantiator   `json:"-"`
	RawPrimaryStorage             json.RawMessage       `json:"primaryStorage"`
	SecondaryStorageInstantiators []StorageInstantiator `json:"-"`
	RawSecondaryStorages          []json.RawMessage     `json:"secondaryStorages"`
	internalConfig.DynamicJsonType
}

func (r *ReplicationStorageConfiguration) UnmarshalJSON(b []byte) error {
	type replicationStorageConfiguration ReplicationStorageConfiguration
	err := json.Unmarshal(b, (*replicationStorageConfiguration)(r))
	if err != nil {
		return err
	}
	r.PrimaryStorageInstantiator, err = CreateStorageInstantiatorFromJson(r.RawPrimaryStorage)
	if err != nil {
		return err
	}
	for _, rawSecondaryStorage := range r.RawSecondaryStorages {
		secondaryStorageInstantiator, err := CreateStorageInstantiatorFromJson(rawSecondaryStorage)
		if err != nil {
			return err
		}
		r.SecondaryStorageInstantiators = append(r.SecondaryStorageInstantiators, secondaryStorageInstantiator)
	}
	return nil
}

func (r *ReplicationStorageConfiguration) Instantiate() (storage.Storage, error) {
	primaryStorage, err := r.PrimaryStorageInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	secondaryStorages := []storage.Storage{}
	for _, secondaryStorageInstantiator := range r.SecondaryStorageInstantiators {
		secondaryStorage, err := secondaryStorageInstantiator.Instantiate()
		if err != nil {
			return nil, err
		}
		secondaryStorages = append(secondaryStorages, secondaryStorage)
	}
	return replication.NewStorage(primaryStorage, secondaryStorages...)
}

type S3ClientStorageConfiguration struct {
	BaseEndpoint    string `json:"baseEndpoint"`
	Region          string `json:"region"`
	AccessKeyId     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	UsePathStyle    bool   `json:"usePathStyle"`
	internalConfig.DynamicJsonType
}

func (s *S3ClientStorageConfiguration) Instantiate() (storage.Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.AccessKeyId, s.SecretAccessKey, "")),
	)
	if err != nil {
		log.Fatalf("Could not loadDefaultConfig: %s", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = s.UsePathStyle
		o.BaseEndpoint = aws.String(s.BaseEndpoint)
	})
	return s3client.NewStorage(s3Client)
}

func CreateStorageInstantiatorFromJson(b []byte) (StorageInstantiator, error) {
	var sc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &sc)
	if err != nil {
		return nil, err
	}

	var si StorageInstantiator
	switch sc.Type {
	case CacheStorageType:
		si = &CacheStorageConfiguration{}
	case MetadataBlobStorageType:
		si = &MetadataBlobStorageConfiguration{}
	case PrometheusStorageMiddlewareType:
		si = &PrometheusStorageMiddlewareConfiguration{}
	case TracingStorageMiddlewareType:
		si = &TracingStorageMiddlewareConfiguration{}
	case OutboxStorageType:
		si = &OutboxStorageConfiguration{}
	case ReplicationStorageType:
		si = &ReplicationStorageConfiguration{}
	case S3ClientStorageType:
		si = &S3ClientStorageConfiguration{}
	default:
		return nil, errors.New("unknown storage type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}
