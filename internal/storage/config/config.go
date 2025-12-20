package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	cacheConfig "github.com/jdillenkofer/pithos/internal/storage/cache/config"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	metadataStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/config"
	partStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/config"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/conditional"
	prometheusMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	cacheStorageType        = "CacheStorage"
	metadataPartStorageType = "MetadataPartStorage"
	// Deprecated: Use MetadataPartStorage instead. Will be removed in a future version.
	metadataBlobStorageType          = "MetadataBlobStorage"
	conditionalStorageMiddlewareType = "ConditionalStorageMiddleware"
	prometheusStorageMiddlewareType  = "PrometheusStorageMiddleware"
	outboxStorageType                = "OutboxStorage"
	replicationStorageType           = "ReplicationStorage"
	s3ClientStorageType              = "S3ClientStorage"
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

func (c *CacheStorageConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := c.CacheInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = c.InnerStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (c *CacheStorageConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	cacheImpl, err := c.CacheInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	innerStorage, err := c.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return cache.New(cacheImpl, innerStorage)
}

// Deprecated: Use MetadataPartStorageConfiguration instead. Will be removed in a future version.
type MetadataBlobStorageConfiguration struct {
	MetadataPartStorageConfiguration
}

func (m *MetadataBlobStorageConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("MetadataBlobStorage is deprecated. Please use MetadataPartStorage instead.")
	// Map old field names to new ones if necessary, but since we use json.RawMessage and custom parsing in MetadataPartStorageConfiguration,
	// we need to check if the json uses "blobStore" or "partStore".
	// However, MetadataPartStorageConfiguration.UnmarshalJSON expects "partStore".
	// So we need a struct that accepts "blobStore" and puts it into "RawPartStore".

	type metadataBlobStorageConfigurationAlias struct {
		RawDatabase      json.RawMessage `json:"db"`
		RawMetadataStore json.RawMessage `json:"metadataStore"`
		RawPartStore     json.RawMessage `json:"partStore"`
		RawBlobStore     json.RawMessage `json:"blobStore"`
		internalConfig.DynamicJsonType
	}
	var alias metadataBlobStorageConfigurationAlias
	err := json.Unmarshal(b, &alias)
	if err != nil {
		return err
	}

	m.RawDatabase = alias.RawDatabase
	m.RawMetadataStore = alias.RawMetadataStore
	m.Type = alias.Type

	if alias.RawBlobStore != nil {
		m.RawPartStore = alias.RawBlobStore
	} else {
		m.RawPartStore = alias.RawPartStore
	}

	m.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(m.RawDatabase)
	if err != nil {
		return err
	}
	m.MetadataStoreInstantiator, err = metadataStoreConfig.CreateMetadataStoreInstantiatorFromJson(m.RawMetadataStore)
	if err != nil {
		return err
	}
	m.PartStoreInstantiator, err = partStoreConfig.CreatePartStoreInstantiatorFromJson(m.RawPartStore)
	if err != nil {
		return err
	}
	return nil
}

type MetadataPartStorageConfiguration struct {
	DatabaseInstantiator      databaseConfig.DatabaseInstantiator           `json:"-"`
	RawDatabase               json.RawMessage                               `json:"db"`
	MetadataStoreInstantiator metadataStoreConfig.MetadataStoreInstantiator `json:"-"`
	RawMetadataStore          json.RawMessage                               `json:"metadataStore"`
	PartStoreInstantiator     partStoreConfig.PartStoreInstantiator         `json:"-"`
	RawPartStore              json.RawMessage                               `json:"partStore"`
	// For backward compatibility
	RawBlobStore json.RawMessage `json:"blobStore"`
	internalConfig.DynamicJsonType
}

func (m *MetadataPartStorageConfiguration) UnmarshalJSON(b []byte) error {
	type metadataPartStorageConfiguration MetadataPartStorageConfiguration
	err := json.Unmarshal(b, (*metadataPartStorageConfiguration)(m))
	if err != nil {
		return err
	}

	if m.RawBlobStore != nil {
		slog.Warn("blobStore field is deprecated. Please use partStore instead.")
		if m.RawPartStore == nil {
			m.RawPartStore = m.RawBlobStore
		}
	}

	m.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(m.RawDatabase)
	if err != nil {
		return err
	}
	m.MetadataStoreInstantiator, err = metadataStoreConfig.CreateMetadataStoreInstantiatorFromJson(m.RawMetadataStore)
	if err != nil {
		return err
	}
	m.PartStoreInstantiator, err = partStoreConfig.CreatePartStoreInstantiatorFromJson(m.RawPartStore)
	if err != nil {
		return err
	}
	return nil
}

func (m *MetadataPartStorageConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := m.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = m.MetadataStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = m.PartStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (m *MetadataPartStorageConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	db, err := m.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	metadataStore, err := m.MetadataStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	partStore, err := m.PartStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return metadatapart.NewStorage(db, metadataStore, partStore)
}

type ConditionalStorageMiddlewareConfiguration struct {
	BucketToStorageInstantiatorMap map[string]StorageInstantiator `json:"-"`
	RawBucketToStorageMap          map[string]json.RawMessage     `json:"bucketToStorageMap"`
	DefaultStorageInstantiator     StorageInstantiator            `json:"-"`
	RawDefaultStorage              json.RawMessage                `json:"defaultStorage"`
	internalConfig.DynamicJsonType
}

func (c *ConditionalStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type conditionalStorageMiddlewareConfiguration ConditionalStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*conditionalStorageMiddlewareConfiguration)(c))
	if err != nil {
		return err
	}
	c.BucketToStorageInstantiatorMap = map[string]StorageInstantiator{}
	for bucket, rawStorage := range c.RawBucketToStorageMap {
		storageInstantiator, err := CreateStorageInstantiatorFromJson(rawStorage)
		if err != nil {
			return err
		}
		c.BucketToStorageInstantiatorMap[bucket] = storageInstantiator
	}

	c.DefaultStorageInstantiator, err = CreateStorageInstantiatorFromJson(c.RawDefaultStorage)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConditionalStorageMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	for _, storageInstantiator := range c.BucketToStorageInstantiatorMap {
		err := storageInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
	}
	err := c.DefaultStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConditionalStorageMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	bucketToStorageMap := map[string]storage.Storage{}
	for bucket, storageInstantiator := range c.BucketToStorageInstantiatorMap {
		storage, err := storageInstantiator.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}
		bucketToStorageMap[bucket] = storage
	}
	defaultStorage, err := c.DefaultStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return conditional.NewStorageMiddleware(bucketToStorageMap, defaultStorage)
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

func (p *PrometheusStorageMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := p.InnerStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (p *PrometheusStorageMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	innerStorage, err := p.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	t := reflect.TypeOf((*prometheus.Registerer)(nil))
	prometheusRegisterer, err := diProvider.LookupByType(t)
	if err != nil {
		return nil, err
	}
	return prometheusMiddleware.NewStorageMiddleware(innerStorage, prometheusRegisterer.(prometheus.Registerer))
}

type OutboxStorageConfiguration struct {
	DatabaseInstantiator     databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase              json.RawMessage                     `json:"db"`
	InnerStorageInstantiator StorageInstantiator                 `json:"-"`
	RawInnerStorage          json.RawMessage                     `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (o *OutboxStorageConfiguration) UnmarshalJSON(b []byte) error {
	type outboxStorageConfiguration OutboxStorageConfiguration
	err := json.Unmarshal(b, (*outboxStorageConfiguration)(o))
	if err != nil {
		return err
	}
	o.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(o.RawDatabase)
	if err != nil {
		return err
	}
	o.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(o.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxStorageConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := o.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = o.InnerStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxStorageConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	db, err := o.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	innerStorage, err := o.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db)
	if err != nil {
		return nil, err
	}
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

func (r *ReplicationStorageConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := r.PrimaryStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	for _, secondaryStorageInstantiator := range r.SecondaryStorageInstantiators {
		err = secondaryStorageInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ReplicationStorageConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	primaryStorage, err := r.PrimaryStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	secondaryStorages := []storage.Storage{}
	for _, secondaryStorageInstantiator := range r.SecondaryStorageInstantiators {
		secondaryStorage, err := secondaryStorageInstantiator.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}
		secondaryStorages = append(secondaryStorages, secondaryStorage)
	}
	return replication.NewStorage(primaryStorage, secondaryStorages...)
}

type S3ClientStorageConfiguration struct {
	BaseEndpoint    internalConfig.StringProvider `json:"baseEndpoint"`
	Region          internalConfig.StringProvider `json:"region"`
	AccessKeyId     internalConfig.StringProvider `json:"accessKeyId"`
	SecretAccessKey internalConfig.StringProvider `json:"secretAccessKey"`
	UsePathStyle    bool                          `json:"usePathStyle"`
	internalConfig.DynamicJsonType
}

func (s *S3ClientStorageConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *S3ClientStorageConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s.Region.Value()),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.AccessKeyId.Value(), s.SecretAccessKey.Value(), "")),
	)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not loadDefaultConfig: %s", err))
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = s.UsePathStyle
		o.BaseEndpoint = aws.String(s.BaseEndpoint.Value())
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
	case cacheStorageType:
		si = &CacheStorageConfiguration{}
	case metadataPartStorageType:
		si = &MetadataPartStorageConfiguration{}
	case metadataBlobStorageType:
		si = &MetadataBlobStorageConfiguration{}
	case conditionalStorageMiddlewareType:
		si = &ConditionalStorageMiddlewareConfiguration{}
	case prometheusStorageMiddlewareType:
		si = &PrometheusStorageMiddlewareConfiguration{}
	case outboxStorageType:
		si = &OutboxStorageConfiguration{}
	case replicationStorageType:
		si = &ReplicationStorageConfiguration{}
	case s3ClientStorageType:
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
