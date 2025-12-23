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
	"crypto/sha512"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	auditMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/audit"
)

const (
	cacheStorageType                 = "CacheStorage"
	metadataPartStorageType          = "MetadataPartStorage"
	conditionalStorageMiddlewareType = "ConditionalStorageMiddleware"
	prometheusStorageMiddlewareType  = "PrometheusStorageMiddleware"
	auditStorageMiddlewareType       = "AuditStorageMiddleware"
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

type MetadataPartStorageConfiguration struct {
	DatabaseInstantiator      databaseConfig.DatabaseInstantiator           `json:"-"`
	RawDatabase               json.RawMessage                               `json:"db"`
	MetadataStoreInstantiator metadataStoreConfig.MetadataStoreInstantiator `json:"-"`
	RawMetadataStore          json.RawMessage                               `json:"metadataStore"`
	PartStoreInstantiator     partStoreConfig.PartStoreInstantiator         `json:"-"`
	RawPartStore              json.RawMessage                               `json:"partStore"`
	internalConfig.DynamicJsonType
}

func (m *MetadataPartStorageConfiguration) UnmarshalJSON(b []byte) error {
	type metadataPartStorageConfiguration MetadataPartStorageConfiguration
	err := json.Unmarshal(b, (*metadataPartStorageConfiguration)(m))
	if err != nil {
		return err
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

type SinkConfiguration struct {
	Type   string `json:"type"`   // "file"
	Format string `json:"format"` // "bin", "json", "text"
	Path   string `json:"path"`
}

type AuditStorageMiddlewareConfiguration struct {
	InnerStorageInstantiator StorageInstantiator `json:"-"`
	RawInnerStorage          json.RawMessage     `json:"innerStorage"`
	Sinks                    []SinkConfiguration `json:"sinks"`
	Ed25519PrivateKey        string              `json:"ed25519PrivateKey"` // Base64 encoded
	MlDsaPrivateKey          string              `json:"mlDsaPrivateKey"`   // Base64 encoded
	internalConfig.DynamicJsonType
}

func (a *AuditStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type auditStorageMiddlewareConfiguration AuditStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*auditStorageMiddlewareConfiguration)(a))
	if err != nil {
		return err
	}
	a.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(a.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (a *AuditStorageMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return a.InnerStorageInstantiator.RegisterReferences(diCollection)
}

func (a *AuditStorageMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	innerStorage, err := a.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	
	edPriv, err := signing.LoadEd25519PrivateKey(a.Ed25519PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load Ed25519 private key: %w", err)
	}

	mlPriv, err := signing.LoadMlDsaPrivateKey(a.MlDsaPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load ML-DSA private key: %w", err)
	}

	var sinks []sink.Sink
	var lastHash []byte
	var initialHashBuffer [][]byte

	for _, sc := range a.Sinks {
		var s serialization.Serializer
		switch sc.Format {
		case "bin":
			s = &serialization.BinarySerializer{}
		case "json":
			s = &serialization.JsonSerializer{Indent: true}
		case "text":
			s = &serialization.TextSerializer{}
		default:
			return nil, fmt.Errorf("unknown audit log format: %s", sc.Format)
		}

		var curSink sink.Sink
		var curHash []byte
		var curBuffer [][]byte

		switch sc.Type {
		case "file":
			var fs *sink.WriterSink
			fs, curHash, curBuffer, err = sink.NewFileSink(sc.Path, s)
			if err != nil {
				return nil, fmt.Errorf("failed to create file sink at %s: %w", sc.Path, err)
			}
			curSink = fs
		default:
			return nil, fmt.Errorf("unknown audit log sink type: %s", sc.Type)
		}

		sinks = append(sinks, curSink)
		if lastHash == nil && len(curHash) > 0 {
			lastHash = curHash
			initialHashBuffer = curBuffer
		}
	}

	if len(sinks) == 0 {
		return nil, errors.New("at least one audit log sink must be configured")
	}

	var finalSink sink.Sink
	if len(sinks) == 1 {
		finalSink = sinks[0]
	} else {
		finalSink = sink.NewMultiSink(sinks...)
	}

	if lastHash == nil {
		lastHash = make([]byte, sha512.Size)
	}
	
	return auditMiddleware.NewAuditLogMiddleware(innerStorage, finalSink, signing.NewEd25519Signer(edPriv), signing.NewMlDsaSigner(mlPriv), lastHash, initialHashBuffer), nil
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
	case conditionalStorageMiddlewareType:
		si = &ConditionalStorageMiddlewareConfiguration{}
	case prometheusStorageMiddlewareType:
		si = &PrometheusStorageMiddlewareConfiguration{}
	case auditStorageMiddlewareType:
		si = &AuditStorageMiddlewareConfiguration{}
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
