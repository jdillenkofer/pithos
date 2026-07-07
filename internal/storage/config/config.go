package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"crypto/sha512"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	signingVault "github.com/jdillenkofer/pithos/internal/auditlog/signing/vault"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	auditlogSinkConfig "github.com/jdillenkofer/pithos/internal/auditlog/sink/config"
	cacheConfig "github.com/jdillenkofer/pithos/internal/cache/config"
	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	metadataStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/config"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	partStoreConfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/config"
	auditMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/audit"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/conditional"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/objectcache"
	prometheusMiddleware "github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultOutboxId                  = "default"
	metadataPartStorageType          = "MetadataPartStorage"
	conditionalStorageMiddlewareType = "ConditionalStorageMiddleware"
	prometheusStorageMiddlewareType  = "PrometheusStorageMiddleware"
	auditStorageMiddlewareType       = "AuditStorageMiddleware"
	outboxStorageType                = "OutboxStorage"
	replicationStorageType           = "ReplicationStorage"
	s3ClientStorageType              = "S3ClientStorage"
	objectCacheStorageMiddlewareType = "ObjectCacheStorageMiddleware"
)

type StorageInstantiator = internalConfig.DynamicJsonInstantiator[storage.Storage]

type MetadataPartStorageConfiguration struct {
	DatabaseInstantiator      databaseConfig.DatabaseInstantiator           `json:"-"`
	RawDatabase               json.RawMessage                               `json:"db"`
	MetadataStoreInstantiator metadataStoreConfig.MetadataStoreInstantiator `json:"-"`
	RawMetadataStore          json.RawMessage                               `json:"metadataStore"`
	// PartStore is the default part store; it backs every storage class
	// without an explicit mapping, so configurations without named stores
	// behave exactly as before.
	PartStoreInstantiator partStoreConfig.PartStoreInstantiator `json:"-"`
	RawPartStore          json.RawMessage                       `json:"partStore"`
	// ExtraPartStores defines additional part stores by name. The name
	// "default" is reserved for the partStore field.
	ExtraPartStoreInstantiators map[string]partStoreConfig.PartStoreInstantiator `json:"-"`
	RawExtraPartStores          map[string]json.RawMessage                       `json:"extraPartStores,omitempty"`
	// StorageClassToPartStore maps a storage class to the name of the part
	// store its object data is written to ("default" is a valid target).
	// Unmapped classes use the default part store.
	StorageClassToPartStore map[string]string `json:"storageClassToPartStore,omitempty"`
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
	m.ExtraPartStoreInstantiators = map[string]partStoreConfig.PartStoreInstantiator{}
	for name, rawPartStore := range m.RawExtraPartStores {
		if name == partstore.DefaultPartStoreName {
			return fmt.Errorf("extraPartStores must not use the reserved name %q (configure it via the partStore field)", partstore.DefaultPartStoreName)
		}
		partStoreInstantiator, err := partStoreConfig.CreatePartStoreInstantiatorFromJson(rawPartStore)
		if err != nil {
			return err
		}
		m.ExtraPartStoreInstantiators[name] = partStoreInstantiator
	}
	for storageClass, name := range m.StorageClassToPartStore {
		if !storage.IsValidStorageClass(storageClass) {
			return fmt.Errorf("storageClassToPartStore key %q is not a recognized storage class", storageClass)
		}
		if name != partstore.DefaultPartStoreName {
			if _, ok := m.RawExtraPartStores[name]; !ok {
				return fmt.Errorf("storageClassToPartStore maps %q to unknown part store %q", storageClass, name)
			}
		}
	}
	if err := m.validateSqlPartStoreIds(); err != nil {
		return err
	}
	return nil
}

type sqlPartStoreConfig struct {
	name        string
	dbIdentity  string
	partStoreId string
}

func (m *MetadataPartStorageConfiguration) validateSqlPartStoreIds() error {
	sqlStores := collectSqlPartStoreConfigs(partstore.DefaultPartStoreName, m.PartStoreInstantiator)
	for name, instantiator := range m.ExtraPartStoreInstantiators {
		sqlStores = append(sqlStores, collectSqlPartStoreConfigs(name, instantiator)...)
	}

	byDb := map[string][]sqlPartStoreConfig{}
	for _, sqlStore := range sqlStores {
		byDb[sqlStore.dbIdentity] = append(byDb[sqlStore.dbIdentity], sqlStore)
	}
	for dbIdentity, stores := range byDb {
		if len(stores) < 2 {
			continue
		}
		seenIds := map[string]string{}
		for _, store := range stores {
			if store.partStoreId == "" {
				return fmt.Errorf("multiple SqlPartStore configurations share database %q; part store %q must set partStoreId", dbIdentity, store.name)
			}
			if existingName, ok := seenIds[store.partStoreId]; ok {
				return fmt.Errorf("multiple SqlPartStore configurations share database %q and partStoreId %q (%q and %q)", dbIdentity, store.partStoreId, existingName, store.name)
			}
			seenIds[store.partStoreId] = store.name
		}
	}
	return nil
}

func collectSqlPartStoreConfigs(name string, instantiator partStoreConfig.PartStoreInstantiator) []sqlPartStoreConfig {
	switch i := instantiator.(type) {
	case *partStoreConfig.SqlPartStoreConfiguration:
		return []sqlPartStoreConfig{{
			name:        name,
			dbIdentity:  databaseInstantiatorIdentity(i.DatabaseInstantiator),
			partStoreId: i.PartStoreId.Value(),
		}}
	case *partStoreConfig.CompressionPartStoreMiddlewareConfiguration:
		return collectSqlPartStoreConfigs(name, i.InnerPartStoreInstantiator)
	case *partStoreConfig.TinkEncryptionPartStoreMiddlewareConfiguration:
		return collectSqlPartStoreConfigs(name, i.InnerPartStoreInstantiator)
	case *partStoreConfig.OutboxPartStoreConfiguration:
		return collectSqlPartStoreConfigs(name, i.InnerPartStoreInstantiator)
	case *partStoreConfig.CachePartStoreConfiguration:
		return collectSqlPartStoreConfigs(name, i.InnerPartStoreInstantiator)
	case *partStoreConfig.ErasureCodedPartStoreMiddlewareConfiguration:
		var sqlStores []sqlPartStoreConfig
		for idx, child := range i.PartStoreInstantiators {
			sqlStores = append(sqlStores, collectSqlPartStoreConfigs(fmt.Sprintf("%s[%d]", name, idx), child)...)
		}
		return sqlStores
	default:
		return nil
	}
}

func databaseInstantiatorIdentity(instantiator databaseConfig.DatabaseInstantiator) string {
	switch i := instantiator.(type) {
	case *databaseConfig.SqliteDatabaseConfiguration:
		return "sqlite:" + i.DbPath.Value()
	case *databaseConfig.PostgresDatabaseConfiguration:
		return "postgres:" + i.DbUrl.Value()
	case *databaseConfig.DatabaseReferenceConfiguration:
		return "ref:" + i.RefName.Value()
	case *databaseConfig.RegisterDatabaseReferenceConfiguration:
		return "ref:" + i.RefName.Value()
	default:
		return fmt.Sprintf("%T", instantiator)
	}
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
	for _, partStoreInstantiator := range m.ExtraPartStoreInstantiators {
		err = partStoreInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
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
	extraPartStores := map[string]partstore.PartStore{}
	for name, partStoreInstantiator := range m.ExtraPartStoreInstantiators {
		extraPartStore, err := partStoreInstantiator.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}
		extraPartStores[name] = extraPartStore
	}
	return metadatapart.NewStorageWithNamedPartStores(db, metadataStore, partStore, extraPartStores, m.StorageClassToPartStore)
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

type VaultSigningConfiguration struct {
	Address  string `json:"address"`
	Token    string `json:"token,omitempty"`
	RoleID   string `json:"roleId,omitempty"`
	SecretID string `json:"secretId,omitempty"`
	KeyPath  string `json:"keyPath"`
}

type Ed25519SigningConfiguration struct {
	PrivateKey string                     `json:"privateKey,omitempty"` // File path or base64 encoded
	Vault      *VaultSigningConfiguration `json:"vault,omitempty"`
}

type MlDsa87SigningConfiguration struct {
	PrivateKey string `json:"privateKey"` // File path or base64 encoded
}

type SigningConfiguration struct {
	Ed25519 *Ed25519SigningConfiguration `json:"ed25519"`
	MlDsa87 *MlDsa87SigningConfiguration `json:"mlDsa87"`
}

type AuditStorageMiddlewareConfiguration struct {
	InnerStorageInstantiator StorageInstantiator                   `json:"-"`
	RawInnerStorage          json.RawMessage                       `json:"innerStorage"`
	SinkInstantiators        []auditlogSinkConfig.SinkInstantiator `json:"-"`
	RawSinks                 []json.RawMessage                     `json:"sinks"`
	Signing                  *SigningConfiguration                 `json:"signing"`

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
	for _, rawSink := range a.RawSinks {
		sinkInstantiator, err := auditlogSinkConfig.CreateSinkInstantiatorFromJson(rawSink)
		if err != nil {
			return err
		}
		a.SinkInstantiators = append(a.SinkInstantiators, sinkInstantiator)
	}
	return nil
}

func (a *AuditStorageMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := a.InnerStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	for _, sinkInstantiator := range a.SinkInstantiators {
		err = sinkInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *AuditStorageMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	innerStorage, err := a.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}

	if a.Signing == nil {
		return nil, errors.New("signing configuration is required")
	}

	// Ed25519 Signer
	var edSigner signing.Signer
	if a.Signing.Ed25519 == nil {
		return nil, errors.New("signing.ed25519 configuration is required")
	}

	if a.Signing.Ed25519.Vault != nil {
		// Use Vault Signer
		vc := a.Signing.Ed25519.Vault
		if vc.Address == "" {
			return nil, errors.New("signing.ed25519.vault.address is required")
		}
		if vc.KeyPath == "" {
			return nil, errors.New("signing.ed25519.vault.keyPath is required")
		}
		edSigner, err = signingVault.NewSigner(vc.Address, vc.Token, vc.RoleID, vc.SecretID, vc.KeyPath, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Vault signer: %w", err)
		}
	} else {
		// Use Local Ed25519 Key
		if a.Signing.Ed25519.PrivateKey == "" {
			return nil, errors.New("signing.ed25519.privateKey is required if vault is not configured")
		}
		edPriv, err := signing.LoadEd25519PrivateKey(a.Signing.Ed25519.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load Ed25519 private key: %w", err)
		}
		edSigner = signing.NewEd25519Signer(edPriv)
	}

	// ML-DSA Signer
	if a.Signing.MlDsa87 == nil {
		return nil, errors.New("signing.mlDsa87 configuration is required")
	}
	if a.Signing.MlDsa87.PrivateKey == "" {
		return nil, errors.New("signing.mlDsa87.privateKey is required")
	}
	mlPriv, err := signing.LoadMlDsa87PrivateKey(a.Signing.MlDsa87.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load ML-DSA-87 private key: %w", err)
	}

	var sinks []sink.Sink
	var lastHash []byte
	var initialHashBuffer [][]byte

	for _, si := range a.SinkInstantiators {
		curSink, err := si.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}

		sinks = append(sinks, curSink)

		if lastHash == nil {
			if isp, ok := curSink.(sink.InitialStateProvider); ok {
				state, err := isp.InitialState()
				if err != nil {
					return nil, fmt.Errorf("failed to get initial state from sink: %w", err)
				}
				if state != nil && len(state.LastHash) > 0 {
					lastHash = state.LastHash
					initialHashBuffer = state.HashBuffer
				}
			}
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

	return auditMiddleware.NewAuditLogMiddleware(innerStorage, finalSink, edSigner, signing.NewMlDsa87Signer(mlPriv), lastHash, initialHashBuffer), nil
}

type OutboxStorageConfiguration struct {
	DatabaseInstantiator     databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase              json.RawMessage                     `json:"db"`
	InnerStorageInstantiator StorageInstantiator                 `json:"-"`
	RawInnerStorage          json.RawMessage                     `json:"innerStorage"`
	OutboxId                 internalConfig.StringProvider       `json:"outboxId,omitempty"`
	ClaimLeaseDurationSecs   *internalConfig.Int64Provider       `json:"claimLeaseDurationSeconds,omitempty"`
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
	outboxId := o.OutboxId.Value()
	if outboxId == "" {
		outboxId = defaultOutboxId
	}
	claimLeaseDuration := 30 * time.Second
	if o.ClaimLeaseDurationSecs != nil {
		claimLeaseDurationSecs := o.ClaimLeaseDurationSecs.Value()
		if claimLeaseDurationSecs <= 0 {
			return nil, errors.New("claimLeaseDurationSeconds must be > 0")
		}
		claimLeaseDuration = time.Duration(claimLeaseDurationSecs) * time.Second
	}
	storageOutboxEntryRepository, err := repositoryFactory.NewStorageOutboxEntryRepository(db)
	if err != nil {
		return nil, err
	}
	t := reflect.TypeOf((*prometheus.Registerer)(nil))
	prometheusRegisterer, err := diProvider.LookupByType(t)
	if err != nil {
		return nil, err
	}
	return outbox.NewStorage(db, outboxId, innerStorage, storageOutboxEntryRepository, prometheusRegisterer.(prometheus.Registerer), claimLeaseDuration)
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

type ObjectCacheStorageMiddlewareConfiguration struct {
	CacheInstantiator        cacheConfig.CacheInstantiator `json:"-"`
	RawCache                 json.RawMessage               `json:"cache"`
	MaxObjectSizeBytes       internalConfig.Int64Provider  `json:"maxObjectSizeBytes"`
	CacheReadErrorsAsMiss    internalConfig.BoolProvider   `json:"cacheReadErrorsAsMiss"`
	InnerStorageInstantiator StorageInstantiator           `json:"-"`
	RawInnerStorage          json.RawMessage               `json:"innerStorage"`
	internalConfig.DynamicJsonType
}

func (s *ObjectCacheStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type objectCacheStorageMiddlewareConfiguration ObjectCacheStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*objectCacheStorageMiddlewareConfiguration)(s))
	if err != nil {
		return err
	}
	s.CacheInstantiator, err = cacheConfig.CreateCacheInstantiatorFromJson(s.RawCache)
	if err != nil {
		return err
	}
	s.InnerStorageInstantiator, err = CreateStorageInstantiatorFromJson(s.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (s *ObjectCacheStorageMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.CacheInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = s.InnerStorageInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *ObjectCacheStorageMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (storage.Storage, error) {
	cacheImpl, err := s.CacheInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	innerStorage, err := s.InnerStorageInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return objectcache.NewStorageMiddleware(innerStorage, cacheImpl, objectcache.Options{
		MaxObjectSizeBytes:    s.MaxObjectSizeBytes.Value(),
		CacheReadErrorsAsMiss: s.CacheReadErrorsAsMiss.Value(),
	})
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
	case objectCacheStorageMiddlewareType:
		si = &ObjectCacheStorageMiddlewareConfiguration{}
	default:
		return nil, errors.New("unknown storage type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}
