package config

import (
	"encoding/json"
	"errors"
	"fmt"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	legacyEncryptionBlobStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/legacy"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/tink"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/tracing"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sftp"
	sftpConfig "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sftp/config"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
)

const (
	filesystemBlobStoreType = "FilesystemBlobStore"
	// @DEPRECATED: This will be removed in a future release.
	encryptionBlobStoreMiddlewareType     = "EncryptionBlobStoreMiddleware"
	tinkEncryptionBlobStoreMiddlewareType = "TinkEncryptionBlobStoreMiddleware"
	tracingBlobStoreMiddlewareType        = "TracingBlobStoreMiddleware"
	outboxBlobStoreType                   = "OutboxBlobStore"
	sftpBlobStoreType                     = "SftpBlobStore"
	sqlBlobStoreType                      = "SqlBlobStore"
)

type BlobStoreInstantiator = internalConfig.DynamicJsonInstantiator[blobstore.BlobStore]

type FilesystemBlobStoreConfiguration struct {
	Root internalConfig.StringProvider `json:"root"`
	internalConfig.DynamicJsonType
}

func (f *FilesystemBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FilesystemBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	return filesystem.New(f.Root.Value())
}

type EncryptionBlobStoreMiddlewareConfiguration struct {
	Password                   internalConfig.StringProvider `json:"password"`
	InnerBlobStoreInstantiator BlobStoreInstantiator         `json:"-"`
	RawInnerBlobStore          json.RawMessage               `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type encryptionBlobStoreMiddlewareConfiguration EncryptionBlobStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*encryptionBlobStoreMiddlewareConfiguration)(e))
	if err != nil {
		return err
	}
	e.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(e.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := e.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	innerBlobStore, err := e.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return legacyEncryptionBlobStoreMiddleware.New(e.Password.Value(), innerBlobStore)
}

type TinkEncryptionBlobStoreMiddlewareConfiguration struct {
	KMSType internalConfig.StringProvider `json:"kmsType"`          // "aws", "vault", "local"
	KeyURI  internalConfig.StringProvider `json:"keyURI,omitempty"` // Not used for local KMS
	// AWS KMS specific
	AWSRegion internalConfig.StringProvider `json:"awsRegion,omitempty"`
	// Vault specific
	VaultAddress internalConfig.StringProvider `json:"vaultAddress,omitempty"`
	VaultToken   internalConfig.StringProvider `json:"vaultToken,omitempty"`
	// Local KMS specific (password for key derivation)
	Password                   internalConfig.StringProvider `json:"password,omitempty"`
	InnerBlobStoreInstantiator BlobStoreInstantiator         `json:"-"`
	RawInnerBlobStore          json.RawMessage               `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (t *TinkEncryptionBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tinkEncryptionBlobStoreMiddlewareConfiguration TinkEncryptionBlobStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tinkEncryptionBlobStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(t.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (t *TinkEncryptionBlobStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := t.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (t *TinkEncryptionBlobStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	innerBlobStore, err := t.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}

	kmsType := t.KMSType.Value()

	switch kmsType {
	case "aws":
		keyURI := t.KeyURI.Value()
		region := t.AWSRegion.Value()
		if keyURI == "" {
			return nil, errors.New("keyURI is required for AWS KMS")
		}
		if region == "" {
			return nil, errors.New("awsRegion is required for AWS KMS")
		}
		return tink.NewWithAWSKMS(keyURI, region, innerBlobStore)
	case "vault":
		keyURI := t.KeyURI.Value()
		address := t.VaultAddress.Value()
		token := t.VaultToken.Value()
		if keyURI == "" {
			return nil, errors.New("keyURI is required for Vault KMS")
		}
		if address == "" || token == "" {
			return nil, errors.New("vaultAddress and vaultToken are required for Vault KMS")
		}
		return tink.NewWithHCVault(address, token, keyURI, innerBlobStore)
	case "local":
		password := t.Password.Value()
		if password == "" {
			return nil, errors.New("password is required for Local KMS")
		}
		return tink.NewWithLocalKMS(password, innerBlobStore)
	default:
		return nil, fmt.Errorf("unsupported KMS type: %s", kmsType)
	}
}

type TracingBlobStoreMiddlewareConfiguration struct {
	RegionName                 internalConfig.StringProvider `json:"regionName"`
	InnerBlobStoreInstantiator BlobStoreInstantiator         `json:"-"`
	RawInnerBlobStore          json.RawMessage               `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (t *TracingBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tracingBlobStoreMiddlewareConfiguration TracingBlobStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tracingBlobStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(t.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingBlobStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := t.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingBlobStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	innerBlobStore, err := t.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName.Value(), innerBlobStore)
}

type OutboxBlobStoreConfiguration struct {
	DatabaseInstantiator       databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase                json.RawMessage                     `json:"db"`
	InnerBlobStoreInstantiator BlobStoreInstantiator               `json:"-"`
	RawInnerBlobStore          json.RawMessage                     `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (o *OutboxBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type outboxBlobStoreConfiguration OutboxBlobStoreConfiguration
	err := json.Unmarshal(b, (*outboxBlobStoreConfiguration)(o))
	if err != nil {
		return err
	}
	o.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(o.RawDatabase)
	if err != nil {
		return err
	}
	o.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(o.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := o.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = o.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	db, err := o.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	blobOutboxEntryRepository, err := repositoryFactory.NewBlobOutboxEntryRepository(db)
	if err != nil {
		return nil, err
	}
	innerBlobStore, err := o.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return outbox.New(db, innerBlobStore, blobOutboxEntryRepository)
}

type SftpBlobStoreConfiguration struct {
	Addr                        internalConfig.StringProvider          `json:"addr"`
	SshClientConfigInstantiator sftpConfig.SshClientConfigInstantiator `json:"-"`
	RawSshClientConfig          json.RawMessage                        `json:"sshClientConfig"`
	Root                        internalConfig.StringProvider          `json:"root"`
	internalConfig.DynamicJsonType
}

func (s *SftpBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sftpBlobStoreConfiguration SftpBlobStoreConfiguration
	err := json.Unmarshal(b, (*sftpBlobStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.SshClientConfigInstantiator, err = sftpConfig.CreateSshClientConfigInstantiatorFromJson(s.RawSshClientConfig)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.SshClientConfigInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	sshClientConfig, err := s.SshClientConfigInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return sftp.New(s.Addr.Value(), sshClientConfig, s.Root.Value())
}

type SqlBlobStoreConfiguration struct {
	DatabaseInstantiator databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase          json.RawMessage                     `json:"db"`
	internalConfig.DynamicJsonType
}

func (s *SqlBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sqlBlobStoreConfiguration SqlBlobStoreConfiguration
	err := json.Unmarshal(b, (*sqlBlobStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(s.RawDatabase)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	db, err := s.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	blobContentRepository, err := repositoryFactory.NewBlobContentRepository(db)
	if err != nil {
		return nil, err
	}
	return sqlBlobStore.New(db, blobContentRepository)
}

func CreateBlobStoreInstantiatorFromJson(b []byte) (BlobStoreInstantiator, error) {
	var bc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &bc)
	if err != nil {
		return nil, err
	}

	var bi BlobStoreInstantiator
	switch bc.Type {
	case filesystemBlobStoreType:
		bi = &FilesystemBlobStoreConfiguration{}
	case encryptionBlobStoreMiddlewareType:
		bi = &EncryptionBlobStoreMiddlewareConfiguration{}
	case tinkEncryptionBlobStoreMiddlewareType:
		bi = &TinkEncryptionBlobStoreMiddlewareConfiguration{}
	case tracingBlobStoreMiddlewareType:
		bi = &TracingBlobStoreMiddlewareConfiguration{}
	case outboxBlobStoreType:
		bi = &OutboxBlobStoreConfiguration{}
	case sftpBlobStoreType:
		bi = &SftpBlobStoreConfiguration{}
	case sqlBlobStoreType:
		bi = &SqlBlobStoreConfiguration{}
	default:
		return nil, errors.New("unknown blobStore type")
	}
	err = json.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
