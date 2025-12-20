package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sftp"
	sftpConfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sftp/config"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
)

const (
	filesystemPartStoreType               = "FilesystemPartStore"
	tinkEncryptionPartStoreMiddlewareType = "TinkEncryptionPartStoreMiddleware"
	outboxPartStoreType                   = "OutboxPartStore"
	sftpPartStoreType                     = "SftpPartStore"
	sqlPartStoreType                      = "SqlPartStore"

	// Deprecated: Use FilesystemPartStore instead. Will be removed in a future version.
	filesystemBlobStoreType = "FilesystemBlobStore"
	// Deprecated: Use TinkEncryptionPartStoreMiddleware instead. Will be removed in a future version.
	tinkEncryptionBlobStoreMiddlewareType = "TinkEncryptionBlobStoreMiddleware"
	// Deprecated: Use OutboxPartStore instead. Will be removed in a future version.
	outboxBlobStoreType = "OutboxBlobStore"
	// Deprecated: Use SftpPartStore instead. Will be removed in a future version.
	sftpBlobStoreType = "SftpBlobStore"
	// Deprecated: Use SqlPartStore instead. Will be removed in a future version.
	sqlBlobStoreType = "SqlBlobStore"
)

type PartStoreInstantiator = internalConfig.DynamicJsonInstantiator[partstore.PartStore]

// Deprecated: Use FilesystemPartStoreConfiguration instead. Will be removed in a future version.
type FilesystemBlobStoreConfiguration struct {
	FilesystemPartStoreConfiguration
}

func (f *FilesystemBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("FilesystemBlobStore is deprecated. Please use FilesystemPartStore instead.")
	type filesystemBlobStoreConfiguration FilesystemBlobStoreConfiguration
	err := json.Unmarshal(b, (*filesystemBlobStoreConfiguration)(f))
	if err != nil {
		return err
	}
	// No nested structs to handle specifically, just the type/root which are shared
	return nil
}

type FilesystemPartStoreConfiguration struct {
	Root internalConfig.StringProvider `json:"root"`
	internalConfig.DynamicJsonType
}

func (f *FilesystemPartStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FilesystemPartStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (partstore.PartStore, error) {
	return filesystem.New(f.Root.Value())
}

// Deprecated: Use TinkEncryptionPartStoreMiddlewareConfiguration instead. Will be removed in a future version.
type TinkEncryptionBlobStoreMiddlewareConfiguration struct {
	TinkEncryptionPartStoreMiddlewareConfiguration
}

func (t *TinkEncryptionBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("TinkEncryptionBlobStoreMiddleware is deprecated. Please use TinkEncryptionPartStoreMiddleware instead.")
	type tinkEncryptionBlobStoreMiddlewareConfigurationAlias struct {
		KMSType             internalConfig.StringProvider `json:"kmsType"`
		KeyURI              internalConfig.StringProvider `json:"keyURI,omitempty"`
		AWSRegion           internalConfig.StringProvider `json:"awsRegion,omitempty"`
		VaultAddress        internalConfig.StringProvider `json:"vaultAddress,omitempty"`
		VaultToken          internalConfig.StringProvider `json:"vaultToken,omitempty"`
		VaultRoleID         internalConfig.StringProvider `json:"vaultRoleId,omitempty"`
		VaultSecretID       internalConfig.StringProvider `json:"vaultSecretId,omitempty"`
		Password            internalConfig.StringProvider `json:"password,omitempty"`
		TPMPath             internalConfig.StringProvider `json:"tpmPath,omitempty"`
		TPMPersistentHandle internalConfig.StringProvider `json:"tpmPersistentHandle,omitempty"`
		TPMKeyFilePath      internalConfig.StringProvider `json:"tpmKeyFilePath,omitempty"`
		RawInnerPartStore   json.RawMessage               `json:"innerPartStore"`
		RawInnerBlobStore   json.RawMessage               `json:"innerBlobStore"`
		internalConfig.DynamicJsonType
	}
	var alias tinkEncryptionBlobStoreMiddlewareConfigurationAlias
	err := json.Unmarshal(b, &alias)
	if err != nil {
		return err
	}

	t.KMSType = alias.KMSType
	t.KeyURI = alias.KeyURI
	t.AWSRegion = alias.AWSRegion
	t.VaultAddress = alias.VaultAddress
	t.VaultToken = alias.VaultToken
	t.VaultRoleID = alias.VaultRoleID
	t.VaultSecretID = alias.VaultSecretID
	t.Password = alias.Password
	t.TPMPath = alias.TPMPath
	t.TPMPersistentHandle = alias.TPMPersistentHandle
	t.TPMKeyFilePath = alias.TPMKeyFilePath
	t.Type = alias.Type

	if alias.RawInnerBlobStore != nil {
		t.RawInnerPartStore = alias.RawInnerBlobStore
	} else {
		t.RawInnerPartStore = alias.RawInnerPartStore
	}

	t.InnerPartStoreInstantiator, err = CreatePartStoreInstantiatorFromJson(t.RawInnerPartStore)
	if err != nil {
		return err
	}
	return nil
}

type TinkEncryptionPartStoreMiddlewareConfiguration struct {
	KMSType internalConfig.StringProvider `json:"kmsType"`          // "aws", "vault", "local", "tpm"
	KeyURI  internalConfig.StringProvider `json:"keyURI,omitempty"` // Not used for local/tpm KMS
	// AWS KMS specific
	AWSRegion internalConfig.StringProvider `json:"awsRegion,omitempty"`
	// Vault specific
	VaultAddress  internalConfig.StringProvider `json:"vaultAddress,omitempty"`
	VaultToken    internalConfig.StringProvider `json:"vaultToken,omitempty"`    // Token-based auth
	VaultRoleID   internalConfig.StringProvider `json:"vaultRoleId,omitempty"`   // AppRole auth
	VaultSecretID internalConfig.StringProvider `json:"vaultSecretId,omitempty"` // AppRole auth
	// Local KMS specific (password for key derivation)
	Password internalConfig.StringProvider `json:"password,omitempty"`
	// TPM specific
	TPMPath                    internalConfig.StringProvider `json:"tpmPath,omitempty"`             // Path to TPM device (e.g., "/dev/tpmrm0")
	TPMPersistentHandle        internalConfig.StringProvider `json:"tpmPersistentHandle,omitempty"` // Persistent handle for TPM key (e.g., "0x81000001")
	TPMKeyFilePath             internalConfig.StringProvider `json:"tpmKeyFilePath,omitempty"`      // Path to file for persisting AES key material (e.g., "./data/tpm-aes-key.json")
	InnerPartStoreInstantiator PartStoreInstantiator         `json:"-"`
	RawInnerPartStore          json.RawMessage               `json:"innerPartStore"`
	// For backward compatibility
	RawInnerBlobStore json.RawMessage `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (t *TinkEncryptionPartStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tinkEncryptionPartStoreMiddlewareConfiguration TinkEncryptionPartStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tinkEncryptionPartStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}

	if t.RawInnerBlobStore != nil {
		if t.RawInnerPartStore == nil {
			t.RawInnerPartStore = t.RawInnerBlobStore
		}
	}

	t.InnerPartStoreInstantiator, err = CreatePartStoreInstantiatorFromJson(t.RawInnerPartStore)
	if err != nil {
		return err
	}
	return nil
}

func (t *TinkEncryptionPartStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := t.InnerPartStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (t *TinkEncryptionPartStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (partstore.PartStore, error) {
	innerPartStore, err := t.InnerPartStoreInstantiator.Instantiate(diProvider)
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
		return tink.NewWithAWSKMS(keyURI, region, innerPartStore)
	case "vault":
		keyURI := t.KeyURI.Value()
		address := t.VaultAddress.Value()
		token := t.VaultToken.Value()
		roleID := t.VaultRoleID.Value()
		secretID := t.VaultSecretID.Value()

		if keyURI == "" {
			return nil, errors.New("keyURI is required for Vault KMS")
		}
		if address == "" {
			return nil, errors.New("vaultAddress is required for Vault KMS")
		}

		// Check that either token OR (roleID AND secretID) is provided
		hasToken := token != ""
		hasAppRole := roleID != "" && secretID != ""

		if !hasToken && !hasAppRole {
			return nil, errors.New("either vaultToken or (vaultRoleId and vaultSecretId) must be provided for Vault KMS")
		}

		if hasToken && hasAppRole {
			return nil, errors.New("cannot use both vaultToken and AppRole authentication - choose one method")
		}

		return tink.NewWithHCVault(address, token, roleID, secretID, keyURI, innerPartStore)
	case "local":
		password := t.Password.Value()
		if password == "" {
			return nil, errors.New("password is required for Local KMS")
		}
		return tink.NewWithLocalKMS(password, innerPartStore)
	case "tpm":
		tpmPath := t.TPMPath.Value()
		if tpmPath == "" {
			return nil, errors.New("tpmPath is required for TPM KMS")
		}

		// Parse persistent handle (default to 0x81000001 if not specified)
		persistentHandleStr := t.TPMPersistentHandle.Value()
		var persistentHandle uint32 = 0x81000001 // Default handle
		if persistentHandleStr != "" {
			var handle uint64
			_, err := fmt.Sscanf(persistentHandleStr, "0x%x", &handle)
			if err != nil {
				return nil, fmt.Errorf("invalid tpmPersistentHandle format (expected hex like 0x81000001): %w", err)
			}
			if handle < 0x81000000 || handle > 0x81FFFFFF {
				return nil, fmt.Errorf("tpmPersistentHandle must be in range 0x81000000-0x81FFFFFF, got 0x%08X", handle)
			}
			persistentHandle = uint32(handle)
		}

		// Get key file path (default to "./data/tpm-aes-key.json" if not specified)
		keyFilePath := t.TPMKeyFilePath.Value()

		return tink.NewWithTPM(tpmPath, persistentHandle, keyFilePath, innerPartStore)
	default:
		return nil, fmt.Errorf("unsupported KMS type: %s", kmsType)
	}
}

// Deprecated: Use OutboxPartStoreConfiguration instead. Will be removed in a future version.
type OutboxBlobStoreConfiguration struct {
	OutboxPartStoreConfiguration
}

func (o *OutboxBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("OutboxBlobStore is deprecated. Please use OutboxPartStore instead.")
	type outboxBlobStoreConfigurationAlias struct {
		RawDatabase        json.RawMessage `json:"db"`
		RawInnerPartStore  json.RawMessage `json:"innerPartStore"`
		RawInnerBlobStore  json.RawMessage `json:"innerBlobStore"`
		internalConfig.DynamicJsonType
	}
	var alias outboxBlobStoreConfigurationAlias
	err := json.Unmarshal(b, &alias)
	if err != nil {
		return err
	}

	o.RawDatabase = alias.RawDatabase
	o.Type = alias.Type

	if alias.RawInnerBlobStore != nil {
		o.RawInnerPartStore = alias.RawInnerBlobStore
	} else {
		o.RawInnerPartStore = alias.RawInnerPartStore
	}

	o.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(o.RawDatabase)
	if err != nil {
		return err
	}
	o.InnerPartStoreInstantiator, err = CreatePartStoreInstantiatorFromJson(o.RawInnerPartStore)
	if err != nil {
		return err
	}
	return nil
}

type OutboxPartStoreConfiguration struct {
	DatabaseInstantiator       databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase                json.RawMessage                     `json:"db"`
	InnerPartStoreInstantiator PartStoreInstantiator               `json:"-"`
	RawInnerPartStore          json.RawMessage                     `json:"innerPartStore"`
	// For backward compatibility
	RawInnerBlobStore json.RawMessage `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (o *OutboxPartStoreConfiguration) UnmarshalJSON(b []byte) error {
	type outboxPartStoreConfiguration OutboxPartStoreConfiguration
	err := json.Unmarshal(b, (*outboxPartStoreConfiguration)(o))
	if err != nil {
		return err
	}

	if o.RawInnerBlobStore != nil {
		if o.RawInnerPartStore == nil {
			o.RawInnerPartStore = o.RawInnerBlobStore
		}
	}

	o.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(o.RawDatabase)
	if err != nil {
		return err
	}
	o.InnerPartStoreInstantiator, err = CreatePartStoreInstantiatorFromJson(o.RawInnerPartStore)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxPartStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := o.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = o.InnerPartStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxPartStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (partstore.PartStore, error) {
	db, err := o.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	partOutboxEntryRepository, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	if err != nil {
		return nil, err
	}
	innerPartStore, err := o.InnerPartStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return outbox.New(db, innerPartStore, partOutboxEntryRepository)
}

// Deprecated: Use SftpPartStoreConfiguration instead. Will be removed in a future version.
type SftpBlobStoreConfiguration struct {
	SftpPartStoreConfiguration
}

func (s *SftpBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("SftpBlobStore is deprecated. Please use SftpPartStore instead.")
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

type SftpPartStoreConfiguration struct {
	Addr                        internalConfig.StringProvider          `json:"addr"`
	SshClientConfigInstantiator sftpConfig.SshClientConfigInstantiator `json:"-"`
	RawSshClientConfig          json.RawMessage                        `json:"sshClientConfig"`
	Root                        internalConfig.StringProvider          `json:"root"`
	internalConfig.DynamicJsonType
}

func (s *SftpPartStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sftpPartStoreConfiguration SftpPartStoreConfiguration
	err := json.Unmarshal(b, (*sftpPartStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.SshClientConfigInstantiator, err = sftpConfig.CreateSshClientConfigInstantiatorFromJson(s.RawSshClientConfig)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpPartStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.SshClientConfigInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SftpPartStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (partstore.PartStore, error) {
	sshClientConfig, err := s.SshClientConfigInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return sftp.New(s.Addr.Value(), sshClientConfig, s.Root.Value())
}

// Deprecated: Use SqlPartStoreConfiguration instead. Will be removed in a future version.
type SqlBlobStoreConfiguration struct {
	SqlPartStoreConfiguration
}

func (s *SqlBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	slog.Warn("SqlBlobStore is deprecated. Please use SqlPartStore instead.")
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

type SqlPartStoreConfiguration struct {
	DatabaseInstantiator databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase          json.RawMessage                     `json:"db"`
	internalConfig.DynamicJsonType
}

func (s *SqlPartStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sqlPartStoreConfiguration SqlPartStoreConfiguration
	err := json.Unmarshal(b, (*sqlPartStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(s.RawDatabase)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlPartStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlPartStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (partstore.PartStore, error) {
	db, err := s.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		return nil, err
	}
	return sqlPartStore.New(db, partContentRepository)
}

func CreatePartStoreInstantiatorFromJson(b []byte) (PartStoreInstantiator, error) {
	var bc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &bc)
	if err != nil {
		return nil, err
	}

	var bi PartStoreInstantiator
	switch bc.Type {
	case filesystemPartStoreType:
		bi = &FilesystemPartStoreConfiguration{}
	case filesystemBlobStoreType:
		bi = &FilesystemBlobStoreConfiguration{}
	case tinkEncryptionPartStoreMiddlewareType:
		bi = &TinkEncryptionPartStoreMiddlewareConfiguration{}
	case tinkEncryptionBlobStoreMiddlewareType:
		bi = &TinkEncryptionBlobStoreMiddlewareConfiguration{}
	case outboxPartStoreType:
		bi = &OutboxPartStoreConfiguration{}
	case outboxBlobStoreType:
		bi = &OutboxBlobStoreConfiguration{}
	case sftpPartStoreType:
		bi = &SftpPartStoreConfiguration{}
	case sftpBlobStoreType:
		bi = &SftpBlobStoreConfiguration{}
	case sqlPartStoreType:
		bi = &SqlPartStoreConfiguration{}
	case sqlBlobStoreType:
		bi = &SqlBlobStoreConfiguration{}
	default:
		return nil, errors.New("unknown partStore type")
	}
	err = json.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
