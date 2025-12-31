package config

import (
	"crypto/mlkem"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink/tpm"
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
)

type PartStoreInstantiator = internalConfig.DynamicJsonInstantiator[partstore.PartStore]

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
	TPMPath             internalConfig.StringProvider `json:"tpmPath,omitempty"`             // Path to TPM device (e.g., "/dev/tpmrm0")
	TPMPersistentHandle internalConfig.StringProvider `json:"tpmPersistentHandle,omitempty"` // Persistent handle for TPM key (e.g., "0x81000001")
	TPMKeyFilePath      internalConfig.StringProvider `json:"tpmKeyFilePath,omitempty"`      // Path to file for persisting AES key material (e.g., "./data/tpm-aes-key.json")
	TPMKeyAlgorithm     internalConfig.StringProvider `json:"tpmKeyAlgorithm,omitempty"`     // Primary key algorithm: "rsa-2048" (default), "rsa-4096", "ecc-p256", "ecc-p384", "ecc-p521", "ecc-brainpool-p256", "ecc-brainpool-p384", "ecc-brainpool-p512"
	// Symmetric key algorithm ("aes-128", "aes-256"). Default is "aes-128".
	TPMSymmetricAlgorithm internalConfig.StringProvider `json:"tpmSymmetricAlgorithm,omitempty"`
	// HMAC algorithm ("sha256", "sha384", "sha512"). Default is "sha256".
	TPMHMACAlgorithm internalConfig.StringProvider `json:"tpmHMACAlgorithm,omitempty"`
	// TPM Password for key authorization (optional, empty string for no password)
	TPMPassword internalConfig.StringProvider `json:"tpmPassword,omitempty"`
	// PQ-safe specific
	// PQSeed is the 64-byte hex-encoded seed for ML-KEM-1024.
	// WARNING: This seed is used to derive the private key. If this seed is lost or changed,
	// previously encrypted data CANNOT be decrypted.
	// To generate: openssl rand -hex 64
	PQSeed                     internalConfig.StringProvider `json:"pqSeed,omitempty"`
	InnerPartStoreInstantiator PartStoreInstantiator         `json:"-"`
	RawInnerPartStore          json.RawMessage               `json:"innerPartStore"`
	internalConfig.DynamicJsonType
}

func (t *TinkEncryptionPartStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tinkEncryptionPartStoreMiddlewareConfiguration TinkEncryptionPartStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tinkEncryptionPartStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
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

	var mlkemKey *mlkem.DecapsulationKey1024
	pqSeedHex := t.PQSeed.Value()
	if pqSeedHex != "" {
		seed, err := hex.DecodeString(pqSeedHex)
		if err != nil {
			return nil, fmt.Errorf("invalid pqSeed format: %w", err)
		}
		mlkemKey, err = mlkem.NewDecapsulationKey1024(seed)
		if err != nil {
			return nil, fmt.Errorf("failed to create ML-KEM key from seed: %w", err)
		}
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
		return tink.NewWithAWSKMS(keyURI, region, innerPartStore, mlkemKey)
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

		return tink.NewWithHCVault(address, token, roleID, secretID, keyURI, innerPartStore, nil, mlkemKey)
	case "local":
		password := t.Password.Value()
		if password == "" {
			return nil, errors.New("password is required for Local KMS")
		}
		return tink.NewWithLocalKMS(password, innerPartStore, mlkemKey)
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

		// Get key algorithm (default to ecc-p256)
		keyAlgorithm := t.TPMKeyAlgorithm.Value()
		if keyAlgorithm == "" {
			keyAlgorithm = tpm.KeyAlgorithmECCP256
		}
		// Validate key algorithm
		switch keyAlgorithm {
		case tpm.KeyAlgorithmRSA2048, tpm.KeyAlgorithmRSA4096, tpm.KeyAlgorithmECCP256, tpm.KeyAlgorithmECCP384, tpm.KeyAlgorithmECCP521, tpm.KeyAlgorithmECCBrainpoolP256, tpm.KeyAlgorithmECCBrainpoolP384, tpm.KeyAlgorithmECCBrainpoolP512:
			// Valid
		default:
			return nil, fmt.Errorf("invalid tpmKeyAlgorithm: %s", keyAlgorithm)
		}

		// Get symmetric key algorithm (default to aes-256)
		symmetricAlgorithm := t.TPMSymmetricAlgorithm.Value()
		if symmetricAlgorithm == "" {
			symmetricAlgorithm = tpm.SymmetricAlgorithmAES256
		}

		// Get HMAC algorithm (default to sha256)
		hmacAlgorithm := t.TPMHMACAlgorithm.Value()
		if hmacAlgorithm == "" {
			hmacAlgorithm = tpm.HMACAlgorithmSHA256
		}
		if hmacAlgorithm != tpm.HMACAlgorithmSHA256 && hmacAlgorithm != tpm.HMACAlgorithmSHA384 && hmacAlgorithm != tpm.HMACAlgorithmSHA512 {
			return nil, fmt.Errorf("invalid tpmHMACAlgorithm: %s", hmacAlgorithm)
		}

		if symmetricAlgorithm != tpm.SymmetricAlgorithmAES128 && symmetricAlgorithm != tpm.SymmetricAlgorithmAES256 {
			return nil, fmt.Errorf("invalid tpmSymmetricAlgorithm: %s", symmetricAlgorithm)
		}

		// Get TPM password (optional, defaults to empty string for backwards compatibility)
		tpmPassword := t.TPMPassword.Value()

		return tink.NewWithTPM(tpmPath, persistentHandle, keyFilePath, keyAlgorithm, symmetricAlgorithm, hmacAlgorithm, tpmPassword, innerPartStore, mlkemKey)
	default:
		return nil, fmt.Errorf("unsupported KMS type: %s", kmsType)
	}
}

type OutboxPartStoreConfiguration struct {
	DatabaseInstantiator       databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase                json.RawMessage                     `json:"db"`
	InnerPartStoreInstantiator PartStoreInstantiator               `json:"-"`
	RawInnerPartStore          json.RawMessage                     `json:"innerPartStore"`
	internalConfig.DynamicJsonType
}

func (o *OutboxPartStoreConfiguration) UnmarshalJSON(b []byte) error {
	type outboxPartStoreConfiguration OutboxPartStoreConfiguration
	err := json.Unmarshal(b, (*outboxPartStoreConfiguration)(o))
	if err != nil {
		return err
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
	case tinkEncryptionPartStoreMiddlewareType:
		bi = &TinkEncryptionPartStoreMiddlewareConfiguration{}
	case outboxPartStoreType:
		bi = &OutboxPartStoreConfiguration{}
	case sftpPartStoreType:
		bi = &SftpPartStoreConfiguration{}
	case sqlPartStoreType:
		bi = &SqlPartStoreConfiguration{}
	default:
		return nil, errors.New("unknown partStore type")
	}
	err = json.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
