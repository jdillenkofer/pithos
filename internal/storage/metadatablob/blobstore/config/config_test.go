package config

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ssh"
)

func createBlobStoreFromJson(b []byte) (blobstore.BlobStore, error) {
	diContainer, err := dependencyinjection.NewContainer()
	if err != nil {
		return nil, err
	}
	dbContainer := config.NewDbContainer()
	err = diContainer.RegisterSingletonByType(reflect.TypeOf((*config.DbContainer)(nil)), dbContainer)
	if err != nil {
		return nil, err
	}
	mi, err := CreateBlobStoreInstantiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	err = mi.RegisterReferences(diContainer)
	if err != nil {
		return nil, err
	}
	return mi.Instantiate(diContainer)
}

func TestCanCreateFilesystemBlobStoreFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "FilesystemBlobStore",
				 "root": %s
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateSftpBlobStoreFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	assert.Nil(t, err)

	privateKeyPath := filepath.Join(*tempDir, "id_rsa.pem")
	pemBlock, err := ssh.MarshalPrivateKey(privateKey, "")
	assert.Nil(t, err)
	pemData := pem.EncodeToMemory(pemBlock)
	err = os.WriteFile(privateKeyPath, pemData, 0600)
	assert.Nil(t, err)

	passphrase := "test"
	privateKeyWithPassphrasePath := filepath.Join(*tempDir, "id_rsa_enc.pem")
	pemBlockEnc, err := ssh.MarshalPrivateKeyWithPassphrase(privateKey, "", []byte(passphrase))
	assert.Nil(t, err)
	pemDataEnc := pem.EncodeToMemory(pemBlockEnc)
	err = os.WriteFile(privateKeyWithPassphrasePath, pemDataEnc, 0600)
	assert.Nil(t, err)

	addr := "localhost:22"
	hostKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDe1NeJm4Ys7jZIXkCT0aASrBHKASVq9cVIbp+oBzCP13z62ILmQ4awd0toQmrz5oP53Mc102s/y3ayzgeMdwg/mvopzmGtb1zqxLel84OlMUEMX492qera0esgJ9tiJTfixabyADY/KJ8euTLbi/WhJ3IxEWlR6cbeaJMGDtO9n/b4d32pjrr4WYb14lZhEQlYN9xccco6P9bqASSBh7dIxUroZX8ogZSAcRJ9B6+6mXE6bm84jDv50traduDd1JQDjw39d8Mk7WPo9ZCcvnDC9HaM4Yg9S5pWMLmIL0jY2cCFQCQ6qRCfebIABTIEeuz49ZMD/VrPGHrcLYoxUlLMhziEsKWeYmRN/ODQ6uJNyacOHum9aigUEXUvACijNDbeGSxC9FxEl8euHVYV6tiW167qeYowIIjU5hJDd4NT5Qtu3RG+ZkSVzhaBbyln5FbImg7QrX3j3wDtFMCHfhl2oSWaEF+oLirMzOLmV1zcN5GlrUdGvxMZNFkR5gmCeErHf4JT+G2XdE/EkaEG63W+QSfgKWpqAIPwsIHshpbF/W23eFvJ/XzhjmNidHy0/vIwj5XqnibKhb4EXDVoXACxigKt1AKNE/9sS/E2p7/YeGiJ9keoaHzYbCviFokzONgZb4WZc+FXIVom5C4SEhUesEVheTLdTXzvZ6FnHzQvqQ=="
	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "SftpBlobStore",
				 "addr": %s,
				 "sshClientConfig": {
							 "user": "user",
							 "authMethods": [
								 {
									 "type": "PasswordAuthMethod",
											 "password": "test"
								 },
								 {
									 "type": "PublicKeyAuthMethod",
											 "signers": [
												 {
													 "type": "Signer",
															 "path": %s
												 },
												 {
													 "type": "SignerWithPassphrase",
															 "path": %s,
															 "passphrase": %s
												 }
											 ]
								 }
							 ],
							 "hostKeyCallback": {
								 "type": "FixedHostKeyCallback",
								 "hostKey": %s
							 },
							 "hostKeyAlgorithms": [
								 "rsa-sha2-256", 
								 "rsa-sha2-512"
							 ],
							 "connectionTimeout": "5s"
				 },
				 "root": %s
			 }`, strconv.Quote(addr), strconv.Quote(privateKeyPath), strconv.Quote(privateKeyWithPassphrasePath), strconv.Quote(passphrase), strconv.Quote(hostKey), strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateEncryptionBlobStoreMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "EncryptionBlobStoreMiddleware",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "local",
				 "password": "test-password-123",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestTinkEncryptionBlobStoreMiddlewareRequiresKeyURIForAWS(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "aws",
				 "awsRegion": "us-east-1",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "keyURI is required for AWS KMS")
}

func TestTinkEncryptionBlobStoreMiddlewareRequiresKeyURIForVault(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "vaultAddress": "https://vault.example.com:8200",
				 "vaultToken": "hvs.test-token",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "keyURI is required for Vault KMS")
}

func TestTinkEncryptionBlobStoreMiddlewareRequiresVaultCredentials(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "keyURI": "hcvault://transit/keys/my-key",
				 "vaultAddress": "https://vault.example.com:8200",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "either vaultToken or (vaultRoleId and vaultSecretId) must be provided")
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareWithAWSKMS(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "aws",
				 "keyURI": "aws-kms://arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
				 "awsRegion": "us-east-1",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	// Test that configuration parsing works, but skip actual KMS connection test
	// since we don't have AWS credentials in test environment
	instantiator, err := CreateBlobStoreInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, instantiator)

	// Verify the configuration was parsed correctly by checking the type
	config, ok := instantiator.(*TinkEncryptionBlobStoreMiddlewareConfiguration)
	assert.True(t, ok)
	assert.Equal(t, "aws", config.KMSType.Value())
	assert.Equal(t, "aws-kms://arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012", config.KeyURI.Value())
	assert.Equal(t, "us-east-1", config.AWSRegion.Value())
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareWithVaultKMS(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "keyURI": "transit/keys/my-key",
				 "vaultAddress": "https://vault.example.com:8200",
				 "vaultToken": "hvs.test-token",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	// Test that configuration parsing works, but skip actual Vault connection test
	// since we don't have a Vault server in test environment
	instantiator, err := CreateBlobStoreInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, instantiator)

	// Verify the configuration was parsed correctly by checking the type
	config, ok := instantiator.(*TinkEncryptionBlobStoreMiddlewareConfiguration)
	assert.True(t, ok)
	assert.Equal(t, "vault", config.KMSType.Value())
	assert.Equal(t, "transit/keys/my-key", config.KeyURI.Value())
	assert.Equal(t, "https://vault.example.com:8200", config.VaultAddress.Value())
	assert.Equal(t, "hvs.test-token", config.VaultToken.Value())
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareWithVaultAppRole(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "keyURI": "transit/keys/my-key",
				 "vaultAddress": "https://vault.example.com:8200",
				 "vaultRoleId": "test-role-id",
				 "vaultSecretId": "test-secret-id",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	// Test that configuration parsing works
	instantiator, err := CreateBlobStoreInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, instantiator)

	// Verify the configuration was parsed correctly
	config, ok := instantiator.(*TinkEncryptionBlobStoreMiddlewareConfiguration)
	assert.True(t, ok)
	assert.Equal(t, "vault", config.KMSType.Value())
	assert.Equal(t, "transit/keys/my-key", config.KeyURI.Value())
	assert.Equal(t, "https://vault.example.com:8200", config.VaultAddress.Value())
	assert.Equal(t, "test-role-id", config.VaultRoleID.Value())
	assert.Equal(t, "test-secret-id", config.VaultSecretID.Value())
}

func TestTinkEncryptionBlobStoreMiddlewareRejectsBothTokenAndAppRole(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "keyURI": "transit/keys/my-key",
				 "vaultAddress": "https://vault.example.com:8200",
				 "vaultToken": "hvs.test-token",
				 "vaultRoleId": "test-role-id",
				 "vaultSecretId": "test-secret-id",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "cannot use both vaultToken and AppRole authentication")
}

func TestTinkEncryptionBlobStoreMiddlewareRequiresBothRoleIdAndSecretId(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	// Test with only roleId
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "vault",
				 "keyURI": "transit/keys/my-key",
				 "vaultAddress": "https://vault.example.com:8200",
				 "vaultRoleId": "test-role-id",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "either vaultToken or (vaultRoleId and vaultSecretId) must be provided")
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareWithTPM(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "tpm",
				 "tpmPath": "/dev/tpmrm0",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	// Test that configuration parsing works
	instantiator, err := CreateBlobStoreInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, instantiator)

	// Verify the configuration was parsed correctly
	config, ok := instantiator.(*TinkEncryptionBlobStoreMiddlewareConfiguration)
	assert.True(t, ok)
	assert.Equal(t, "tpm", config.KMSType.Value())
	assert.Equal(t, "/dev/tpmrm0", config.TPMPath.Value())
}

func TestTinkEncryptionBlobStoreMiddlewareRequiresTPMPath(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "tpm",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.NotNil(t, err)
	assert.Nil(t, blobStore)
	assert.Contains(t, err.Error(), "tpmPath is required for TPM KMS")
}

func TestCanCreateTinkEncryptionBlobStoreMiddlewareWithTPMAndPersistentHandle(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	jsonData := fmt.Sprintf(`{
				 "type": "TinkEncryptionBlobStoreMiddleware",
				 "kmsType": "tpm",
				 "tpmPath": "/dev/tpmrm0",
				 "tpmPersistentHandle": "0x81000002",
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(storagePath))

	// Test that configuration parsing works
	instantiator, err := CreateBlobStoreInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, instantiator)

	// Verify the configuration was parsed correctly
	config, ok := instantiator.(*TinkEncryptionBlobStoreMiddlewareConfiguration)
	assert.True(t, ok)
	assert.Equal(t, "tpm", config.KMSType.Value())
	assert.Equal(t, "/dev/tpmrm0", config.TPMPath.Value())
	assert.Equal(t, "0x81000002", config.TPMPersistentHandle.Value())
}

func TestCanCreateOutboxBlobStoreFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
				 "type": "OutboxBlobStore",
				 "db": {
					 "type": "SqliteDatabase",
							 "dbPath": %s
				 },
				 "innerBlobStore": {
					 "type": "FilesystemBlobStore",
					 "root": %s
				 }
			 }`, strconv.Quote(dbPath), strconv.Quote(storagePath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}

func TestCanCreateSqlBlobStoreFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, cleanup, err := config.CreateTempDir()
	assert.Nil(t, err)
	t.Cleanup(cleanup)

	storagePath := *tempDir
	dbPath := filepath.Join(storagePath, "pithos.db")
	jsonData := fmt.Sprintf(`{
				 "type": "SqlBlobStore",
				 "db": {
					 "type": "SqliteDatabase",
							 "dbPath": %s
				 }
			 }`, strconv.Quote(dbPath))

	blobStore, err := createBlobStoreFromJson([]byte(jsonData))
	assert.Nil(t, err)
	assert.NotNil(t, blobStore)
}
