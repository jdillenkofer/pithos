package tink

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"golang.org/x/crypto/scrypt"

	aeadsubtle "github.com/google/tink/go/aead/subtle"
	"github.com/google/tink/go/integration/awskms"
	"github.com/google/tink/go/integration/hcvault"
	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
	"github.com/google/tink/go/tink"
)

const (
	// BlobHeaderVersion is the current version of the blob header format
	BlobHeaderVersion = 1

	// Key types for different KMS providers
	KeyTypeAWS   = "aws"
	KeyTypeVault = "vault"
	KeyTypeLocal = "local"
)

// BlobHeader contains metadata about the encryption used for a blob
type BlobHeader struct {
	Version      int    `json:"version"`      // Format version for future compatibility
	KeyType      string `json:"keyType"`      // KeyTypeAWS, KeyTypeVault, or KeyTypeLocal
	KeyURI       string `json:"keyURI"`       // Key identifier (empty for local)
	EncryptedDEK []byte `json:"encryptedDEK"` // The encrypted DEK
}

// TinkEncryptionBlobStoreMiddleware uses envelope encryption where each blob has its own DEK.
type TinkEncryptionBlobStoreMiddleware struct {
	masterAEAD     tink.AEAD // Master key for encrypting DEKs
	innerBlobStore blobstore.BlobStore
	// Metadata for key rotation support
	keyType string // KeyTypeAWS, KeyTypeVault, or KeyTypeLocal
	keyURI  string // Key identifier (empty for local)
	// Vault-specific configuration for key rotation
	vaultAddr  string // Vault address (for recreating client)
	vaultToken string // Vault token (for recreating client)
}

// testKeyAvailability performs a small encrypt/decrypt test to verify the AEAD key is accessible and functional
func testKeyAvailability(aead tink.AEAD, kmsType string) error {
	testData := []byte("test")
	encrypted, err := aead.Encrypt(testData, nil)
	if err != nil {
		return fmt.Errorf("%s KMS key test failed - key may not be available: %w", kmsType, err)
	}
	decrypted, err := aead.Decrypt(encrypted, nil)
	if err != nil {
		return fmt.Errorf("%s KMS key test failed - decrypt error: %w", kmsType, err)
	}
	if string(decrypted) != string(testData) {
		return fmt.Errorf("%s KMS key test failed - data integrity check failed", kmsType)
	}
	return nil
}

// NewWithHCVault creates a new TinkEncryptionBlobStoreMiddleware using HashiCorp Vault KMS.
// Uses envelope encryption where each blob has its own DEK encrypted with the Vault master key.
// vaultAddr: e.g. "http://127.0.0.1:8200"
// token: Vault token
// keyURI: relative path e.g. "transit/keys/my-key"
func NewWithHCVault(vaultAddr, token, keyURI string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	// Convert vaultAddr to hcvault scheme for the uriPrefix
	var uriPrefix string
	if strings.HasPrefix(vaultAddr, "https://") {
		uriPrefix = "hcvault://" + vaultAddr[len("https://"):]
	} else if strings.HasPrefix(vaultAddr, "http://") {
		uriPrefix = "hcvault://" + vaultAddr[len("http://"):]
	} else {
		uriPrefix = "hcvault://" + vaultAddr
	}

	// Construct the full keyURI by combining the hcvault prefix with the relative path
	fullKeyURI := uriPrefix + "/" + strings.TrimPrefix(keyURI, "/")

	// Register the Vault KMS client with Tink
	kmsClient, err := hcvault.NewClient(uriPrefix, nil, token)
	if err != nil {
		return nil, err
	}

	// Get AEAD primitive from Vault using the full keyURI
	vaultAEAD, err := kmsClient.GetAEAD(fullKeyURI)
	if err != nil {
		return nil, err
	}

	// Test key availability
	if err := testKeyAvailability(vaultAEAD, KeyTypeVault); err != nil {
		return nil, err
	}

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     vaultAEAD,
		innerBlobStore: innerBlobStore,
		keyType:        KeyTypeVault,
		keyURI:         keyURI,
		vaultAddr:      vaultAddr,
		vaultToken:     token,
	}, nil
}

// NewWithLocalKMS creates a new TinkEncryptionBlobStoreMiddleware using a local master key (KEK).
// Uses envelope encryption where each blob has its own DEK encrypted with the local master key.
// kekBytes: the master key derived from a password using scrypt.
func NewWithLocalKMS(password string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	kekBytes, err := scrypt.Key([]byte(password), []byte("pithos"), 1<<16, 8, 1, 32)
	if err != nil {
		return nil, err
	}

	// Create an AEAD primitive from the provided KEK bytes
	kekAEAD, err := aeadsubtle.NewAESGCM(kekBytes)
	if err != nil {
		return nil, err
	}

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     kekAEAD,
		innerBlobStore: innerBlobStore,
		keyType:        KeyTypeLocal,
		keyURI:         "", // No URI for local keys
	}, nil
}

// NewWithAWSKMS creates a new TinkEncryptionBlobStoreMiddleware using AWS KMS.
// Uses envelope encryption where each blob has its own DEK encrypted with the AWS KMS master key.
// keyURI: e.g. "aws-kms://arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
// region: AWS region (e.g. "us-east-1")
func NewWithAWSKMS(keyURI, region string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	// Create AWS KMS client
	kmsClient, err := awskms.NewClient(keyURI)
	if err != nil {
		return nil, err
	}

	// Get AEAD primitive from AWS KMS
	awsAEAD, err := kmsClient.GetAEAD(keyURI)
	if err != nil {
		return nil, err
	}

	// Test key availability
	if err := testKeyAvailability(awsAEAD, KeyTypeAWS); err != nil {
		return nil, err
	}

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     awsAEAD,
		innerBlobStore: innerBlobStore,
		keyType:        KeyTypeAWS,
		keyURI:         keyURI,
	}, nil
}

func (mw *TinkEncryptionBlobStoreMiddleware) Start(ctx context.Context) error {
	return mw.innerBlobStore.Start(ctx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) Stop(ctx context.Context) error {
	return mw.innerBlobStore.Stop(ctx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	// Generate a new 32-byte DEK for this blob
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return err
	}

	// Create streaming AEAD with the DEK
	dekStreamingAEAD, err := streamingaeadsubtle.NewAESGCMHKDF(dek, "SHA256", 32, 4096, 0)
	if err != nil {
		return err
	}

	// Encrypt the DEK with the master AEAD
	encryptedDEK, err := mw.masterAEAD.Encrypt(dek, blobId[:])
	if err != nil {
		return err
	}

	// Create header with key metadata and encrypted DEK
	header := BlobHeader{
		Version:      BlobHeaderVersion,
		KeyType:      mw.keyType,
		KeyURI:       mw.keyURI,
		EncryptedDEK: encryptedDEK,
	}

	// Serialize header to JSON
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return err
	}

	// Create a pipe for the combined data (header + encrypted blob)
	encryptReader, encryptWriter := io.Pipe()
	go func() {
		defer encryptWriter.Close()

		// Write the header length (4 bytes big-endian)
		headerLen := uint32(len(headerBytes))
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, headerLen)
		if _, err := encryptWriter.Write(lengthBytes); err != nil {
			encryptWriter.CloseWithError(err)
			return
		}

		// Write the header
		if _, err := encryptWriter.Write(headerBytes); err != nil {
			encryptWriter.CloseWithError(err)
			return
		}

		// Now stream encrypt the blob data with the DEK
		streamWriter, err := dekStreamingAEAD.NewEncryptingWriter(encryptWriter, blobId[:])
		if err != nil {
			encryptWriter.CloseWithError(err)
			return
		}

		// Stream copy from reader to encrypting writer
		if _, err := io.Copy(streamWriter, reader); err != nil {
			encryptWriter.CloseWithError(err)
			return
		}

		// Close the stream writer to finalize encryption
		if err := streamWriter.Close(); err != nil {
			encryptWriter.CloseWithError(err)
			return
		}
	}()

	return mw.innerBlobStore.PutBlob(ctx, tx, blobId, encryptReader)
}

func (mw *TinkEncryptionBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	rc, err := mw.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	// Read the header length (4 bytes big-endian)
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(rc, lengthBytes); err != nil {
		rc.Close()
		return nil, err
	}

	headerLen := binary.BigEndian.Uint32(lengthBytes)

	// Read and parse the header
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(rc, headerBytes); err != nil {
		rc.Close()
		return nil, err
	}

	var header BlobHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		rc.Close()
		return nil, err
	}

	if header.Version != BlobHeaderVersion {
		rc.Close()
		return nil, io.ErrUnexpectedEOF
	}

	// Extract the encrypted DEK from the header
	encryptedDEK := header.EncryptedDEK

	// Decrypt the DEK with master AEAD
	dek, err := mw.masterAEAD.Decrypt(encryptedDEK, blobId[:])
	if err != nil {
		rc.Close()
		return nil, err
	}

	// Create streaming AEAD with the DEK
	dekStreamingAEAD, err := streamingaeadsubtle.NewAESGCMHKDF(dek, "SHA256", 32, 4096, 0)
	if err != nil {
		rc.Close()
		return nil, err
	}

	// Create a decrypting reader for the remaining data
	decryptingReader, err := dekStreamingAEAD.NewDecryptingReader(rc, blobId[:])
	if err != nil {
		rc.Close()
		return nil, err
	}

	return &compositeReadCloser{decryptingReader, rc}, nil
}

// compositeReadCloser combines a Reader with a Closer
type compositeReadCloser struct {
	io.Reader
	closer io.Closer
}

func (c *compositeReadCloser) Close() error {
	return c.closer.Close()
}

func (mw *TinkEncryptionBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	return mw.innerBlobStore.GetBlobIds(ctx, tx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	return mw.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
