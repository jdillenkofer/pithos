package tink

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"io"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"golang.org/x/crypto/scrypt"

	aeadsubtle "github.com/google/tink/go/aead/subtle"
	"github.com/google/tink/go/integration/awskms"
	"github.com/google/tink/go/integration/hcvault"
	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
	"github.com/google/tink/go/tink"
)

// TinkEncryptionBlobStoreMiddleware uses envelope encryption where each blob has its own DEK.
type TinkEncryptionBlobStoreMiddleware struct {
	masterAEAD     tink.AEAD // Master key for encrypting DEKs
	innerBlobStore blobstore.BlobStore
}

// NewWithHCVault creates a new TinkEncryptionBlobStoreMiddleware using HashiCorp Vault KMS.
// Uses envelope encryption where each blob has its own DEK encrypted with the Vault master key.
// vaultAddr: e.g. "http://127.0.0.1:8200"
// token: Vault token
// keyURI: e.g. "hcvault://my-keyring/keys/my-key"
func NewWithHCVault(vaultAddr, token, keyURI string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	// Register the Vault KMS client with Tink
	kmsClient, err := hcvault.NewClient(vaultAddr, nil, token)
	if err != nil {
		return nil, err
	}

	// Get AEAD primitive from Vault
	vaultAEAD, err := kmsClient.GetAEAD(keyURI)
	if err != nil {
		return nil, err
	}

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     vaultAEAD,
		innerBlobStore: innerBlobStore,
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

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     awsAEAD,
		innerBlobStore: innerBlobStore,
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

	// Encrypt the DEK with master AEAD
	encryptedDEK, err := mw.masterAEAD.Encrypt(dek, blobId[:])
	if err != nil {
		return err
	}

	// Create a pipe for the combined data (encrypted DEK + encrypted blob)
	encryptReader, encryptWriter := io.Pipe()
	go func() {
		defer encryptWriter.Close()

		// First write the length of encrypted DEK (4 bytes big-endian)
		dekLen := uint32(len(encryptedDEK))
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, dekLen)
		if _, err := encryptWriter.Write(lengthBytes); err != nil {
			encryptWriter.CloseWithError(err)
			return
		}

		// Write the encrypted DEK
		if _, err := encryptWriter.Write(encryptedDEK); err != nil {
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

	// Read the DEK length (4 bytes)
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(rc, lengthBytes); err != nil {
		rc.Close()
		return nil, err
	}

	dekLen := binary.BigEndian.Uint32(lengthBytes)

	// Read the encrypted DEK
	encryptedDEK := make([]byte, dekLen)
	if _, err := io.ReadFull(rc, encryptedDEK); err != nil {
		rc.Close()
		return nil, err
	}

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

	// Return a composite ReadCloser that closes both the decrypting reader and the original reader
	return &compositeReadCloser{
		Reader: decryptingReader,
		closer: rc,
	}, nil
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
