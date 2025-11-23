package tink

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/tink/tpm"
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
	KeyTypeTPM   = "tpm"
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
	// Vault-specific configuration for key rotation and token refresh
	vaultAddr     string // Vault address (for recreating client)
	vaultToken    string // Vault token (for token-based auth)
	vaultRoleID   string // Vault AppRole role ID (for AppRole auth)
	vaultSecretID string // Vault AppRole secret ID (for AppRole auth)
	// Token refresh mechanism
	tokenMutex      sync.RWMutex  // Protects token refresh operations
	tokenExpiry     time.Time     // When the current token expires
	stopRefresh     chan struct{} // Signal to stop the refresh goroutine
	refreshShutdown sync.WaitGroup
	tracer          trace.Tracer
}

// Compile-time check to ensure TinkEncryptionBlobStoreMiddleware implements blobstore.BlobStore
var _ blobstore.BlobStore = (*TinkEncryptionBlobStoreMiddleware)(nil)

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

// vaultAuthResponse represents the response from Vault authentication
type vaultAuthResponse struct {
	Auth struct {
		ClientToken   string `json:"client_token"`
		LeaseDuration int    `json:"lease_duration"`
		Renewable     bool   `json:"renewable"`
	} `json:"auth"`
}

// authenticateWithAppRole authenticates with Vault using AppRole and returns the client token and expiry
func authenticateWithAppRole(vaultAddr, roleID, secretID string) (string, time.Time, error) {
	// Convert vault address to proper HTTP URL
	httpAddr := vaultAddr
	if strings.HasPrefix(vaultAddr, "hcvault://") {
		httpAddr = "https://" + vaultAddr[len("hcvault://"):]
	} else if !strings.HasPrefix(vaultAddr, "http://") && !strings.HasPrefix(vaultAddr, "https://") {
		httpAddr = "https://" + vaultAddr
	}

	// Prepare the AppRole login request
	loginData := map[string]string{
		"role_id":   roleID,
		"secret_id": secretID,
	}
	loginJSON, err := json.Marshal(loginData)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to marshal AppRole login data: %w", err)
	}

	// Make the login request
	loginURL := strings.TrimRight(httpAddr, "/") + "/v1/auth/approle/login"
	resp, err := http.Post(loginURL, "application/json", bytes.NewReader(loginJSON))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to authenticate with AppRole: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("AppRole authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var authResp vaultAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to decode AppRole response: %w", err)
	}

	if authResp.Auth.ClientToken == "" {
		return "", time.Time{}, fmt.Errorf("AppRole authentication returned empty token")
	}

	// Calculate token expiry (refresh at 80% of lease duration to be safe)
	leaseDuration := time.Duration(authResp.Auth.LeaseDuration) * time.Second
	refreshBuffer := leaseDuration * 20 / 100 // 20% buffer
	expiry := time.Now().Add(leaseDuration - refreshBuffer)

	return authResp.Auth.ClientToken, expiry, nil
}

// refreshVaultToken refreshes the AEAD with a new token
func (mw *TinkEncryptionBlobStoreMiddleware) refreshVaultToken() error {
	mw.tokenMutex.Lock()
	defer mw.tokenMutex.Unlock()

	// Get new token using AppRole
	newToken, newExpiry, err := authenticateWithAppRole(mw.vaultAddr, mw.vaultRoleID, mw.vaultSecretID)
	if err != nil {
		return fmt.Errorf("failed to refresh Vault token: %w", err)
	}

	// Create new Vault client with the new token
	var uriPrefix string
	if strings.HasPrefix(mw.vaultAddr, "https://") {
		uriPrefix = "hcvault://" + mw.vaultAddr[len("https://"):]
	} else if strings.HasPrefix(mw.vaultAddr, "http://") {
		uriPrefix = "hcvault://" + mw.vaultAddr[len("http://"):]
	} else {
		uriPrefix = "hcvault://" + mw.vaultAddr
	}

	fullKeyURI := uriPrefix + "/" + strings.TrimPrefix(mw.keyURI, "/")

	kmsClient, err := hcvault.NewClient(uriPrefix, nil, newToken)
	if err != nil {
		return fmt.Errorf("failed to create new Vault client: %w", err)
	}

	newAEAD, err := kmsClient.GetAEAD(fullKeyURI)
	if err != nil {
		return fmt.Errorf("failed to get AEAD with new token: %w", err)
	}

	// Test the new key
	if err := testKeyAvailability(newAEAD, KeyTypeVault); err != nil {
		return fmt.Errorf("new token validation failed: %w", err)
	}

	// Update the AEAD and token
	mw.masterAEAD = newAEAD
	mw.vaultToken = newToken
	mw.tokenExpiry = newExpiry

	return nil
}

// startTokenRefreshLoop starts a background goroutine that refreshes the Vault token before it expires
func (mw *TinkEncryptionBlobStoreMiddleware) startTokenRefreshLoop() {
	if mw.vaultRoleID == "" || mw.vaultSecretID == "" {
		// Not using AppRole, no need to refresh
		return
	}

	mw.refreshShutdown.Add(1)
	go func() {
		defer mw.refreshShutdown.Done()

		ticker := time.NewTicker(1 * time.Minute) // Check every minute
		defer ticker.Stop()

		for {
			select {
			case <-mw.stopRefresh:
				return
			case <-ticker.C:
				mw.tokenMutex.RLock()
				needsRefresh := time.Now().After(mw.tokenExpiry)
				mw.tokenMutex.RUnlock()

				if needsRefresh {
					if err := mw.refreshVaultToken(); err != nil {
						fmt.Printf("Warning: failed to refresh Vault token: %v\n", err)
					}
				}
			}
		}
	}()
}

// NewWithHCVault creates a new TinkEncryptionBlobStoreMiddleware using HashiCorp Vault KMS.
// Uses envelope encryption where each blob has its own DEK encrypted with the Vault master key.
// Supports both token-based and AppRole authentication.
//
// vaultAddr: e.g. "https://vault.example.com:8200"
// token: Vault token (use empty string "" if using AppRole)
// roleID: Vault AppRole role ID (use empty string "" if using token)
// secretID: Vault AppRole secret ID (use empty string "" if using token)
// keyURI: relative path e.g. "transit/keys/my-key"
//
// Either (token) OR (roleID AND secretID) must be provided.
func NewWithHCVault(vaultAddr, token, roleID, secretID, keyURI string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	// Validate authentication parameters
	hasToken := token != ""
	hasAppRole := roleID != "" && secretID != ""

	if !hasToken && !hasAppRole {
		return nil, fmt.Errorf("either vaultToken or (vaultRoleId and vaultSecretId) must be provided")
	}

	if hasToken && hasAppRole {
		return nil, fmt.Errorf("cannot use both vaultToken and AppRole authentication - choose one method")
	}

	// If using AppRole, authenticate to get initial token
	var actualToken string
	var tokenExpiry time.Time
	if hasAppRole {
		var err error
		actualToken, tokenExpiry, err = authenticateWithAppRole(vaultAddr, roleID, secretID)
		if err != nil {
			return nil, fmt.Errorf("AppRole authentication failed: %w", err)
		}
	} else {
		actualToken = token
		// For static tokens, we don't have an expiry (set far in future)
		tokenExpiry = time.Now().Add(100 * 365 * 24 * time.Hour)
	}

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
	kmsClient, err := hcvault.NewClient(uriPrefix, nil, actualToken)
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

	mw := &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     vaultAEAD,
		innerBlobStore: innerBlobStore,
		keyType:        KeyTypeVault,
		keyURI:         keyURI,
		vaultAddr:      vaultAddr,
		vaultToken:     actualToken,
		vaultRoleID:    roleID,
		vaultSecretID:  secretID,
		tokenExpiry:    tokenExpiry,
		stopRefresh:    make(chan struct{}),
		tracer:         otel.Tracer("internal/storage/metadatablob/blobstore/middlewares/encryption/tink"),
	}

	// Start token refresh loop if using AppRole
	mw.startTokenRefreshLoop()

	return mw, nil
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
		tracer:         otel.Tracer("internal/storage/metadatablob/blobstore/middlewares/encryption/tink"),
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
		tracer:         otel.Tracer("internal/storage/metadatablob/blobstore/middlewares/encryption/tink"),
	}, nil
}

// NewWithTPM creates a new TinkEncryptionBlobStoreMiddleware using TPM for key management.
// Uses envelope encryption where each blob has its own DEK encrypted with the TPM master key.
// The master key never leaves the TPM hardware.
// tpmPath: path to TPM device (e.g. "/dev/tpmrm0" or "/dev/tpm0")
// persistentHandle: persistent handle for the TPM key (0x81000000–0x81FFFFFF)
// keyFilePath: path to file where AES key material will be persisted (e.g., "./data/tpm-aes-key.json")
func NewWithTPM(tpmPath string, persistentHandle uint32, keyFilePath string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	// Create TPM AEAD
	tpmAEAD, err := tpm.NewAEAD(tpmPath, persistentHandle, keyFilePath)
	if err != nil {
		return nil, err
	}

	// Test key availability
	if err := testKeyAvailability(tpmAEAD, KeyTypeTPM); err != nil {
		tpmAEAD.Close()
		return nil, err
	}

	return &TinkEncryptionBlobStoreMiddleware{
		masterAEAD:     tpmAEAD,
		innerBlobStore: innerBlobStore,
		keyType:        KeyTypeTPM,
		keyURI:         fmt.Sprintf("tpm://%s/0x%08X", tpmPath, persistentHandle),
		tracer:         otel.Tracer("internal/storage/metadatablob/blobstore/middlewares/encryption/tink"),
	}, nil
}

func (mw *TinkEncryptionBlobStoreMiddleware) Start(ctx context.Context) error {
	return mw.innerBlobStore.Start(ctx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) Stop(ctx context.Context) error {
	// Stop token refresh goroutine if running
	if mw.stopRefresh != nil {
		close(mw.stopRefresh)
		mw.refreshShutdown.Wait()
	}

	// If using TPM, close the TPM device
	if mw.keyType == KeyTypeTPM {
		if tpmAEAD, ok := mw.masterAEAD.(*tpm.AEAD); ok {
			if err := tpmAEAD.Close(); err != nil {
				// Log error but continue with stopping inner blob store
				fmt.Printf("Warning: failed to close TPM AEAD: %v\n", err)
			}
		}
	}
	return mw.innerBlobStore.Stop(ctx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionBlobStoreMiddleware.PutBlob")
	defer span.End()

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
	encryptedDEK, err := mw.masterAEAD.Encrypt(dek, blobId.Bytes())
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
		streamWriter, err := dekStreamingAEAD.NewEncryptingWriter(encryptWriter, blobId.Bytes())
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
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionBlobStoreMiddleware.GetBlob")
	defer span.End()

	rc, err := mw.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	// Use lazy initialization to defer header parsing and DEK decryption until first read
	// This allows streaming to start immediately without blocking on KMS/Vault operations
	return ioutils.NewLazyReadCloser(func() (io.ReadCloser, error) {
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

		// Decrypt the DEK with master AEAD
		dek, err := mw.masterAEAD.Decrypt(header.EncryptedDEK, blobId.Bytes())
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
		decryptReader, err := dekStreamingAEAD.NewDecryptingReader(rc, blobId.Bytes())
		if err != nil {
			rc.Close()
			return nil, err
		}

		// Return a composite reader that wraps the decrypt reader with the underlying closer
		return &compositeReadCloser{decryptReader, rc}, nil
	}), nil
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
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionBlobStoreMiddleware.GetBlobIds")
	defer span.End()

	return mw.innerBlobStore.GetBlobIds(ctx, tx)
}

func (mw *TinkEncryptionBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionBlobStoreMiddleware.DeleteBlob")
	defer span.End()

	return mw.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
