package tink

import (
	"context"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/pkg/vault"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink/tpm"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/scrypt"

	aeadsubtle "github.com/google/tink/go/aead/subtle"
	"github.com/google/tink/go/integration/awskms"
	"github.com/google/tink/go/integration/hcvault"
	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
	"github.com/google/tink/go/tink"
)

const (
	// PartHeaderVersion is the current version of the part header format
	PartHeaderVersion = 3

	// Key types for different KMS providers
	KeyTypeAWS   = "aws"
	KeyTypeVault = "vault"
	KeyTypeLocal = "local"
	KeyTypeTPM   = "tpm"

	// DefaultSegmentSize is the ciphertext segment size for new parts (128KB)
	DefaultSegmentSize = 128 * 1024
	// LegacySegmentSize is the ciphertext segment size for old parts (4KB)
	LegacySegmentSize = 4096
)

// PartHeader contains metadata about the encryption used for a part
type PartHeader struct {
	Version           int    `json:"version"`                     // Format version for future compatibility
	KeyType           string `json:"keyType"`                     // KeyTypeAWS, KeyTypeVault, or KeyTypeLocal
	KeyURI            string `json:"keyURI"`                      // Key identifier (empty for local)
	EncryptedDEK      []byte `json:"encryptedDEK"`                // The encrypted DEK
	SegmentSize       int    `json:"segmentSize,omitempty"`       // Size of ciphertext segments (defaults to 4096 if 0)
	PQEncapsulatedKey []byte `json:"pqEncapsulatedKey,omitempty"` // PQ-safe encapsulated key (ML-KEM-1024)
}

// TinkEncryptionPartStoreMiddleware uses envelope encryption where each part has its own DEK.
type TinkEncryptionPartStoreMiddleware struct {
	masterAEAD     tink.AEAD // Master key for encrypting DEKs
	innerPartStore partstore.PartStore
	// Metadata for key rotation support
	keyType string // KeyTypeAWS, KeyTypeVault, or KeyTypeLocal
	keyURI  string // Key identifier (empty for local)
	// PQ-safe encryption
	mlkemKey *mlkem.DecapsulationKey1024
	// Vault-specific configuration for key rotation and token refresh
	vaultAddr     string      // Vault address (for recreating client)
	vaultToken    string      // Vault token (for token-based auth)
	vaultRoleID   string      // Vault AppRole role ID (for AppRole auth)
	vaultSecretID string      // Vault AppRole secret ID (for AppRole auth)
	tlsConfig     *tls.Config // TLS configuration for Vault client
	// Token refresh mechanism
	tokenMutex      sync.RWMutex  // Protects token refresh operations
	tokenExpiry     time.Time     // When the current token expires
	stopRefresh     chan struct{} // Signal to stop the refresh goroutine
	refreshShutdown sync.WaitGroup
	tracer          trace.Tracer
}

// Compile-time check to ensure TinkEncryptionPartStoreMiddleware implements partstore.PartStore
var _ partstore.PartStore = (*TinkEncryptionPartStoreMiddleware)(nil)

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

// refreshVaultToken refreshes the AEAD with a new token
func (mw *TinkEncryptionPartStoreMiddleware) refreshVaultToken() error {
	mw.tokenMutex.Lock()
	defer mw.tokenMutex.Unlock()

	// Get new token using AppRole
	newToken, newExpiry, err := vault.AuthenticateWithAppRole(mw.vaultAddr, mw.vaultRoleID, mw.vaultSecretID, mw.tlsConfig)
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

	kmsClient, err := hcvault.NewClient(uriPrefix, mw.tlsConfig, newToken)
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
func (mw *TinkEncryptionPartStoreMiddleware) startTokenRefreshLoop() {
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

func deriveHybridKey(classicalKey, pqKey, salt []byte) ([]byte, error) {
	// Use HKDF to combine the keys
	// Salt is the part ID bytes
	// Info is a fixed string
	h := hkdf.New(sha256.New, append(classicalKey, pqKey...), salt, []byte("pithos-hybrid-dek"))
	derivedKey := make([]byte, 32)
	if _, err := io.ReadFull(h, derivedKey); err != nil {
		return nil, err
	}
	return derivedKey, nil
}

// NewWithHCVault creates a new TinkEncryptionPartStoreMiddleware using HashiCorp Vault KMS.
// Uses envelope encryption where each part has its own DEK encrypted with the Vault master key.
// Supports both token-based and AppRole authentication.
//
// vaultAddr: e.g. "https://vault.example.com:8200"
// token: Vault token (use empty string "" if using AppRole)
// roleID: Vault AppRole role ID (use empty string "" if using token)
// secretID: Vault AppRole secret ID (use empty string "" if using token)
// keyURI: relative path e.g. "transit/keys/my-key"
//
// Either (token) OR (roleID AND secretID) must be provided.
func NewWithHCVault(vaultAddr, token, roleID, secretID, keyURI string, innerPartStore partstore.PartStore, tlsConfig *tls.Config, mlkemKey *mlkem.DecapsulationKey1024) (partstore.PartStore, error) {
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
		actualToken, tokenExpiry, err = vault.AuthenticateWithAppRole(vaultAddr, roleID, secretID, tlsConfig)
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
	kmsClient, err := hcvault.NewClient(uriPrefix, tlsConfig, actualToken)
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

	mw := &TinkEncryptionPartStoreMiddleware{
		masterAEAD:     vaultAEAD,
		innerPartStore: innerPartStore,
		keyType:        KeyTypeVault,
		keyURI:         keyURI,
		mlkemKey:       mlkemKey,
		vaultAddr:      vaultAddr,
		vaultToken:     actualToken,
		vaultRoleID:    roleID,
		vaultSecretID:  secretID,
		tlsConfig:      tlsConfig,
		tokenExpiry:    tokenExpiry,
		stopRefresh:    make(chan struct{}),
		tracer:         otel.Tracer("internal/storage/metadatapart/partstore/middlewares/encryption/tink"),
	}

	// Start token refresh loop if using AppRole
	mw.startTokenRefreshLoop()

	return mw, nil
}

// NewWithLocalKMS creates a new TinkEncryptionPartStoreMiddleware using a local master key (KEK).
// Uses envelope encryption where each part has its own DEK encrypted with the local master key.
// kekBytes: the master key derived from a password using scrypt.
func NewWithLocalKMS(password string, innerPartStore partstore.PartStore, mlkemKey *mlkem.DecapsulationKey1024) (partstore.PartStore, error) {
	kekBytes, err := scrypt.Key([]byte(password), []byte("pithos"), 1<<16, 8, 1, 32)
	if err != nil {
		return nil, err
	}

	// Create an AEAD primitive from the provided KEK bytes
	kekAEAD, err := aeadsubtle.NewAESGCM(kekBytes)
	if err != nil {
		return nil, err
	}

	return &TinkEncryptionPartStoreMiddleware{
		masterAEAD:     kekAEAD,
		innerPartStore: innerPartStore,
		keyType:        KeyTypeLocal,
		keyURI:         "", // No URI for local keys
		mlkemKey:       mlkemKey,
		tracer:         otel.Tracer("internal/storage/metadatapart/partstore/middlewares/encryption/tink"),
	}, nil
}

// NewWithAWSKMS creates a new TinkEncryptionPartStoreMiddleware using AWS KMS.
// Uses envelope encryption where each part has its own DEK encrypted with the AWS KMS master key.
// keyURI: e.g. "aws-kms://arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
// region: AWS region (e.g. "us-east-1")
func NewWithAWSKMS(keyURI, region string, innerPartStore partstore.PartStore, mlkemKey *mlkem.DecapsulationKey1024) (partstore.PartStore, error) {
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

	return &TinkEncryptionPartStoreMiddleware{
		masterAEAD:     awsAEAD,
		innerPartStore: innerPartStore,
		keyType:        KeyTypeAWS,
		keyURI:         keyURI,
		mlkemKey:       mlkemKey,
		tracer:         otel.Tracer("internal/storage/metadatapart/partstore/middlewares/encryption/tink"),
	}, nil
}

// NewWithTPM creates a new TinkEncryptionPartStoreMiddleware using TPM for key management.
// Uses envelope encryption where each part has its own DEK encrypted with the TPM master key.
// The master key never leaves the TPM hardware.
// tpmPath: path to TPM device (e.g. "/dev/tpmrm0" or "/dev/tpm0")
// persistentHandle: persistent handle for the TPM key (0x81000000–0x81FFFFFF)
// keyFilePath: path to file where AES key material will be persisted (e.g., "./data/tpm-aes-key.json")
// keyAlgorithm: the primary key algorithm, defaults to ECC P-256 if empty
// symmetricAlgorithm: the symmetric key algorithm (e.g. "aes-128", "aes-256")
// hmacAlgorithm: the HMAC algorithm ("sha256", "sha384", "sha512"), defaults to "sha256"
func NewWithTPM(tpmPath string, persistentHandle uint32, keyFilePath string, keyAlgorithm string, symmetricAlgorithm string, hmacAlgorithm string, innerPartStore partstore.PartStore, mlkemKey *mlkem.DecapsulationKey1024) (partstore.PartStore, error) {
	// Create TPM AEAD
	tpmAEAD, err := tpm.NewAEAD(tpmPath, persistentHandle, keyFilePath, keyAlgorithm, symmetricAlgorithm, hmacAlgorithm)
	if err != nil {
		return nil, err
	}

	// Test key availability
	if err := testKeyAvailability(tpmAEAD, KeyTypeTPM); err != nil {
		tpmAEAD.Close()
		return nil, err
	}

	return &TinkEncryptionPartStoreMiddleware{
		masterAEAD:     tpmAEAD,
		innerPartStore: innerPartStore,
		keyType:        KeyTypeTPM,
		keyURI:         fmt.Sprintf("tpm://%s/0x%08X", tpmPath, persistentHandle),
		mlkemKey:       mlkemKey,
		tracer:         otel.Tracer("internal/storage/metadatapart/partstore/middlewares/encryption/tink"),
	}, nil
}

func (mw *TinkEncryptionPartStoreMiddleware) Start(ctx context.Context) error {
	return mw.innerPartStore.Start(ctx)
}

func (mw *TinkEncryptionPartStoreMiddleware) Stop(ctx context.Context) error {
	// Stop token refresh goroutine if running
	if mw.stopRefresh != nil {
		close(mw.stopRefresh)
		mw.refreshShutdown.Wait()
	}

	// If using TPM, close the TPM device
	if mw.keyType == KeyTypeTPM {
		if tpmAEAD, ok := mw.masterAEAD.(*tpm.AEAD); ok {
			if err := tpmAEAD.Close(); err != nil {
				// Log error but continue with stopping inner part store
				fmt.Printf("Warning: failed to close TPM AEAD: %v\n", err)
			}
		}
	}
	return mw.innerPartStore.Stop(ctx)
}

func (mw *TinkEncryptionPartStoreMiddleware) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionPartStoreMiddleware.PutPart")
	defer span.End()

	// Generate a new 32-byte classical DEK for this part
	classicalDEK := make([]byte, 32)
	if _, err := rand.Read(classicalDEK); err != nil {
		return err
	}

	var pqSharedSecret []byte
	var pqEncapsulatedKey []byte
	if mw.mlkemKey != nil {
		pqSharedSecret, pqEncapsulatedKey = mw.mlkemKey.EncapsulationKey().Encapsulate()
	}

	finalDEK := classicalDEK
	if pqSharedSecret != nil {
		var err error
		finalDEK, err = deriveHybridKey(classicalDEK, pqSharedSecret, partId.Bytes())
		if err != nil {
			return err
		}
	}

	segmentSize := DefaultSegmentSize

	// Create streaming AEAD with the final (possibly hybrid) DEK
	dekStreamingAEAD, err := streamingaeadsubtle.NewAESGCMHKDF(finalDEK, "SHA256", 32, segmentSize, 0)
	if err != nil {
		return err
	}

	// Encrypt the classical DEK with the master AEAD
	encryptedDEK, err := mw.masterAEAD.Encrypt(classicalDEK, partId.Bytes())
	if err != nil {
		return err
	}

	// Create header with key metadata and encrypted DEK
	header := PartHeader{
		Version:           PartHeaderVersion,
		KeyType:           mw.keyType,
		KeyURI:            mw.keyURI,
		EncryptedDEK:      encryptedDEK,
		SegmentSize:       segmentSize,
		PQEncapsulatedKey: pqEncapsulatedKey,
	}

	// Serialize header to JSON
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return err
	}

	// Create a pipe for the combined data (header + encrypted part)
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		// Make sure we always close writer
		defer pipeWriter.Close()

		// --- 1. Write header (plaintext) into the same pipe ---
		// Write the header length + header bytes (4 bytes big-endian length + header)
		buf := make([]byte, 4+len(headerBytes))
		binary.BigEndian.PutUint32(buf[:4], uint32(len(headerBytes)))
		copy(buf[4:], headerBytes)
		if _, err := pipeWriter.Write(buf); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		// --- 2. Wrap SAME PipeWriter in encryption writer ---
		streamWriter, err := dekStreamingAEAD.NewEncryptingWriter(
			pipeWriter,
			partId.Bytes(),
		)
		if err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		// --- 3. Stream body → EncryptingWriter → PipeWriter ---
		if _, err := ioutils.Copy(streamWriter, reader); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		// --- 4. Finalize encryption ---
		if err := streamWriter.Close(); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

	}()

	return mw.innerPartStore.PutPart(ctx, tx, partId, pipeReader)
}

func (mw *TinkEncryptionPartStoreMiddleware) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionPartStoreMiddleware.GetPart")
	defer span.End()

	rc, err := mw.innerPartStore.GetPart(ctx, tx, partId)
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

		var header PartHeader
		if err := json.Unmarshal(headerBytes, &header); err != nil {
			rc.Close()
			return nil, err
		}

		if header.Version > PartHeaderVersion || header.Version < 1 {
			rc.Close()
			return nil, fmt.Errorf("unsupported part header version: %d", header.Version)
		}

		// Decrypt the classical DEK with master AEAD
		classicalDEK, err := mw.masterAEAD.Decrypt(header.EncryptedDEK, partId.Bytes())
		if err != nil {
			rc.Close()
			return nil, err
		}

		finalDEK := classicalDEK
		if len(header.PQEncapsulatedKey) > 0 {
			if mw.mlkemKey == nil {
				rc.Close()
				return nil, fmt.Errorf("part is PQ-encrypted but no ML-KEM key is configured")
			}
			pqSharedSecret, err := mw.mlkemKey.Decapsulate(header.PQEncapsulatedKey)
			if err != nil {
				rc.Close()
				return nil, fmt.Errorf("failed to decapsulate PQ key: %w", err)
			}
			finalDEK, err = deriveHybridKey(classicalDEK, pqSharedSecret, partId.Bytes())
			if err != nil {
				rc.Close()
				return nil, err
			}
		}

		segmentSize := header.SegmentSize
		if segmentSize == 0 {
			segmentSize = LegacySegmentSize
		}

		// Create streaming AEAD with the final DEK
		dekStreamingAEAD, err := streamingaeadsubtle.NewAESGCMHKDF(finalDEK, "SHA256", 32, segmentSize, 0)
		if err != nil {
			rc.Close()
			return nil, err
		}

		// Create a decrypting reader for the remaining data
		decryptReader, err := dekStreamingAEAD.NewDecryptingReader(rc, partId.Bytes())
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

func (mw *TinkEncryptionPartStoreMiddleware) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionPartStoreMiddleware.GetPartIds")
	defer span.End()

	return mw.innerPartStore.GetPartIds(ctx, tx)
}

func (mw *TinkEncryptionPartStoreMiddleware) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	ctx, span := mw.tracer.Start(ctx, "TinkEncryptionPartStoreMiddleware.DeletePart")
	defer span.End()

	return mw.innerPartStore.DeletePart(ctx, tx, partId)
}
