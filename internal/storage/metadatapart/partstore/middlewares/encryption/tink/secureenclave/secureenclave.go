package secureenclave

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/hkdf"
)

// AEAD implements tink.AEAD interface using macOS Secure Enclave for key operations
// Uses ECIES (Elliptic Curve Integrated Encryption Scheme) pattern:
// - Master EC P-256 key pair stored in Secure Enclave (private key never leaves hardware)
// - Encryption: Generate ephemeral EC key, ECDH with SE public key, derive AES key via HKDF, AES-GCM encrypt
// - Decryption: SE performs ECDH with ephemeral public key, derive AES key, AES-GCM decrypt
type AEAD struct {
	mu       sync.Mutex
	keyLabel string
	keyRef   secKeyRef // Platform-specific reference to the Secure Enclave key
}

// ciphertext format:
// [ephemeral public key length (2 bytes)][ephemeral public key][nonce (12 bytes)][ciphertext + tag]
const (
	nonceSize           = 12
	pubKeyLengthSize    = 2
	minCiphertextLength = pubKeyLengthSize + 65 + nonceSize + 16 // 2 + uncompressed P256 key + nonce + AES-GCM tag
)

// NewAEAD creates a new AEAD primitive that uses macOS Secure Enclave for key management.
// The P-256 private key is generated and stored in the Secure Enclave; it never leaves the hardware.
// keyLabel: A unique identifier for the key in the Keychain (e.g., "pithos-master-key")
func NewAEAD(keyLabel string) (*AEAD, error) {
	keyRef, err := getOrCreateSecureEnclaveKey(keyLabel)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create Secure Enclave key: %w", err)
	}

	return &AEAD{
		keyLabel: keyLabel,
		keyRef:   keyRef,
	}, nil
}

// Encrypt encrypts plaintext with associatedData using ECIES with Secure Enclave.
// This implements the tink.AEAD interface.
func (a *AEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Get the public key from the Secure Enclave key
	publicKey, err := getPublicKey(a.keyRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get public key: %w", err)
	}

	// Generate ephemeral EC key pair
	ephemeralPrivate, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ephemeral key: %w", err)
	}
	ephemeralPublic := ephemeralPrivate.PublicKey()

	// Perform ECDH with Secure Enclave's public key
	sharedSecret, err := ephemeralPrivate.ECDH(publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to compute ECDH shared secret: %w", err)
	}

	// Derive AES key using HKDF
	aesKey, err := deriveAESKey(sharedSecret, ephemeralPublic.Bytes(), associatedData)
	if err != nil {
		return nil, fmt.Errorf("failed to derive AES key: %w", err)
	}

	// Create AES-GCM cipher
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt plaintext
	ciphertext := aead.Seal(nil, nonce, plaintext, associatedData)

	// Build output: [ephemeral pub key length][ephemeral pub key][nonce][ciphertext]
	ephemeralPubBytes := ephemeralPublic.Bytes()
	result := make([]byte, pubKeyLengthSize+len(ephemeralPubBytes)+nonceSize+len(ciphertext))

	// Write ephemeral public key length (2 bytes, big endian)
	binary.BigEndian.PutUint16(result[0:pubKeyLengthSize], uint16(len(ephemeralPubBytes)))
	offset := pubKeyLengthSize

	// Write ephemeral public key
	copy(result[offset:], ephemeralPubBytes)
	offset += len(ephemeralPubBytes)

	// Write nonce
	copy(result[offset:], nonce)
	offset += nonceSize

	// Write ciphertext
	copy(result[offset:], ciphertext)

	return result, nil
}

// Decrypt decrypts ciphertext with associatedData using ECIES with Secure Enclave.
// This implements the tink.AEAD interface.
func (a *AEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(ciphertext) < minCiphertextLength {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Parse ephemeral public key length
	ephemeralPubLen := int(binary.BigEndian.Uint16(ciphertext[0:pubKeyLengthSize]))
	offset := pubKeyLengthSize

	// Validate length
	if len(ciphertext) < pubKeyLengthSize+ephemeralPubLen+nonceSize+16 {
		return nil, fmt.Errorf("ciphertext too short for declared key length")
	}

	// Parse ephemeral public key
	ephemeralPubBytes := ciphertext[offset : offset+ephemeralPubLen]
	offset += ephemeralPubLen

	ephemeralPublic, err := ecdh.P256().NewPublicKey(ephemeralPubBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ephemeral public key: %w", err)
	}

	// Use Secure Enclave to perform ECDH
	sharedSecret, err := secureEnclaveECDH(a.keyRef, ephemeralPublic)
	if err != nil {
		return nil, fmt.Errorf("failed to perform ECDH in Secure Enclave: %w", err)
	}

	// Derive AES key using HKDF
	aesKey, err := deriveAESKey(sharedSecret, ephemeralPubBytes, associatedData)
	if err != nil {
		return nil, fmt.Errorf("failed to derive AES key: %w", err)
	}

	// Create AES-GCM cipher
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Parse nonce
	nonce := ciphertext[offset : offset+nonceSize]
	offset += nonceSize

	// Decrypt
	plaintext, err := aead.Open(nil, nonce, ciphertext[offset:], associatedData)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return plaintext, nil
}

// Close releases any resources held by the AEAD.
func (a *AEAD) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return releaseKeyRef(a.keyRef)
}

// deriveAESKey derives a 256-bit AES key from the ECDH shared secret using HKDF-SHA256.
func deriveAESKey(sharedSecret, ephemeralPubKey, info []byte) ([]byte, error) {
	// Use HKDF with SHA-256
	// Salt: ephemeral public key (provides domain separation per encryption)
	// Info: associated data (binds the key derivation to the context)
	hkdfReader := hkdf.New(sha256.New, sharedSecret, ephemeralPubKey, info)

	aesKey := make([]byte, 32) // 256-bit key
	if _, err := io.ReadFull(hkdfReader, aesKey); err != nil {
		return nil, err
	}

	return aesKey, nil
}
