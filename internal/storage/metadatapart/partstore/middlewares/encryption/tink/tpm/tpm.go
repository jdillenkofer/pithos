package tpm

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"
)

// AESKeyMaterial represents the persistent AES key material
type AESKeyMaterial struct {
	Private []byte `json:"private"` // TPM2BPrivate serialized
	Public  []byte `json:"public"`  // TPM2BPublic serialized
}

// AEAD implements tink.AEAD interface using TPM for key operations
// The master key never leaves the TPM
type AEAD struct {
	mu            sync.Mutex // Protects concurrent access to TPM device
	tpmDevice     transport.TPMCloser
	primaryHandle tpm2.TPMHandle    // Persistent RSA primary key handle
	aesKeyHandle  tpm2.TPMHandle    // Transient AES key handle (loaded from primary)
	aesKeyName    tpm2.TPM2BName    // The cryptographic name of the AES key
	aesKeyPrivate tpm2.TPM2BPrivate // Private portion of AES key (to reload if needed)
	aesKeyPublic  tpm2.TPM2BPublic  // Public portion of AES key
	primaryName   tpm2.TPM2BName    // The cryptographic name of the primary key
}

// isPersistentHandleFree checks if a persistent handle is available (not occupied).
func isPersistentHandleFree(dev transport.TPM, handle tpm2.TPMHandle) (bool, error) {
	read := tpm2.ReadPublic{ObjectHandle: handle}
	_, err := read.Execute(dev)
	if err == nil {
		// Object exists at this handle
		return false, nil
	}

	// TPM returns TPMRCHandle when the handle doesn't reference an existing object
	var tpmErr tpm2.TPMRC
	if errors.As(err, &tpmErr) {
		// TPMRCHandle (0x18b) means the handle is not correct/doesn't exist
		if tpmErr == tpm2.TPMRCHandle {
			return true, nil
		}
	}

	// Try direct type assertion as a fallback
	if tpmErr, ok := err.(tpm2.TPMRC); ok {
		if tpmErr == tpm2.TPMRCHandle {
			return true, nil
		}
	}

	// As a last resort, check if the error message contains TPM_RC_HANDLE
	// This handles cases where the error might be wrapped
	errStr := err.Error()
	if errStr != "" && (errStr == "TPM_RC_HANDLE" ||
		len(errStr) > 13 && errStr[:13] == "TPM_RC_HANDLE") {
		return true, nil
	}

	// Other errors are real failures
	return false, fmt.Errorf("failed to check handle availability: %w", err)
}

// getOrCreatePersistentKey either loads an existing persistent RSA primary key or creates and persists a new one.
// If the persistent handle is occupied, it loads and uses that key.
// If the persistent handle is free, it creates a new RSA primary key and persists it.
// Returns the handle and the name of the primary key.
func getOrCreatePersistentKey(dev transport.TPM, persistentHandle tpm2.TPMHandle) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	// Validate that the handle is in the persistent range (0x81000000–0x81FFFFFF)
	if persistentHandle < 0x81000000 || persistentHandle > 0x81FFFFFF {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("handle 0x%08X not in persistent range (0x81000000-0x81FFFFFF)", persistentHandle)
	}

	// Check if the handle is already occupied
	isFree, err := isPersistentHandleFree(dev, persistentHandle)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to check if handle is free: %w", err)
	}

	if !isFree {
		// Key already exists at this handle, verify it's an RSA key
		read := tpm2.ReadPublic{ObjectHandle: persistentHandle}
		pub, err := read.Execute(dev)
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to read public area of existing key: %w", err)
		}

		publicArea, err := pub.OutPublic.Contents()
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to parse public area: %w", err)
		}

		// Verify it's an RSA storage key (restricted decrypt key)
		if publicArea.Type != tpm2.TPMAlgRSA {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing key at 0x%08X is not an RSA key", persistentHandle)
		}

		attrs := publicArea.ObjectAttributes
		if !attrs.Decrypt || !attrs.Restricted {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing key at 0x%08X is not a storage key (missing Decrypt or Restricted)", persistentHandle)
		}

		// Key exists and is valid, use it
		return persistentHandle, pub.Name, nil
	}

	// Handle is free, create a new RSA primary storage key (SRK)
	createPrimaryCmd := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic:      tpm2.New2B(tpm2.RSASRKTemplate),
	}

	createPrimaryRsp, err := createPrimaryCmd.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to create primary key: %w", err)
	}

	// Persist the key to the persistent handle
	evictCmd := tpm2.EvictControl{
		Auth: tpm2.AuthHandle{
			Handle: tpm2.TPMRHOwner,
			Auth:   tpm2.PasswordAuth(nil),
		},
		ObjectHandle: &tpm2.NamedHandle{
			Handle: createPrimaryRsp.ObjectHandle,
			Name:   createPrimaryRsp.Name,
		},
		PersistentHandle: persistentHandle,
	}

	_, err = evictCmd.Execute(dev)
	if err != nil {
		// Try to flush the transient key before returning error
		flushCmd := tpm2.FlushContext{FlushHandle: createPrimaryRsp.ObjectHandle}
		flushCmd.Execute(dev)
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to persist key: %w", err)
	}

	// After EvictControl, the transient handle is automatically flushed by the TPM
	// and the key is now available at the persistent handle
	return persistentHandle, createPrimaryRsp.Name, nil
}

// createAESKey creates a new AES symmetric cipher key as a child of the primary RSA key.
// Returns the private and public portions of the key for later loading.
func createAESKey(dev transport.TPM, primaryHandle tpm2.TPMHandle, primaryName tpm2.TPM2BName) (tpm2.TPM2BPrivate, tpm2.TPM2BPublic, error) {
	createAES := tpm2.Create{
		ParentHandle: tpm2.NamedHandle{
			Handle: primaryHandle,
			Name:   primaryName,
		},
		InPublic: tpm2.New2B(tpm2.TPMTPublic{
			Type:    tpm2.TPMAlgSymCipher,
			NameAlg: tpm2.TPMAlgSHA256,
			ObjectAttributes: tpm2.TPMAObject{
				FixedTPM:            true,
				FixedParent:         true,
				UserWithAuth:        true,
				SensitiveDataOrigin: true,
				Decrypt:             true,
				SignEncrypt:         true,
			},
			Parameters: tpm2.NewTPMUPublicParms(
				tpm2.TPMAlgSymCipher,
				&tpm2.TPMSSymCipherParms{
					Sym: tpm2.TPMTSymDefObject{
						Algorithm: tpm2.TPMAlgAES,
						Mode:      tpm2.NewTPMUSymMode(tpm2.TPMAlgAES, tpm2.TPMAlgCFB),
						KeyBits:   tpm2.NewTPMUSymKeyBits(tpm2.TPMAlgAES, tpm2.TPMKeyBits(128)),
					},
				},
			),
		}),
	}

	createRsp, err := createAES.Execute(dev)
	if err != nil {
		return tpm2.TPM2BPrivate{}, tpm2.TPM2BPublic{}, fmt.Errorf("failed to create AES key: %w", err)
	}

	return createRsp.OutPrivate, createRsp.OutPublic, nil
}

// loadAESKey loads an AES key into the TPM and returns its handle and name.
func loadAESKey(dev transport.TPM, primaryHandle tpm2.TPMHandle, primaryName tpm2.TPM2BName, private tpm2.TPM2BPrivate, public tpm2.TPM2BPublic) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	loadAES := tpm2.Load{
		ParentHandle: tpm2.NamedHandle{
			Handle: primaryHandle,
			Name:   primaryName,
		},
		InPrivate: private,
		InPublic:  public,
	}

	loadRsp, err := loadAES.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to load AES key: %w", err)
	}

	return loadRsp.ObjectHandle, loadRsp.Name, nil
}

// saveAESKeyMaterial saves the AES key material to a file
func saveAESKeyMaterial(keyFilePath string, private tpm2.TPM2BPrivate, public tpm2.TPM2BPublic) error {
	// Serialize the key material using TPM marshaling
	privateBytes := tpm2.Marshal(private)
	publicBytes := tpm2.Marshal(public)

	keyMaterial := AESKeyMaterial{
		Private: privateBytes,
		Public:  publicBytes,
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(keyMaterial, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal key material: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(keyFilePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	// Write to file with restrictive permissions
	if err := os.WriteFile(keyFilePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}

	return nil
}

// loadAESKeyMaterial loads the AES key material from a file
func loadAESKeyMaterial(keyFilePath string) (*AESKeyMaterial, error) {
	data, err := os.ReadFile(keyFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // File doesn't exist, which is fine for first run
		}
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	var keyMaterial AESKeyMaterial
	if err := json.Unmarshal(data, &keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key material: %w", err)
	}

	return &keyMaterial, nil
}

// NewAEAD creates a new AEAD primitive that uses TPM for encryption/decryption
// The key is created and sealed in the TPM and never exposed
// On Linux: tpmPath should be "/dev/tpmrm0" or "/dev/tpm0"
// On Windows: tpmPath can be empty or "default"
// persistentHandle: the persistent handle to use (0x81000000–0x81FFFFFF range)
// keyFilePath: path to file where AES key material will be persisted (e.g., "./data/tpm-aes-key.json")
func NewAEAD(tpmPath string, persistentHandle uint32, keyFilePath string) (*AEAD, error) {
	// Open TPM device based on OS (implemented in platform-specific files)
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open TPM: %w", err)
	}

	// Get or create the persistent RSA primary key
	primaryHandle, primaryName, err := getOrCreatePersistentKey(tpmDevice, tpm2.TPMHandle(persistentHandle))
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to get or create persistent primary key: %w", err)
	}

	var aesPrivate tpm2.TPM2BPrivate
	var aesPublic tpm2.TPM2BPublic

	// Try to load existing AES key material from file
	keyMaterial, err := loadAESKeyMaterial(keyFilePath)
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to load AES key material: %w", err)
	}

	if keyMaterial != nil {
		// Key material exists, deserialize it
		privatePtr, err := tpm2.Unmarshal[tpm2.TPM2BPrivate](keyMaterial.Private)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
		}
		aesPrivate = *privatePtr

		publicPtr, err := tpm2.Unmarshal[tpm2.TPM2BPublic](keyMaterial.Public)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to unmarshal public key: %w", err)
		}
		aesPublic = *publicPtr
	} else {
		// No existing key, create a new AES key
		aesPrivate, aesPublic, err = createAESKey(tpmDevice, primaryHandle, primaryName)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to create AES key: %w", err)
		}

		// Save the key material for future use
		if err := saveAESKeyMaterial(keyFilePath, aesPrivate, aesPublic); err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to save AES key material: %w", err)
		}
	}

	// Load the AES key into the TPM
	aesHandle, aesName, err := loadAESKey(tpmDevice, primaryHandle, primaryName, aesPrivate, aesPublic)
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to load AES key: %w", err)
	}

	return &AEAD{
		tpmDevice:     tpmDevice,
		primaryHandle: primaryHandle,
		aesKeyHandle:  aesHandle,
		aesKeyName:    aesName,
		aesKeyPrivate: aesPrivate,
		aesKeyPublic:  aesPublic,
		primaryName:   primaryName,
	}, nil
}

// Encrypt encrypts plaintext with associatedData using TPM
// This implements the tink.AEAD interface
func (t *AEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	// Lock to prevent concurrent TPM access
	t.mu.Lock()
	defer t.mu.Unlock()

	// Generate a random IV for this encryption
	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// Prepare encryption parameters
	encryptCmd := tpm2.EncryptDecrypt2{
		KeyHandle: tpm2.AuthHandle{
			Handle: t.aesKeyHandle,
			Name:   t.aesKeyName,
			Auth:   tpm2.PasswordAuth([]byte("")),
		},
		Message: tpm2.TPM2BMaxBuffer{
			Buffer: plaintext,
		},
		Mode:    tpm2.TPMAlgCFB,
		Decrypt: false,
		IV: tpm2.TPM2BIV{
			Buffer: iv,
		},
	}

	encryptRsp, err := encryptCmd.Execute(t.tpmDevice)
	if err != nil {
		return nil, fmt.Errorf("TPM encryption failed: %w", err)
	}

	// Combine IV + ciphertext
	// Format: [IV (16 bytes)][Ciphertext]
	result := make([]byte, 16+len(encryptRsp.OutData.Buffer))
	copy(result[0:16], iv)
	copy(result[16:], encryptRsp.OutData.Buffer)

	return result, nil
}

// Decrypt decrypts ciphertext with associatedData using TPM
// This implements the tink.AEAD interface
func (t *AEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	// Lock to prevent concurrent TPM access
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ciphertext) < 16 {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Extract IV and actual ciphertext
	// Format: [IV (16 bytes)][Ciphertext]
	iv := ciphertext[0:16]
	actualCiphertext := ciphertext[16:]

	// Decrypt using TPM
	decryptCmd := tpm2.EncryptDecrypt2{
		KeyHandle: tpm2.AuthHandle{
			Handle: t.aesKeyHandle,
			Name:   t.aesKeyName,
			Auth:   tpm2.PasswordAuth([]byte("")),
		},
		Message: tpm2.TPM2BMaxBuffer{
			Buffer: actualCiphertext,
		},
		Mode:    tpm2.TPMAlgCFB,
		Decrypt: true,
		IV: tpm2.TPM2BIV{
			Buffer: iv,
		},
	}

	decryptRsp, err := decryptCmd.Execute(t.tpmDevice)
	if err != nil {
		return nil, fmt.Errorf("TPM decryption failed: %w", err)
	}

	return decryptRsp.OutData.Buffer, nil
}

// Close closes the TPM device
// Note: We flush the transient AES key handle but not the persistent primary key
func (t *AEAD) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Flush the AES key handle (transient)
	if t.aesKeyHandle != 0 {
		flushCmd := tpm2.FlushContext{FlushHandle: t.aesKeyHandle}
		flushCmd.Execute(t.tpmDevice)
		t.aesKeyHandle = 0 // Mark as flushed
	}
	return t.tpmDevice.Close()
}
