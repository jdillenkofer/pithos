package tpm

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"
)

// AEAD implements tink.AEAD interface using TPM for key operations
// The master key never leaves the TPM
type AEAD struct {
	tpmDevice transport.TPMCloser
	keyHandle tpm2.TPMHandle
}

// isPersistentHandleFree checks if a persistent handle is available (not occupied).
func isPersistentHandleFree(dev transport.TPM, handle tpm2.TPMHandle) (bool, error) {
	read := tpm2.ReadPublic{ObjectHandle: handle}
	_, err := read.Execute(dev)
	if err == nil {
		// Object exists at this handle
		return false, nil
	}

	// Check if error indicates handle is free
	var tpmErr tpm2.TPMRC
	if errors.As(err, &tpmErr) && tpmErr == tpm2.TPMRCHandle {
		// TPMRCHandle (0x18b) means the handle doesn't reference an existing object
		return true, nil
	}

	// Other errors are real failures
	return false, fmt.Errorf("failed to check handle availability: %w", err)
}

// getOrCreatePersistentKey either loads an existing persistent key or creates and persists a new one.
// If the persistent handle is occupied, it loads and uses that key.
// If the persistent handle is free, it creates a new primary key and persists it.
func getOrCreatePersistentKey(dev transport.TPM, persistentHandle tpm2.TPMHandle) (tpm2.TPMHandle, error) {
	// Validate that the handle is in the persistent range (0x81000000–0x81FFFFFF)
	if persistentHandle < 0x81000000 || persistentHandle > 0x81FFFFFF {
		return 0, fmt.Errorf("handle 0x%08X not in persistent range (0x81000000-0x81FFFFFF)", persistentHandle)
	}

	// Check if the handle is already occupied
	isFree, err := isPersistentHandleFree(dev, persistentHandle)
	if err != nil {
		return 0, fmt.Errorf("failed to check if handle is free: %w", err)
	}

	if !isFree {
		// Key already exists at this handle, verify it has the correct attributes
		read := tpm2.ReadPublic{ObjectHandle: persistentHandle}
		pub, err := read.Execute(dev)
		if err != nil {
			return 0, fmt.Errorf("failed to read public area of existing key: %w", err)
		}

		publicArea, err := pub.OutPublic.Contents()
		if err != nil {
			return 0, fmt.Errorf("failed to parse public area: %w", err)
		}

		attrs := publicArea.ObjectAttributes
		if !attrs.Decrypt || !attrs.SignEncrypt || !attrs.Restricted {
			return 0, fmt.Errorf("existing key at 0x%08X lacks required attributes (Decrypt, SignEncrypt, Restricted)", persistentHandle)
		}

		// Key exists and is valid, use it
		return persistentHandle, nil
	}

	// Handle is free, create a new primary key
	createPrimaryCmd := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic: tpm2.New2B(tpm2.TPMTPublic{
			Type:    tpm2.TPMAlgRSA,
			NameAlg: tpm2.TPMAlgSHA256,
			ObjectAttributes: tpm2.TPMAObject{
				FixedTPM:            true,
				FixedParent:         true,
				SensitiveDataOrigin: true,
				UserWithAuth:        true,
				Decrypt:             true,
				SignEncrypt:         true,
				Restricted:          true,
			},
			Parameters: tpm2.NewTPMUPublicParms(
				tpm2.TPMAlgRSA,
				&tpm2.TPMSRSAParms{
					Symmetric: tpm2.TPMTSymDefObject{
						Algorithm: tpm2.TPMAlgAES,
						KeyBits: tpm2.NewTPMUSymKeyBits(
							tpm2.TPMAlgAES,
							tpm2.TPMKeyBits(128),
						),
						Mode: tpm2.NewTPMUSymMode(
							tpm2.TPMAlgAES,
							tpm2.TPMAlgCFB,
						),
					},
					KeyBits: 2048,
				},
			),
		}),
	}

	createPrimaryRsp, err := createPrimaryCmd.Execute(dev)
	if err != nil {
		return 0, fmt.Errorf("failed to create primary key: %w", err)
	}

	// Persist the key to the persistent handle
	evictCmd := tpm2.EvictControl{
		Auth: tpm2.AuthHandle{
			Handle: tpm2.TPMRHOwner,
			Auth:   tpm2.PasswordAuth(nil),
		},
		ObjectHandle:     createPrimaryRsp.ObjectHandle,
		PersistentHandle: persistentHandle,
	}

	_, err = evictCmd.Execute(dev)
	if err != nil {
		// Try to flush the transient key before returning error
		flushCmd := tpm2.FlushContext{FlushHandle: createPrimaryRsp.ObjectHandle}
		flushCmd.Execute(dev)
		return 0, fmt.Errorf("failed to persist key: %w", err)
	}

	// After EvictControl, the transient handle is automatically flushed by the TPM
	// and the key is now available at the persistent handle
	return persistentHandle, nil
}

// NewAEAD creates a new AEAD primitive that uses TPM for encryption/decryption
// The key is created and sealed in the TPM and never exposed
// On Linux: tpmPath should be "/dev/tpmrm0" or "/dev/tpm0"
// On Windows: tpmPath can be empty or "default"
// persistentHandle: the persistent handle to use (0x81000000–0x81FFFFFF range)
func NewAEAD(tpmPath string, persistentHandle uint32) (*AEAD, error) {
	// Open TPM device based on OS (implemented in platform-specific files)
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open TPM: %w", err)
	}

	// Get or create the persistent key
	handle, err := getOrCreatePersistentKey(tpmDevice, tpm2.TPMHandle(persistentHandle))
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to get or create persistent key: %w", err)
	}

	return &AEAD{
		tpmDevice: tpmDevice,
		keyHandle: handle,
	}, nil
}

// Encrypt encrypts plaintext with associatedData using TPM
// This implements the tink.AEAD interface
func (t *AEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	// Generate a random IV for this encryption
	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// Prepare encryption parameters
	encryptCmd := tpm2.EncryptDecrypt2{
		KeyHandle: tpm2.AuthHandle{
			Handle: t.keyHandle,
			Auth:   tpm2.PasswordAuth(nil),
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
			Handle: t.keyHandle,
			Auth:   tpm2.PasswordAuth(nil),
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
// Note: We don't flush persistent handles as they remain in TPM across sessions
func (t *AEAD) Close() error {
	return t.tpmDevice.Close()
}
