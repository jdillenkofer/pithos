package tpm

import (
	"crypto/rand"
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

// NewAEAD creates a new AEAD primitive that uses TPM for encryption/decryption
// The key is created and sealed in the TPM and never exposed
// On Linux: tpmPath should be "/dev/tpmrm0" or "/dev/tpm0"
// On Windows: tpmPath can be empty or "default"
func NewAEAD(tpmPath string) (*AEAD, tpm2.TPMHandle, error) {
	// Open TPM device based on OS (implemented in platform-specific files)
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open TPM: %w", err)
	}

	// Create a primary key in the TPM
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

	createPrimaryRsp, err := createPrimaryCmd.Execute(tpmDevice)
	if err != nil {
		tpmDevice.Close()
		return nil, 0, fmt.Errorf("failed to create primary key: %w", err)
	}

	return &AEAD{
		tpmDevice: tpmDevice,
		keyHandle: createPrimaryRsp.ObjectHandle,
	}, createPrimaryRsp.ObjectHandle, nil
}

// NewTPMAEADFromHandle creates a TPMAEAD from an existing TPM key handle
// On Linux: tpmPath should be "/dev/tpmrm0" or "/dev/tpm0"
// On Windows: tpmPath is ignored (uses TBS)
func NewTPMAEADFromHandle(tpmPath string, keyHandle tpm2.TPMHandle) (*AEAD, error) {
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open TPM: %w", err)
	}

	return &AEAD{
		tpmDevice: tpmDevice,
		keyHandle: keyHandle,
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

// Close closes the TPM device and flushes the key handle
func (t *AEAD) Close() error {
	// Flush the key handle from TPM
	flushCmd := tpm2.FlushContext{
		FlushHandle: t.keyHandle,
	}

	if _, err := flushCmd.Execute(t.tpmDevice); err != nil {
		// Log error but continue to close device
		fmt.Printf("Warning: failed to flush TPM handle: %v\n", err)
	}

	return t.tpmDevice.Close()
}
