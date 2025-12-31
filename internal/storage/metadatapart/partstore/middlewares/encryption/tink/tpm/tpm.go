package tpm

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpm2/transport"
)

// Key algorithm constants for TPM primary key types
const (
	// KeyAlgorithmRSA2048 specifies RSA-2048 as the primary key algorithm
	KeyAlgorithmRSA2048 = "rsa-2048"
	// KeyAlgorithmRSA4096 specifies RSA-4096 as the primary key algorithm
	KeyAlgorithmRSA4096 = "rsa-4096"
	// KeyAlgorithmECCP256 specifies ECC P-256 (NIST P-256) as the primary key algorithm
	KeyAlgorithmECCP256 = "ecc-p256"
	// KeyAlgorithmECCP384 specifies ECC P-384 (NIST P-384) as the primary key algorithm
	KeyAlgorithmECCP384 = "ecc-p384"
	// KeyAlgorithmECCP521 specifies ECC P-521 (NIST P-521) as the primary key algorithm
	KeyAlgorithmECCP521 = "ecc-p521"
	// KeyAlgorithmECCBrainpoolP256 specifies Brainpool P256r1 as the primary key algorithm
	KeyAlgorithmECCBrainpoolP256 = "ecc-brainpool-p256"
	// KeyAlgorithmECCBrainpoolP384 specifies Brainpool P384r1 as the primary key algorithm
	KeyAlgorithmECCBrainpoolP384 = "ecc-brainpool-p384"
	// KeyAlgorithmECCBrainpoolP512 specifies Brainpool P512r1 as the primary key algorithm
	KeyAlgorithmECCBrainpoolP512 = "ecc-brainpool-p512"

	// HMACAlgorithmSHA256 specifies HMAC-SHA256
	HMACAlgorithmSHA256 = "sha256"
	// HMACAlgorithmSHA384 specifies HMAC-SHA384
	HMACAlgorithmSHA384 = "sha384"
	// HMACAlgorithmSHA512 specifies HMAC-SHA512
	HMACAlgorithmSHA512 = "sha512"

	// SymmetricAlgorithmAES128 specifies AES-128
	SymmetricAlgorithmAES128 = "aes-128"
	// SymmetricAlgorithmAES256 specifies AES-256
	SymmetricAlgorithmAES256 = "aes-256"

	// EncryptedDataVersion1 is the version byte for the authenticated encryption format
	EncryptedDataVersion1 = byte(1)
)

// AESKeyMaterial represents the persistent AES key material and optional HMAC key material
type AESKeyMaterial struct {
	Private     []byte `json:"private"`               // TPM2BPrivate serialized (AES)
	Public      []byte `json:"public"`                // TPM2BPublic serialized (AES)
	HMACPrivate []byte `json:"hmacPrivate,omitempty"` // TPM2BPrivate serialized (HMAC)
	HMACPublic  []byte `json:"hmacPublic,omitempty"`  // TPM2BPublic serialized (HMAC)
}

// AEAD implements tink.AEAD interface using TPM for key operations
// The master key never leaves the TPM
type AEAD struct {
	mu               sync.Mutex // Protects concurrent access to TPM device
	tpmDevice        transport.TPMCloser
	primaryHandle    tpm2.TPMHandle    // Persistent RSA primary key handle
	aesKeyHandle     tpm2.TPMHandle    // Transient AES key handle
	aesKeyName       tpm2.TPM2BName    // The cryptographic name of the AES key
	aesKeyPrivate    tpm2.TPM2BPrivate // Private portion of AES key
	aesKeyPublic     tpm2.TPM2BPublic  // Public portion of AES key
	hmacKeyHandle    tpm2.TPMHandle    // Transient HMAC key handle
	hmacKeyName      tpm2.TPM2BName    // The cryptographic name of the HMAC key
	hmacKeyPrivate   tpm2.TPM2BPrivate // Private portion of HMAC key
	hmacKeyPublic    tpm2.TPM2BPublic  // Public portion of HMAC key
	primaryName      tpm2.TPM2BName    // The cryptographic name of the primary key
	symmetricKeySize uint16            // The symmetric key size in bits (128 or 256)
	hmacAlg          tpm2.TPMAlgID     // The HMAC algorithm
	hmacSize         int               // The size of the HMAC tag
	password         string            // Optional password for key authorization
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

// getOrCreatePersistentKey dispatches to the appropriate key creation function based on the key algorithm.
// If the persistent handle is occupied, it verifies the existing key matches the expected algorithm.
// If the persistent handle is free, it creates a new primary key with the specified algorithm.
// Returns the handle and the name of the primary key.
func getOrCreatePersistentKey(dev transport.TPM, persistentHandle tpm2.TPMHandle, keyAlgorithm string, symmetricKeySize uint16) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
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
		// Key already exists at this handle, verify it matches the expected algorithm
		return verifyExistingKey(dev, persistentHandle, keyAlgorithm, symmetricKeySize)
	}

	// Handle is free, create a new primary key based on algorithm
	switch keyAlgorithm {
	case KeyAlgorithmRSA2048:
		return createPersistentRSAKey(dev, persistentHandle, 2048, symmetricKeySize)
	case KeyAlgorithmRSA4096:
		return createPersistentRSAKey(dev, persistentHandle, 4096, symmetricKeySize)
	case KeyAlgorithmECCP256:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCNistP256, symmetricKeySize)
	case KeyAlgorithmECCP384:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCNistP384, symmetricKeySize)
	case KeyAlgorithmECCP521:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCNistP521, symmetricKeySize)
	case KeyAlgorithmECCBrainpoolP256:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCBrainpoolP256R1, symmetricKeySize)
	case KeyAlgorithmECCBrainpoolP384:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCBrainpoolP384R1, symmetricKeySize)
	case KeyAlgorithmECCBrainpoolP512:
		return createPersistentECCKey(dev, persistentHandle, tpm2.TPMECCBrainpoolP512R1, symmetricKeySize)
	default:
		return 0, tpm2.TPM2BName{}, fmt.Errorf("unsupported key algorithm: %s", keyAlgorithm)
	}
}

// verifyExistingKey verifies that an existing key at the handle matches the expected algorithm.
func verifyExistingKey(dev transport.TPM, persistentHandle tpm2.TPMHandle, keyAlgorithm string, symmetricKeySize uint16) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	read := tpm2.ReadPublic{ObjectHandle: persistentHandle}
	pub, err := read.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to read public area of existing key: %w", err)
	}

	publicArea, err := pub.OutPublic.Contents()
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to parse public area: %w", err)
	}

	attrs := publicArea.ObjectAttributes
	if !attrs.Decrypt || !attrs.Restricted {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("existing key at 0x%08X is not a storage key (missing Decrypt or Restricted)", persistentHandle)
	}

	// Verify the key algorithm matches
	switch keyAlgorithm {
	case KeyAlgorithmRSA2048, KeyAlgorithmRSA4096:
		if publicArea.Type != tpm2.TPMAlgRSA {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing key at 0x%08X is %s, but RSA was requested", persistentHandle, algName(publicArea.Type))
		}
		// Verify RSA key size
		rsaParams, err := publicArea.Parameters.RSADetail()
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to get RSA parameters: %w", err)
		}
		expectedBits := tpm2.TPMKeyBits(2048)
		if keyAlgorithm == KeyAlgorithmRSA4096 {
			expectedBits = 4096
		}
		if rsaParams.KeyBits != expectedBits {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing RSA key at 0x%08X is %d bits, but %d was requested", persistentHandle, rsaParams.KeyBits, expectedBits)
		}
		// Verify symmetric protection size
		if rsaParams.Symmetric.Algorithm != tpm2.TPMAlgAES {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing RSA key at 0x%08X uses %s for symmetric protection, but AES was requested", persistentHandle, algName(rsaParams.Symmetric.Algorithm))
		}
		keyBits, err := rsaParams.Symmetric.KeyBits.AES()
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to get AES key bits: %w", err)
		}
		if *keyBits != tpm2.TPMKeyBits(symmetricKeySize) {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing RSA key at 0x%08X uses %d-bit AES, but %d-bit was requested", persistentHandle, *keyBits, symmetricKeySize)
		}
	case KeyAlgorithmECCP256, KeyAlgorithmECCP384, KeyAlgorithmECCP521, KeyAlgorithmECCBrainpoolP256, KeyAlgorithmECCBrainpoolP384, KeyAlgorithmECCBrainpoolP512:
		if publicArea.Type != tpm2.TPMAlgECC {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing key at 0x%08X is %s, but ECC was requested", persistentHandle, algName(publicArea.Type))
		}
		// Verify ECC curve
		eccParams, err := publicArea.Parameters.ECCDetail()
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to get ECC parameters: %w", err)
		}

		var expectedCurve tpm2.TPMECCCurve
		switch keyAlgorithm {
		case KeyAlgorithmECCP256:
			expectedCurve = tpm2.TPMECCNistP256
		case KeyAlgorithmECCP384:
			expectedCurve = tpm2.TPMECCNistP384
		case KeyAlgorithmECCP521:
			expectedCurve = tpm2.TPMECCNistP521
		case KeyAlgorithmECCBrainpoolP256:
			expectedCurve = tpm2.TPMECCBrainpoolP256R1
		case KeyAlgorithmECCBrainpoolP384:
			expectedCurve = tpm2.TPMECCBrainpoolP384R1
		case KeyAlgorithmECCBrainpoolP512:
			expectedCurve = tpm2.TPMECCBrainpoolP512R1
		}

		if eccParams.CurveID != expectedCurve {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing ECC key at 0x%08X uses curve %d, but curve %d was requested", persistentHandle, eccParams.CurveID, expectedCurve)
		}

		// Verify symmetric protection size
		if eccParams.Symmetric.Algorithm != tpm2.TPMAlgAES {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing ECC key at 0x%08X uses %s for symmetric protection, but AES was requested", persistentHandle, algName(eccParams.Symmetric.Algorithm))
		}
		keyBits, err := eccParams.Symmetric.KeyBits.AES()
		if err != nil {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to get AES key bits: %w", err)
		}
		if *keyBits != tpm2.TPMKeyBits(symmetricKeySize) {
			return 0, tpm2.TPM2BName{}, fmt.Errorf("existing ECC key at 0x%08X uses %d-bit AES, but %d-bit was requested", persistentHandle, *keyBits, symmetricKeySize)
		}
	default:
		return 0, tpm2.TPM2BName{}, fmt.Errorf("unsupported key algorithm: %s", keyAlgorithm)
	}

	// Key exists and is valid, use it
	return persistentHandle, pub.Name, nil
}

// algName returns a human-readable name for a TPM algorithm
func algName(alg tpm2.TPMAlgID) string {
	switch alg {
	case tpm2.TPMAlgRSA:
		return "RSA"
	case tpm2.TPMAlgTDES:
		return "TDES"
	case tpm2.TPMAlgSHA1:
		return "SHA1"
	case tpm2.TPMAlgHMAC:
		return "HMAC"
	case tpm2.TPMAlgAES:
		return "AES"
	case tpm2.TPMAlgMGF1:
		return "MGF1"
	case tpm2.TPMAlgKeyedHash:
		return "KeyedHash"
	case tpm2.TPMAlgXOR:
		return "XOR"
	case tpm2.TPMAlgSHA256:
		return "SHA256"
	case tpm2.TPMAlgSHA384:
		return "SHA384"
	case tpm2.TPMAlgSHA512:
		return "SHA512"
	case tpm2.TPMAlgNull:
		return "Null"
	case tpm2.TPMAlgSM4:
		return "SM4"
	case tpm2.TPMAlgRSASSA:
		return "RSASSA"
	case tpm2.TPMAlgRSAES:
		return "RSAES"
	case tpm2.TPMAlgRSAPSS:
		return "RSAPSS"
	case tpm2.TPMAlgOAEP:
		return "OAEP"
	case tpm2.TPMAlgECDSA:
		return "ECDSA"
	case tpm2.TPMAlgECDH:
		return "ECDH"
	case tpm2.TPMAlgECDAA:
		return "ECDAA"
	case tpm2.TPMAlgSM2:
		return "SM2"
	case tpm2.TPMAlgECSchnorr:
		return "ECSchnorr"
	case tpm2.TPMAlgKDF1SP80056A:
		return "KDF1_SP800_56A"
	case tpm2.TPMAlgKDF2:
		return "KDF2"
	case tpm2.TPMAlgKDF1SP800108:
		return "KDF1_SP800_108"
	case tpm2.TPMAlgECC:
		return "ECC"
	case tpm2.TPMAlgSymCipher:
		return "SymCipher"
	case tpm2.TPMAlgCamellia:
		return "Camellia"
	case tpm2.TPMAlgCMAC:
		return "CMAC"
	case tpm2.TPMAlgCTR:
		return "CTR"
	case tpm2.TPMAlgOFB:
		return "OFB"
	case tpm2.TPMAlgCBC:
		return "CBC"
	case tpm2.TPMAlgCFB:
		return "CFB"
	case tpm2.TPMAlgECB:
		return "ECB"
	case tpm2.TPMAlgCCM:
		return "CCM"
	default:
		return fmt.Sprintf("unknown(0x%04X)", alg)
	}
}

// createPersistentRSAKey creates a new RSA primary storage key (SRK) and persists it.
func createPersistentRSAKey(dev transport.TPM, persistentHandle tpm2.TPMHandle, keyBits uint16, symmetricKeySize uint16) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	// RSA Storage Root Key template with AES symmetric protection
	rsaSRKTemplate := tpm2.TPMTPublic{
		Type:    tpm2.TPMAlgRSA,
		NameAlg: tpm2.TPMAlgSHA256,
		ObjectAttributes: tpm2.TPMAObject{
			FixedTPM:            true,
			FixedParent:         true,
			SensitiveDataOrigin: true,
			UserWithAuth:        true,
			NoDA:                true,
			Restricted:          true,
			Decrypt:             true,
		},
		Parameters: tpm2.NewTPMUPublicParms(
			tpm2.TPMAlgRSA,
			&tpm2.TPMSRSAParms{
				Symmetric: tpm2.TPMTSymDefObject{
					Algorithm: tpm2.TPMAlgAES,
					KeyBits:   tpm2.NewTPMUSymKeyBits(tpm2.TPMAlgAES, tpm2.TPMKeyBits(symmetricKeySize)),
					Mode:      tpm2.NewTPMUSymMode(tpm2.TPMAlgAES, tpm2.TPMAlgCFB),
				},
				KeyBits: tpm2.TPMKeyBits(keyBits),
			},
		),
	}

	createPrimaryCmd := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic:      tpm2.New2B(rsaSRKTemplate),
	}

	createPrimaryRsp, err := createPrimaryCmd.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to create RSA primary key: %w", err)
	}

	return persistKey(dev, persistentHandle, createPrimaryRsp.ObjectHandle, createPrimaryRsp.Name)
}

// createPersistentECCKey creates a new ECC primary storage key and persists it.
func createPersistentECCKey(dev transport.TPM, persistentHandle tpm2.TPMHandle, curveID tpm2.TPMECCCurve, symmetricKeySize uint16) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	// ECC Storage Root Key template
	eccSRKTemplate := tpm2.TPMTPublic{
		Type:    tpm2.TPMAlgECC,
		NameAlg: tpm2.TPMAlgSHA256,
		ObjectAttributes: tpm2.TPMAObject{
			FixedTPM:            true,
			FixedParent:         true,
			SensitiveDataOrigin: true,
			UserWithAuth:        true,
			NoDA:                true,
			Restricted:          true,
			Decrypt:             true,
		},
		Parameters: tpm2.NewTPMUPublicParms(
			tpm2.TPMAlgECC,
			&tpm2.TPMSECCParms{
				Symmetric: tpm2.TPMTSymDefObject{
					Algorithm: tpm2.TPMAlgAES,
					KeyBits:   tpm2.NewTPMUSymKeyBits(tpm2.TPMAlgAES, tpm2.TPMKeyBits(symmetricKeySize)),
					Mode:      tpm2.NewTPMUSymMode(tpm2.TPMAlgAES, tpm2.TPMAlgCFB),
				},
				CurveID: curveID,
			},
		),
	}

	createPrimaryCmd := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic:      tpm2.New2B(eccSRKTemplate),
	}

	createPrimaryRsp, err := createPrimaryCmd.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to create ECC primary key: %w", err)
	}

	return persistKey(dev, persistentHandle, createPrimaryRsp.ObjectHandle, createPrimaryRsp.Name)
}

// persistKey persists a transient key to a persistent handle.
func persistKey(dev transport.TPM, persistentHandle tpm2.TPMHandle, transientHandle tpm2.TPMHandle, name tpm2.TPM2BName) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	evictCmd := tpm2.EvictControl{
		Auth: tpm2.AuthHandle{
			Handle: tpm2.TPMRHOwner,
			Auth:   tpm2.PasswordAuth(nil),
		},
		ObjectHandle: &tpm2.NamedHandle{
			Handle: transientHandle,
			Name:   name,
		},
		PersistentHandle: persistentHandle,
	}

	_, err := evictCmd.Execute(dev)
	if err != nil {
		// Try to flush the transient key before returning error
		flushCmd := tpm2.FlushContext{FlushHandle: transientHandle}
		flushCmd.Execute(dev)
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to persist key: %w", err)
	}

	// After EvictControl, the transient handle is automatically flushed by the TPM
	// and the key is now available at the persistent handle
	return persistentHandle, name, nil
}

// createAESKey creates a new AES symmetric cipher key as a child of the primary RSA key.
// Returns the private and public portions of the key for later loading.
func createAESKey(dev transport.TPM, primaryHandle tpm2.TPMHandle, primaryName tpm2.TPM2BName, symmetricKeySize uint16, password string) (tpm2.TPM2BPrivate, tpm2.TPM2BPublic, error) {
	createAES := tpm2.Create{
		ParentHandle: tpm2.NamedHandle{
			Handle: primaryHandle,
			Name:   primaryName,
		},
		InSensitive: tpm2.TPM2BSensitiveCreate{
			Sensitive: &tpm2.TPMSSensitiveCreate{
				UserAuth: tpm2.TPM2BAuth{
					Buffer: []byte(password),
				},
			},
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
						KeyBits:   tpm2.NewTPMUSymKeyBits(tpm2.TPMAlgAES, tpm2.TPMKeyBits(symmetricKeySize)),
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

// createHMACKey creates a new HMAC key as a child of the primary key.
func createHMACKey(dev transport.TPM, primaryHandle tpm2.TPMHandle, primaryName tpm2.TPM2BName, alg tpm2.TPMAlgID, password string) (tpm2.TPM2BPrivate, tpm2.TPM2BPublic, error) {
	createHMAC := tpm2.Create{
		ParentHandle: tpm2.NamedHandle{
			Handle: primaryHandle,
			Name:   primaryName,
		},
		InSensitive: tpm2.TPM2BSensitiveCreate{
			Sensitive: &tpm2.TPMSSensitiveCreate{
				UserAuth: tpm2.TPM2BAuth{
					Buffer: []byte(password),
				},
			},
		},
		InPublic: tpm2.New2B(tpm2.TPMTPublic{
			Type:    tpm2.TPMAlgKeyedHash,
			NameAlg: tpm2.TPMAlgSHA256, // Name algorithm for the key itself
			ObjectAttributes: tpm2.TPMAObject{
				FixedTPM:            true,
				FixedParent:         true,
				UserWithAuth:        true,
				SensitiveDataOrigin: true,
				SignEncrypt:         true, // Required for HMAC operations
			},
			Parameters: tpm2.NewTPMUPublicParms(
				tpm2.TPMAlgKeyedHash,
				&tpm2.TPMSKeyedHashParms{
					Scheme: tpm2.TPMTKeyedHashScheme{
						Scheme: tpm2.TPMAlgHMAC,
						Details: tpm2.NewTPMUSchemeKeyedHash(
							tpm2.TPMAlgHMAC,
							&tpm2.TPMSSchemeHMAC{
								HashAlg: alg,
							},
						),
					},
				},
			),
		}),
	}

	createRsp, err := createHMAC.Execute(dev)
	if err != nil {
		return tpm2.TPM2BPrivate{}, tpm2.TPM2BPublic{}, fmt.Errorf("failed to create HMAC key: %w", err)
	}

	return createRsp.OutPrivate, createRsp.OutPublic, nil
}

// loadKey loads a key into the TPM and returns its handle and name.
func loadKey(dev transport.TPM, primaryHandle tpm2.TPMHandle, primaryName tpm2.TPM2BName, private tpm2.TPM2BPrivate, public tpm2.TPM2BPublic) (tpm2.TPMHandle, tpm2.TPM2BName, error) {
	load := tpm2.Load{
		ParentHandle: tpm2.NamedHandle{
			Handle: primaryHandle,
			Name:   primaryName,
		},
		InPrivate: private,
		InPublic:  public,
	}

	loadRsp, err := load.Execute(dev)
	if err != nil {
		return 0, tpm2.TPM2BName{}, fmt.Errorf("failed to load key: %w", err)
	}

	return loadRsp.ObjectHandle, loadRsp.Name, nil
}

// saveAESKeyMaterial saves the AES and HMAC key material to a file
func saveAESKeyMaterial(keyFilePath string, privateAES, privateHMAC tpm2.TPM2BPrivate, publicAES, publicHMAC tpm2.TPM2BPublic) error {
	// Serialize the key material using TPM marshaling
	privateAESBytes := tpm2.Marshal(privateAES)
	publicAESBytes := tpm2.Marshal(publicAES)
	privateHMACBytes := tpm2.Marshal(privateHMAC)
	publicHMACBytes := tpm2.Marshal(publicHMAC)

	keyMaterial := AESKeyMaterial{
		Private:     privateAESBytes,
		Public:      publicAESBytes,
		HMACPrivate: privateHMACBytes,
		HMACPublic:  publicHMACBytes,
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

// NewAEAD creates a new AEAD instance that uses TPM for key operations.
// keyAlgorithm: the primary key algorithm
// symmetricAlgorithm: the symmetric key algorithm (e.g. "aes-128", "aes-256")
// hmacAlgorithm: the HMAC algorithm ("sha256", "sha384", "sha512")
// password: optional password for key authorization (empty string for no password)
func NewAEAD(tpmPath string, persistentHandle uint32, keyFilePath string, keyAlgorithm string, symmetricAlgorithm string, hmacAlgorithm string, password string) (*AEAD, error) {
	// Open TPM device based on OS (implemented in platform-specific files)
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open TPM: %w", err)
	}

	// Map symmetric algorithm to key size
	var symmetricKeySize uint16
	switch symmetricAlgorithm {
	case SymmetricAlgorithmAES128:
		symmetricKeySize = 128
	case SymmetricAlgorithmAES256:
		symmetricKeySize = 256
	default:
		tpmDevice.Close()
		return nil, fmt.Errorf("unsupported symmetric algorithm: %s", symmetricAlgorithm)
	}

	// Determine HMAC algorithm and size from configuration
	// Default to SHA256
	hmacAlg := tpm2.TPMAlgSHA256
	hmacSize := 32

	switch hmacAlgorithm {
	case HMACAlgorithmSHA256:
		hmacAlg = tpm2.TPMAlgSHA256
		hmacSize = 32
	case HMACAlgorithmSHA384:
		hmacAlg = tpm2.TPMAlgSHA384
		hmacSize = 48
	case HMACAlgorithmSHA512:
		hmacAlg = tpm2.TPMAlgSHA512
		hmacSize = 64
	default:
		tpmDevice.Close()
		return nil, fmt.Errorf("unsupported HMAC algorithm: %s", hmacAlgorithm)
	}

	// Get or create the persistent primary key with the specified algorithm
	primaryHandle, primaryName, err := getOrCreatePersistentKey(tpmDevice, tpm2.TPMHandle(persistentHandle), keyAlgorithm, symmetricKeySize)
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to get or create persistent primary key: %w", err)
	}

	var aesPrivate tpm2.TPM2BPrivate
	var aesPublic tpm2.TPM2BPublic
	var hmacPrivate tpm2.TPM2BPrivate
	var hmacPublic tpm2.TPM2BPublic

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

		// HMAC keys are required if key file exists
		if len(keyMaterial.HMACPrivate) == 0 {
			tpmDevice.Close()
			return nil, fmt.Errorf("key file exists but missing HMAC key material - please regenerate keys")
		}

		privHMAC, err := tpm2.Unmarshal[tpm2.TPM2BPrivate](keyMaterial.HMACPrivate)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to unmarshal HMAC private key: %w", err)
		}
		hmacPrivate = *privHMAC

		pubHMAC, err := tpm2.Unmarshal[tpm2.TPM2BPublic](keyMaterial.HMACPublic)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to unmarshal HMAC public key: %w", err)
		}
		hmacPublic = *pubHMAC

		hmacPublicArea, err := pubHMAC.Contents()
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to parse HMAC public area: %w", err)
		}

		// Inspect the existing HMAC key to set the algorithm and size correctly
		hmacParams, err := hmacPublicArea.Parameters.KeyedHashDetail()
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to get HMAC params: %w", err)
		}

		hmacScheme, err := hmacParams.Scheme.Details.HMAC()
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to get HMAC scheme details: %w", err)
		}

		hmacAlg = hmacScheme.HashAlg
		switch hmacAlg {
		case tpm2.TPMAlgSHA256:
			hmacSize = 32
		case tpm2.TPMAlgSHA384:
			hmacSize = 48
		case tpm2.TPMAlgSHA512:
			hmacSize = 64
		default:
			tpmDevice.Close()
			return nil, fmt.Errorf("loaded HMAC key uses unsupported hash algorithm: 0x%x", hmacAlg)
		}
	} else {
		// No existing key, create new AES and HMAC keys
		aesPrivate, aesPublic, err = createAESKey(tpmDevice, primaryHandle, primaryName, symmetricKeySize, password)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to create AES key: %w", err)
		}
		hmacPrivate, hmacPublic, err = createHMACKey(tpmDevice, primaryHandle, primaryName, hmacAlg, password)
		if err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to create HMAC key: %w", err)
		}

		// Save the key material for future use
		if err := saveAESKeyMaterial(keyFilePath, aesPrivate, hmacPrivate, aesPublic, hmacPublic); err != nil {
			tpmDevice.Close()
			return nil, fmt.Errorf("failed to save AES key material: %w", err)
		}
	}

	// Load the AES key into the TPM
	aesHandle, aesName, err := loadKey(tpmDevice, primaryHandle, primaryName, aesPrivate, aesPublic)
	if err != nil {
		tpmDevice.Close()
		return nil, fmt.Errorf("failed to load AES key: %w", err)
	}

	// Load the HMAC key into the TPM
	hmacHandle, hmacName, err := loadKey(tpmDevice, primaryHandle, primaryName, hmacPrivate, hmacPublic)
	if err != nil {
		tpmDevice.Close()
		// Cleanup AES handle
		tpm2.FlushContext{FlushHandle: aesHandle}.Execute(tpmDevice)
		return nil, fmt.Errorf("failed to load HMAC key: %w", err)
	}

	return &AEAD{
		tpmDevice:        tpmDevice,
		primaryHandle:    primaryHandle,
		aesKeyHandle:     aesHandle,
		aesKeyName:       aesName,
		aesKeyPrivate:    aesPrivate,
		aesKeyPublic:     aesPublic,
		hmacKeyHandle:    hmacHandle,
		hmacKeyName:      hmacName,
		hmacKeyPrivate:   hmacPrivate,
		hmacKeyPublic:    hmacPublic,
		primaryName:      primaryName,
		symmetricKeySize: symmetricKeySize,
		hmacAlg:          hmacAlg,
		hmacSize:         hmacSize,
		password:         password,
	}, nil
}

// computeHMAC computes the HMAC for the given data using the TPM
func (t *AEAD) computeHMAC(data []byte) ([]byte, error) {
	// TPM2_HMAC can handle up to 1024 bytes (MAX_BUFFER) usually.
	// For larger data, we would need to use HMAC_Start/Update/Complete.
	// Here, we are signing (version || IV || ciphertext).
	// IV=16, Version=1, Ciphertext=32 (for DEK). Total = 49 bytes.
	// This fits easily in a single TPM2_HMAC call.

	hmacCmd := tpm2.Hmac{
		Handle: tpm2.AuthHandle{
			Handle: t.hmacKeyHandle,
			Name:   t.hmacKeyName,
			Auth:   tpm2.PasswordAuth([]byte(t.password)),
		},
		Buffer: tpm2.TPM2BMaxBuffer{
			Buffer: data,
		},
		HashAlg: t.hmacAlg,
	}

	hmacRsp, err := hmacCmd.Execute(t.tpmDevice)
	if err != nil {
		return nil, fmt.Errorf("TPM HMAC failed: %w", err)
	}

	return hmacRsp.OutHMAC.Buffer, nil
}

// Encrypt encrypts plaintext with associatedData using TPM
// This implements the tink.AEAD interface with Encrypt-then-MAC
func (t *AEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	// Lock to prevent concurrent TPM access
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Generate IV
	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// 2. Encrypt with AES-CFB
	encryptCmd := tpm2.EncryptDecrypt2{
		KeyHandle: tpm2.AuthHandle{
			Handle: t.aesKeyHandle,
			Name:   t.aesKeyName,
			Auth:   tpm2.PasswordAuth([]byte(t.password)),
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
	ciphertext := encryptRsp.OutData.Buffer

	// 3. Compute HMAC over (associatedData || version || IV || ciphertext)
	// Format: [version(1)] [IV(16)] [ciphertext]
	// We authenticate the AAD and the encrypted blob
	versionByte := []byte{EncryptedDataVersion1}

	hmacInput := make([]byte, 0, len(associatedData)+len(versionByte)+len(iv)+len(ciphertext))
	hmacInput = append(hmacInput, associatedData...)
	hmacInput = append(hmacInput, versionByte...)
	hmacInput = append(hmacInput, iv...)
	hmacInput = append(hmacInput, ciphertext...)

	tag, err := t.computeHMAC(hmacInput)
	if err != nil {
		return nil, err
	}

	// 4. Construct result: [version(1)] [IV(16)] [ciphertext] [tag]
	result := make([]byte, 0, len(versionByte)+len(iv)+len(ciphertext)+len(tag))
	result = append(result, versionByte...)
	result = append(result, iv...)
	result = append(result, ciphertext...)
	result = append(result, tag...)

	return result, nil
}

// Decrypt decrypts ciphertext with associatedData using TPM
// This implements the tink.AEAD interface
func (t *AEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	// Lock to prevent concurrent TPM access
	t.mu.Lock()
	defer t.mu.Unlock()

	// Authenticated Mode: [Version(1)] [IV(16)] [Ciphertext] [Tag(t.hmacSize)]
	if len(ciphertext) < 1+16+t.hmacSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	if ciphertext[0] != EncryptedDataVersion1 {
		return nil, fmt.Errorf("unsupported ciphertext version: %d", ciphertext[0])
	}

	// Parse components
	// Version is at index 0
	iv := ciphertext[1 : 1+16]
	tagOffset := len(ciphertext) - t.hmacSize
	actualCiphertext := ciphertext[1+16 : tagOffset]
	tag := ciphertext[tagOffset:]

	// 1. Verify HMAC
	// Reconstruct input: associatedData || version || iv || ciphertext
	versionByte := []byte{EncryptedDataVersion1}
	hmacInput := make([]byte, 0, len(associatedData)+len(versionByte)+len(iv)+len(actualCiphertext))
	hmacInput = append(hmacInput, associatedData...)
	hmacInput = append(hmacInput, versionByte...)
	hmacInput = append(hmacInput, iv...)
	hmacInput = append(hmacInput, actualCiphertext...)

	computedTag, err := t.computeHMAC(hmacInput)
	if err != nil {
		return nil, err
	}

	if subtle.ConstantTimeCompare(tag, computedTag) != 1 {
		return nil, fmt.Errorf("HMAC verification failed")
	}

	// 2. Decrypt
	decryptCmd := tpm2.EncryptDecrypt2{
		KeyHandle: tpm2.AuthHandle{
			Handle: t.aesKeyHandle,
			Name:   t.aesKeyName,
			Auth:   tpm2.PasswordAuth([]byte(t.password)),
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

// TPMFeatures represents the supported features of a TPM as supported by Pithos
type TPMFeatures struct {
	PrimaryAlgorithms   []string
	HMACAlgorithms      []string
	SymmetricAlgorithms []string
}

// DetectFeatures queries the TPM for supported features and filters them to those supported by Pithos
func DetectFeatures(tpmPath string) (*TPMFeatures, error) {
	tpmDevice, err := openTPMDevice(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open TPM: %w", err)
	}
	defer tpmDevice.Close()

	features := &TPMFeatures{}

	// We create a transient primary key to test child creation
	createPrimaryCmd := tpm2.CreatePrimary{
		PrimaryHandle: tpm2.TPMRHOwner,
		InPublic:      tpm2.New2B(tpm2.RSASRKTemplate),
	}
	createPrimaryRsp, err := createPrimaryCmd.Execute(tpmDevice)
	if err != nil {
		return nil, fmt.Errorf("failed to create test primary key: %w", err)
	}
	primaryHandle := createPrimaryRsp.ObjectHandle
	primaryName := createPrimaryRsp.Name
	defer tpm2.FlushContext{FlushHandle: primaryHandle}.Execute(tpmDevice)

	// Detect supported Primary Algorithms (RSA)
	for _, alg := range []struct {
		name string
		bits uint16
	}{
		{KeyAlgorithmRSA2048, 2048},
		{KeyAlgorithmRSA4096, 4096},
	} {
		createRSA := tpm2.Create{
			ParentHandle: tpm2.NamedHandle{
				Handle: primaryHandle,
				Name:   primaryName,
			},
			InPublic: tpm2.New2B(tpm2.TPMTPublic{
				Type:    tpm2.TPMAlgRSA,
				NameAlg: tpm2.TPMAlgSHA256,
				ObjectAttributes: tpm2.TPMAObject{
					FixedTPM:            true,
					FixedParent:         true,
					SensitiveDataOrigin: true,
					UserWithAuth:        true,
					Decrypt:             true,
				},
				Parameters: tpm2.NewTPMUPublicParms(
					tpm2.TPMAlgRSA,
					&tpm2.TPMSRSAParms{
						Symmetric: tpm2.TPMTSymDefObject{
							Algorithm: tpm2.TPMAlgNull,
						},
						KeyBits: tpm2.TPMKeyBits(alg.bits),
					},
				),
			}),
		}
		_, err = createRSA.Execute(tpmDevice)
		if err == nil {
			features.PrimaryAlgorithms = append(features.PrimaryAlgorithms, alg.name)
		}
	}

	// Detect supported Primary Algorithms (ECC)
	for _, alg := range []struct {
		name  string
		curve tpm2.TPMECCCurve
	}{
		{KeyAlgorithmECCP256, tpm2.TPMECCNistP256},
		{KeyAlgorithmECCP384, tpm2.TPMECCNistP384},
		{KeyAlgorithmECCP521, tpm2.TPMECCNistP521},
		{KeyAlgorithmECCBrainpoolP256, tpm2.TPMECCBrainpoolP256R1},
		{KeyAlgorithmECCBrainpoolP384, tpm2.TPMECCBrainpoolP384R1},
		{KeyAlgorithmECCBrainpoolP512, tpm2.TPMECCBrainpoolP512R1},
	} {
		createECC := tpm2.Create{
			ParentHandle: tpm2.NamedHandle{
				Handle: primaryHandle,
				Name:   primaryName,
			},
			InPublic: tpm2.New2B(tpm2.TPMTPublic{
				Type:    tpm2.TPMAlgECC,
				NameAlg: tpm2.TPMAlgSHA256,
				ObjectAttributes: tpm2.TPMAObject{
					FixedTPM:            true,
					FixedParent:         true,
					SensitiveDataOrigin: true,
					UserWithAuth:        true,
					Decrypt:             true,
				},
				Parameters: tpm2.NewTPMUPublicParms(
					tpm2.TPMAlgECC,
					&tpm2.TPMSECCParms{
						Symmetric: tpm2.TPMTSymDefObject{
							Algorithm: tpm2.TPMAlgNull,
						},
						CurveID: alg.curve,
					},
				),
			}),
		}
		_, err = createECC.Execute(tpmDevice)
		if err == nil {
			features.PrimaryAlgorithms = append(features.PrimaryAlgorithms, alg.name)
		}
	}

	// Detect supported HMAC algorithms
	for _, alg := range []struct {
		name string
		alg  tpm2.TPMAlgID
	}{
		{HMACAlgorithmSHA256, tpm2.TPMAlgSHA256},
		{HMACAlgorithmSHA384, tpm2.TPMAlgSHA384},
		{HMACAlgorithmSHA512, tpm2.TPMAlgSHA512},
	} {
		createHMAC := tpm2.Create{
			ParentHandle: tpm2.NamedHandle{
				Handle: primaryHandle,
				Name:   primaryName,
			},
			InPublic: tpm2.New2B(tpm2.TPMTPublic{
				Type:    tpm2.TPMAlgKeyedHash,
				NameAlg: tpm2.TPMAlgSHA256,
				ObjectAttributes: tpm2.TPMAObject{
					FixedTPM:            true,
					FixedParent:         true,
					UserWithAuth:        true,
					SensitiveDataOrigin: true,
					SignEncrypt:         true,
				},
				Parameters: tpm2.NewTPMUPublicParms(
					tpm2.TPMAlgKeyedHash,
					&tpm2.TPMSKeyedHashParms{
						Scheme: tpm2.TPMTKeyedHashScheme{
							Scheme: tpm2.TPMAlgHMAC,
							Details: tpm2.NewTPMUSchemeKeyedHash(
								tpm2.TPMAlgHMAC,
								&tpm2.TPMSSchemeHMAC{
									HashAlg: alg.alg,
								},
							),
						},
					},
				),
			}),
		}
		_, err = createHMAC.Execute(tpmDevice)
		if err == nil {
			features.HMACAlgorithms = append(features.HMACAlgorithms, alg.name)
		}
	}

	// Detect supported Symmetric Algorithms
	for _, alg := range []struct {
		name string
		bits uint16
	}{
		{SymmetricAlgorithmAES128, 128},
		{SymmetricAlgorithmAES256, 256},
	} {
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
					SensitiveDataOrigin: true,
					UserWithAuth:        true,
					Decrypt:             true,
					SignEncrypt:         true,
				},
				Parameters: tpm2.NewTPMUPublicParms(
					tpm2.TPMAlgSymCipher,
					&tpm2.TPMSSymCipherParms{
						Sym: tpm2.TPMTSymDefObject{
							Algorithm: tpm2.TPMAlgAES,
							Mode:      tpm2.NewTPMUSymMode(tpm2.TPMAlgAES, tpm2.TPMAlgCFB),
							KeyBits:   tpm2.NewTPMUSymKeyBits(tpm2.TPMAlgAES, tpm2.TPMKeyBits(alg.bits)),
						},
					},
				),
			}),
		}
		_, err = createAES.Execute(tpmDevice)
		if err == nil {
			features.SymmetricAlgorithms = append(features.SymmetricAlgorithms, alg.name)
		}
	}

	return features, nil
}

// Close closes the TPM device
// Note: We flush the transient AES and HMAC key handles but not the persistent primary key
func (t *AEAD) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Flush the AES key handle (transient)
	if t.aesKeyHandle != 0 {
		tpm2.FlushContext{FlushHandle: t.aesKeyHandle}.Execute(t.tpmDevice)
		t.aesKeyHandle = 0
	}
	// Flush the HMAC key handle (transient)
	if t.hmacKeyHandle != 0 {
		tpm2.FlushContext{FlushHandle: t.hmacKeyHandle}.Execute(t.tpmDevice)
		t.hmacKeyHandle = 0
	}

	return t.tpmDevice.Close()
}
