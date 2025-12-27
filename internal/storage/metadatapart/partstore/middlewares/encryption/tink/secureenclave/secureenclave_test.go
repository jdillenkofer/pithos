//go:build darwin

package secureenclave

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecureEnclaveAEAD_EncryptDecrypt(t *testing.T) {
	// Skip if running in CI without Secure Enclave access
	// This test requires macOS with Apple Silicon or T1/T2 chip

	keyLabel := "pithos-test-key-" + t.Name()

	// Create AEAD
	aead, err := NewAEAD(keyLabel)
	if err != nil {
		// On Macs without Secure Enclave or without proper entitlements, skip
		t.Skipf("Secure Enclave not available: %v", err)
	}
	defer aead.Close()

	// Test basic encrypt/decrypt
	plaintext := []byte("Hello, Secure Enclave!")
	associatedData := []byte("test-associated-data")

	ciphertext, err := aead.Encrypt(plaintext, associatedData)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext)

	decrypted, err := aead.Decrypt(ciphertext, associatedData)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestSecureEnclaveAEAD_EncryptDecrypt_EmptyAssociatedData(t *testing.T) {
	keyLabel := "pithos-test-key-empty-" + t.Name()

	aead, err := NewAEAD(keyLabel)
	if err != nil {
		t.Skipf("Secure Enclave not available: %v", err)
	}
	defer aead.Close()

	plaintext := []byte("Test with empty associated data")

	ciphertext, err := aead.Encrypt(plaintext, nil)
	require.NoError(t, err)

	decrypted, err := aead.Decrypt(ciphertext, nil)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestSecureEnclaveAEAD_DecryptWithWrongAssociatedData(t *testing.T) {
	keyLabel := "pithos-test-key-wrong-ad-" + t.Name()

	aead, err := NewAEAD(keyLabel)
	if err != nil {
		t.Skipf("Secure Enclave not available: %v", err)
	}
	defer aead.Close()

	plaintext := []byte("Test data for wrong associated data test")
	associatedData := []byte("correct-associated-data")

	ciphertext, err := aead.Encrypt(plaintext, associatedData)
	require.NoError(t, err)

	// Try to decrypt with wrong associated data - should fail
	_, err = aead.Decrypt(ciphertext, []byte("wrong-associated-data"))
	assert.Error(t, err)
}

func TestSecureEnclaveAEAD_KeyPersistence(t *testing.T) {
	// Test that creating AEAD with the same label uses the same key
	keyLabel := "pithos-test-key-persistence"

	aead1, err := NewAEAD(keyLabel)
	if err != nil {
		t.Skipf("Secure Enclave not available: %v", err)
	}

	plaintext := []byte("Test key persistence")
	ciphertext, err := aead1.Encrypt(plaintext, nil)
	require.NoError(t, err)
	aead1.Close()

	// Create new AEAD with same label - should be able to decrypt
	aead2, err := NewAEAD(keyLabel)
	require.NoError(t, err)
	defer aead2.Close()

	decrypted, err := aead2.Decrypt(ciphertext, nil)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestSecureEnclaveAEAD_LargeData(t *testing.T) {
	keyLabel := "pithos-test-key-large-" + t.Name()

	aead, err := NewAEAD(keyLabel)
	if err != nil {
		t.Skipf("Secure Enclave not available: %v", err)
	}
	defer aead.Close()

	// Test with 1MB of data
	plaintext := make([]byte, 1024*1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	ciphertext, err := aead.Encrypt(plaintext, nil)
	require.NoError(t, err)

	decrypted, err := aead.Decrypt(ciphertext, nil)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}
