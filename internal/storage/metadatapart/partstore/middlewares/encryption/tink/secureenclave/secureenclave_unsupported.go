//go:build !darwin

package secureenclave

import (
	"crypto/ecdh"
	"fmt"
	"runtime"
)

// secKeyRef is a placeholder for non-Darwin platforms
type secKeyRef uintptr

// getOrCreateSecureEnclaveKey is not available on non-Darwin platforms
func getOrCreateSecureEnclaveKey(keyLabel string) (secKeyRef, error) {
	return 0, fmt.Errorf("Secure Enclave is not available on %s", runtime.GOOS)
}

// getPublicKey is not available on non-Darwin platforms
func getPublicKey(keyRef secKeyRef) (*ecdh.PublicKey, error) {
	return nil, fmt.Errorf("Secure Enclave is not available on %s", runtime.GOOS)
}

// secureEnclaveECDH is not available on non-Darwin platforms
func secureEnclaveECDH(keyRef secKeyRef, ephemeralPublic *ecdh.PublicKey) ([]byte, error) {
	return nil, fmt.Errorf("Secure Enclave is not available on %s", runtime.GOOS)
}

// releaseKeyRef is a no-op on non-Darwin platforms
func releaseKeyRef(keyRef secKeyRef) error {
	return nil
}
