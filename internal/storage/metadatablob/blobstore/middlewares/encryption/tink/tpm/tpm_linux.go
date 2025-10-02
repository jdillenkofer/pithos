//go:build linux

package tpm

import (
	"fmt"

	"github.com/google/go-tpm/tpm2/transport"
	"github.com/google/go-tpm/tpm2/transport/linuxtpm"
)

// openTPMDevice opens the TPM device on Linux
// tpmPath should be something like "/dev/tpmrm0" or "/dev/tpm0"
func openTPMDevice(tpmPath string) (transport.TPMCloser, error) {
	device, err := linuxtpm.Open(tpmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Linux TPM device %s: %w", tpmPath, err)
	}

	return device, nil
}
