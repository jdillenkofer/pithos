//go:build windows

package tpm

import (
	"fmt"

	"github.com/google/go-tpm/tpm2/transport"
	"github.com/google/go-tpm/tpm2/transport/windowstpm"
)

// openTPMDevice opens the TPM device on Windows
// tpmPath is ignored on Windows - Windows uses TBS (TPM Base Services)
// which automatically manages TPM access
func openTPMDevice(tpmPath string) (transport.TPMCloser, error) {
	device, err := windowstpm.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open Windows TPM device: %w", err)
	}

	return device, nil
}
