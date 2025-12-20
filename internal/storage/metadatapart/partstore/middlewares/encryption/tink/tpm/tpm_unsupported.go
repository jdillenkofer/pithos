//go:build !linux && !windows

package tpm

import (
	"fmt"
	"runtime"

	"github.com/google/go-tpm/tpm2/transport"
)

// openTPMDevice is not implemented for this operating system
func openTPMDevice(tpmPath string) (transport.TPMCloser, error) {
	return nil, fmt.Errorf("TPM support is not available on %s", runtime.GOOS)
}
