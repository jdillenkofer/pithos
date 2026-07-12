//go:build !linux || (!amd64 && !arm64)

// Package st implements tape.Device for Linux SCSI tape devices driven by
// the st driver. On other platforms Open returns tape.ErrUnsupportedPlatform.
package st

import (
	"fmt"
	"runtime"

	"github.com/jdillenkofer/pithos/internal/tape"
)

// Open is not implemented for this operating system or architecture.
func Open(path string) (tape.Device, error) {
	return nil, fmt.Errorf("tape device support is not available on %s/%s: %w", runtime.GOOS, runtime.GOARCH, tape.ErrUnsupportedPlatform)
}
