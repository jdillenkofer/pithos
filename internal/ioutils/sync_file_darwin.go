package ioutils

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// SyncFile flushes file data, including the device cache on macOS when the
// filesystem supports F_FULLFSYNC. Plain fsync alone need not flush that cache.
func SyncFile(file *os.File) error {
	if err := file.Sync(); err != nil {
		return err
	}
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
		return nil
	}
	return err
}
