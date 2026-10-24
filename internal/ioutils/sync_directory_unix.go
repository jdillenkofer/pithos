//go:build !windows

package ioutils

import (
	"errors"
	"os"
	"syscall"
)

// SyncDirectory persists directory entries where the filesystem supports it.
// Explicitly unsupported directory synchronization is a documented limitation;
// other errors, especially I/O failures, must propagate to the caller.
func SyncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = SyncFile(dir)
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		err = nil
	}
	return errors.Join(err, dir.Close())
}
