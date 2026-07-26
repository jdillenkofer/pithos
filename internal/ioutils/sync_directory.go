//go:build !windows

package ioutils

import "os"

// SyncDirectory makes directory-entry changes durable on platforms that
// support syncing an open directory.
func SyncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
