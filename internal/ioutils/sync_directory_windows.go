//go:build windows

package ioutils

import "os"

// SyncDirectory validates the directory on Windows but does not flush it,
// because Windows rejects os.File.Sync on directory handles. Callers must sync
// file contents before renaming.
func SyncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return dir.Close()
}
