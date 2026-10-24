package ioutils

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// EnsureDurableDirectory creates a directory and persists newly created path
// components using the directory synchronization available on this platform.
func EnsureDurableDirectory(path string, mode fs.FileMode) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	return ensureDurableDirectory(path, mode)
}

func ensureDurableDirectory(path string, mode fs.FileMode) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("not a directory: %s", path)
		}
		// Also handle an entry left behind by a previous failed creation.
		return SyncDirectory(filepath.Dir(path))
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := ensureDurableDirectory(filepath.Dir(path), mode); err != nil {
		return err
	}
	if err := os.Mkdir(path, mode); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return SyncDirectory(filepath.Dir(path))
}
