//go:build !darwin

package ioutils

import "os"

// SyncFile flushes file data and metadata using the platform's file sync API.
func SyncFile(file *os.File) error {
	return file.Sync()
}
