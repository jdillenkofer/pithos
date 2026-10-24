package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

func isValueTemporaryFilename(target, name string) bool {
	prefix := "." + filepath.Base(target) + ".tmp-"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == "" {
		return false
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// readPersistentValue assumes exclusive ownership of the target and its staging
// files. Atomic writes preserve the current file until publication; abandoned
// temporary writes are discarded, never promoted.
func readPersistentValue(target string) ([]byte, error) {
	value, err := os.ReadFile(target)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(target)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	removed := false
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !isValueTemporaryFilename(target, entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(parent, entry.Name())); err != nil {
			return nil, err
		}
		removed = true
	}
	if removed {
		if err := ioutils.SyncDirectory(parent); err != nil {
			return nil, err
		}
	}
	return value, nil
}
