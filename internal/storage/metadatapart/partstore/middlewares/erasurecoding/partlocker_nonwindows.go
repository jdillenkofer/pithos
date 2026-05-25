//go:build !windows

package erasurecoding

import "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"

type partLocker interface {
	Lock(partstore.PartId) func()
}

type noopPartLocker struct{}

func newPartLocker() partLocker {
	return &noopPartLocker{}
}

func (l *noopPartLocker) Lock(partstore.PartId) func() {
	return func() {}
}
