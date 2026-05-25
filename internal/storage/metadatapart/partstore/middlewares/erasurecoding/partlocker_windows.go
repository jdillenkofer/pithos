//go:build windows

package erasurecoding

import (
	"sync"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type partLocker interface {
	Lock(partstore.PartId) func()
}

type windowsPartLocker struct {
	mu    sync.Mutex
	locks map[string]*partLockEntry
}

type partLockEntry struct {
	refs int
	mu   sync.Mutex
}

func newPartLocker() partLocker {
	return &windowsPartLocker{locks: make(map[string]*partLockEntry)}
}

func (l *windowsPartLocker) Lock(partId partstore.PartId) func() {
	key := partId.String()

	l.mu.Lock()
	entry, ok := l.locks[key]
	if !ok {
		entry = &partLockEntry{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()

		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
}
