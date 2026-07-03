package erasurecoding

import (
	"sync"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type partLocker interface {
	Lock(partstore.PartId) func()
	RLock(partstore.PartId) func()
}

type rwPartLocker struct {
	mu    sync.Mutex
	locks map[string]*partLockEntry
}

type partLockEntry struct {
	refs int
	mu   sync.RWMutex
}

func newPartLocker() partLocker {
	return &rwPartLocker{locks: make(map[string]*partLockEntry)}
}

func (l *rwPartLocker) Lock(partId partstore.PartId) func() {
	key, entry := l.lockEntry(partId)
	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()
		l.unlockEntry(key, entry)
	}
}

func (l *rwPartLocker) RLock(partId partstore.PartId) func() {
	key, entry := l.lockEntry(partId)
	entry.mu.RLock()

	return func() {
		entry.mu.RUnlock()
		l.unlockEntry(key, entry)
	}
}

func (l *rwPartLocker) lockEntry(partId partstore.PartId) (string, *partLockEntry) {
	key := partId.String()

	l.mu.Lock()
	entry, ok := l.locks[key]
	if !ok {
		entry = &partLockEntry{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	return key, entry
}

func (l *rwPartLocker) unlockEntry(key string, entry *partLockEntry) {
	l.mu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(l.locks, key)
	}
	l.mu.Unlock()
}
