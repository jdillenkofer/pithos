package tape

import (
	"sync"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// partLocker serializes logical mutations of one part without making
// unrelated parts contend with each other.
type partLocker struct {
	mu    sync.Mutex
	locks map[string]*partLock
}

type partLock struct {
	refs int
	mu   sync.Mutex
}

func newPartLocker() *partLocker {
	return &partLocker{locks: make(map[string]*partLock)}
}

func (l *partLocker) Lock(partID partstore.PartId) func() {
	key := partID.String()

	l.mu.Lock()
	entry, ok := l.locks[key]
	if !ok {
		entry = &partLock{}
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
