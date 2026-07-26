package tape

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/tape/journal"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
)

type readCacheEntry struct {
	path       string
	size       int64
	modTime    time.Time
	lastAccess time.Time
	hash       [32]byte
	verified   bool
	readers    int
}

type recallState struct {
	done chan struct{}
	err  error
}

func (s *tapePartStore) initializeReadCache() error {
	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		return err
	}
	s.cacheEntries = make(map[journal.GenerationID]*readCacheEntry)
	s.recalls = make(map[journal.GenerationID]*recallState)
	s.cacheBytes = 0
	for _, dirEntry := range entries {
		if dirEntry.IsDir() {
			continue
		}
		name := dirEntry.Name()
		path := filepath.Join(s.cacheDir, name)
		if strings.HasPrefix(name, ".staging-") {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		if !strings.HasSuffix(name, ".part") {
			continue
		}
		generation, err := parseCacheGeneration(strings.TrimSuffix(name, ".part"))
		if err != nil {
			continue
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		s.cacheEntries[generation] = &readCacheEntry{
			path:       path,
			size:       info.Size(),
			modTime:    info.ModTime(),
			lastAccess: info.ModTime(),
		}
		s.cacheBytes += info.Size()
	}
	s.evictReadCacheLocked(journal.GenerationID{})
	return nil
}

func parseCacheGeneration(value string) (journal.GenerationID, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return journal.GenerationID{}, errors.New("invalid cache generation")
	}
	var generation journal.GenerationID
	copy(generation[:], decoded)
	return generation, nil
}

// openCachedTapePart coalesces concurrent recalls of one immutable generation.
func (s *tapePartStore) openCachedTapePart(ctx context.Context, partID partstore.PartId, entry indexEntry) (io.ReadCloser, error) {
	path := filepath.Join(s.cacheDir, entry.generation.String()+".part")
	if ok, err := s.validateCacheEntry(path, entry); err != nil {
		return nil, err
	} else if ok {
		reader, err := s.openCacheEntry(entry.generation)
		if err == nil {
			return reader, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	s.mu.Lock()
	if existing := s.recalls[entry.generation]; existing != nil {
		s.mu.Unlock()
		select {
		case <-existing.done:
			if existing.err != nil {
				return nil, existing.err
			}
			reader, err := s.openCacheEntry(entry.generation)
			if errors.Is(err, os.ErrNotExist) {
				// The leader's caller may have closed its reader and a very
				// small quota may already have evicted the file. Rejoin the
				// single-flight path instead of leaking an ENOENT to callers.
				return s.openCachedTapePart(ctx, partID, entry)
			}
			return reader, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	recall := &recallState{done: make(chan struct{})}
	s.recalls[entry.generation] = recall
	s.mu.Unlock()

	err := s.stageTapePart(ctx, path, partID, entry)
	s.mu.Lock()
	var reader io.ReadCloser
	if err == nil {
		reader, err = s.openCacheEntryLocked(entry.generation)
	}
	recall.err = err
	delete(s.recalls, entry.generation)
	close(recall.done)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

type readCacheFile struct {
	*os.File
	store      *tapePartStore
	generation journal.GenerationID
	once       bool
}

func (r *readCacheFile) Close() error {
	err := r.File.Close()
	r.store.mu.Lock()
	if !r.once {
		r.once = true
		if entry := r.store.cacheEntries[r.generation]; entry != nil && entry.readers > 0 {
			entry.readers--
		}
		r.store.evictReadCacheLocked(journal.GenerationID{})
	}
	r.store.mu.Unlock()
	return err
}

func (s *tapePartStore) openCacheEntry(generation journal.GenerationID) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openCacheEntryLocked(generation)
}

func (s *tapePartStore) openCacheEntryLocked(generation journal.GenerationID) (io.ReadCloser, error) {
	entry := s.cacheEntries[generation]
	if entry == nil {
		return nil, os.ErrNotExist
	}
	file, err := os.Open(entry.path)
	if err != nil {
		return nil, err
	}
	entry.readers++
	entry.lastAccess = time.Now()
	return &readCacheFile{File: file, store: s, generation: generation}, nil
}

func (s *tapePartStore) validateCacheEntry(path string, expected indexEntry) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if uint64(info.Size()) != expected.length {
		if err := s.removeCacheEntry(expected.generation, path); err != nil {
			return false, err
		}
		return false, nil
	}

	s.mu.Lock()
	cached := s.cacheEntries[expected.generation]
	if cached != nil && cached.verified && cached.size == info.Size() && cached.modTime.Equal(info.ModTime()) && cached.hash == expected.hash {
		cached.lastAccess = time.Now()
		s.mu.Unlock()
		return true, nil
	}
	s.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	hasher := sha256.New()
	_, hashErr := io.Copy(hasher, file)
	closeErr := file.Close()
	if hashErr != nil {
		return false, hashErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	var actual [32]byte
	copy(actual[:], hasher.Sum(nil))
	if actual != expected.hash {
		if err := s.removeCacheEntry(expected.generation, path); err != nil {
			return false, err
		}
		return false, nil
	}
	s.mu.Lock()
	s.cacheEntries[expected.generation] = &readCacheEntry{
		path:       path,
		size:       info.Size(),
		modTime:    info.ModTime(),
		lastAccess: time.Now(),
		hash:       actual,
		verified:   true,
	}
	s.mu.Unlock()
	return true, nil
}

func (s *tapePartStore) stageTapePart(ctx context.Context, path string, partID partstore.PartId, entry indexEntry) error {
	// Recheck after becoming the single-flight leader: a preceding recall may
	// have completed between the optimistic check and recall registration.
	if ok, err := s.validateCacheEntry(path, entry); err != nil || ok {
		return err
	}
	tmp, err := os.CreateTemp(s.cacheDir, ".staging-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	s.mu.Lock()
	scheduler := s.scheduler
	s.mu.Unlock()
	if scheduler == nil {
		_ = tmp.Close()
		return tapedev.ErrClosed
	}
	value, copyErr := scheduler.recall(ctx, entry.tapeBlock, func(jobCtx context.Context, device tapedev.Device) (any, error) {
		reader := s.newTapePartReader(jobCtx, device, partID, entry)
		written, err := io.Copy(tmp, reader)
		closeErr := reader.Close()
		if err != nil {
			return written, err
		}
		return written, closeErr
	})
	if copyErr != nil {
		_ = tmp.Close()
		return fmt.Errorf("staging tape part %s: %w", partID.String(), copyErr)
	}
	written, ok := value.(int64)
	if !ok {
		_ = tmp.Close()
		return errors.New("tape recall returned an invalid byte count")
	}
	if uint64(written) != entry.length {
		_ = tmp.Close()
		return fmt.Errorf("staging tape part %s: short copy %d of %d bytes", partID.String(), written, entry.length)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	s.mu.Lock()
	s.evictReadCacheLocked(entry.generation)
	s.mu.Unlock()
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := ioutils.SyncDirectory(s.cacheDir); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if previous := s.cacheEntries[entry.generation]; previous != nil {
		s.cacheBytes -= previous.size
	}
	s.cacheEntries[entry.generation] = &readCacheEntry{
		path:       path,
		size:       info.Size(),
		modTime:    info.ModTime(),
		lastAccess: time.Now(),
		hash:       entry.hash,
		verified:   true,
	}
	s.cacheBytes += info.Size()
	s.evictReadCacheLocked(entry.generation)
	s.mu.Unlock()
	return nil
}

func (s *tapePartStore) removeCacheEntry(generation journal.GenerationID, path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.mu.Lock()
	if cached := s.cacheEntries[generation]; cached != nil {
		s.cacheBytes -= cached.size
		delete(s.cacheEntries, generation)
	}
	s.mu.Unlock()
	return nil
}

// evictReadCacheLocked removes least-recently-used entries until the cache is
// within quota. preserve is the generation currently being staged.
func (s *tapePartStore) evictReadCacheLocked(preserve journal.GenerationID) {
	for s.cacheMaxBytes > 0 && s.cacheBytes > s.cacheMaxBytes {
		var oldestGeneration journal.GenerationID
		var oldest *readCacheEntry
		for generation, entry := range s.cacheEntries {
			if generation == preserve || s.recalls[generation] != nil || entry.readers > 0 {
				continue
			}
			if oldest == nil || entry.lastAccess.Before(oldest.lastAccess) {
				oldestGeneration = generation
				oldest = entry
			}
		}
		if oldest == nil {
			return
		}
		if err := os.Remove(oldest.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return
		}
		s.cacheBytes -= oldest.size
		delete(s.cacheEntries, oldestGeneration)
	}
}
