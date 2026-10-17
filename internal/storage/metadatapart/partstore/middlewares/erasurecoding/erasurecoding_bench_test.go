package erasurecoding

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type benchmarkPartStore struct {
	mu    sync.Mutex
	parts map[partstore.PartId][]byte
	// bytesPerSecond throttles every part stream to simulate a backend with
	// limited bandwidth; 0 means unlimited.
	bytesPerSecond int64
	// broken stores report every part as missing and discard writes, so each
	// read takes the reconstruct-and-heal path.
	broken bool
}

func newBenchmarkPartStore(bytesPerSecond int64, broken bool) *benchmarkPartStore {
	return &benchmarkPartStore{parts: map[partstore.PartId][]byte{}, bytesPerSecond: bytesPerSecond, broken: broken}
}

func (s *benchmarkPartStore) Start(ctx context.Context) error { return nil }
func (s *benchmarkPartStore) Stop(ctx context.Context) error  { return nil }

func (s *benchmarkPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	data, err := io.ReadAll(s.throttle(reader))
	if err != nil {
		return err
	}
	if s.broken {
		return nil
	}
	s.mu.Lock()
	s.parts[partId] = data
	s.mu.Unlock()
	return nil
}

func (s *benchmarkPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	if s.broken {
		return nil, partstore.ErrPartNotFound
	}
	s.mu.Lock()
	data, ok := s.parts[partId]
	s.mu.Unlock()
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	return io.NopCloser(s.throttle(bytes.NewReader(data))), nil
}

func (s *benchmarkPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	return nil, nil
}

func (s *benchmarkPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	return nil
}

func (s *benchmarkPartStore) throttle(r io.Reader) io.Reader {
	if s.bytesPerSecond == 0 {
		return r
	}
	return &throttledReader{r: r, bytesPerSecond: s.bytesPerSecond}
}

type throttledReader struct {
	r              io.Reader
	bytesPerSecond int64
	start          time.Time
	read           int64
}

func (t *throttledReader) Read(p []byte) (int, error) {
	if t.start.IsZero() {
		t.start = time.Now()
	}
	n, err := t.r.Read(p)
	t.read += int64(n)
	due := t.start.Add(time.Duration(t.read * int64(time.Second) / t.bytesPerSecond))
	if wait := time.Until(due); wait > 0 {
		time.Sleep(wait)
	}
	return n, err
}

// BenchmarkErasureCodingPartStore uses 4 data + 2 parity shards with the
// default 64KB stripe shard size. "throttled" backends are limited to
// 100MB/s each, so the result shows whether shards move in parallel.
func BenchmarkErasureCodingPartStore(b *testing.B) {
	ctx := context.Background()
	const dataShards, parityShards = 4, 2
	sizes := []struct {
		name string
		size int
	}{
		{"64KB", 64 * 1024},
		{"4MB", 4 * 1024 * 1024},
	}
	backends := []struct {
		name           string
		bytesPerSecond int64
	}{
		{"memory", 0},
		{"throttled", 100 * 1024 * 1024},
	}

	for _, backend := range backends {
		for _, s := range sizes {
			payload := make([]byte, s.size)
			rand.New(rand.NewSource(1)).Read(payload)

			newStore := func(b *testing.B, brokenShard int) partstore.PartStore {
				stores := make([]partstore.PartStore, dataShards+parityShards)
				for i := range stores {
					stores[i] = newBenchmarkPartStore(backend.bytesPerSecond, i == brokenShard)
				}
				store, err := NewWithPartStores(dataShards, parityShards, 64*1024, stores, WithHealScanInterval(0))
				if err != nil {
					b.Fatal(err)
				}
				return store
			}
			getPart := func(b *testing.B, store partstore.PartStore, partId partstore.PartId) {
				rc, err := store.GetPart(ctx, nil, partId)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, rc); err != nil {
					b.Fatal(err)
				}
				if err := rc.Close(); err != nil {
					b.Fatal(err)
				}
			}
			partId, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("%s/%s", backend.name, s.name)

			b.Run("Put/"+name, func(b *testing.B) {
				store := newStore(b, -1)
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				for b.Loop() {
					if err := store.PutPart(ctx, nil, *partId, bytes.NewReader(payload)); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("Get/"+name, func(b *testing.B) {
				store := newStore(b, -1)
				if err := store.PutPart(ctx, nil, *partId, bytes.NewReader(payload)); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				for b.Loop() {
					getPart(b, store, *partId)
				}
			})

			b.Run("GetDegraded/"+name, func(b *testing.B) {
				store := newStore(b, 0)
				if err := store.PutPart(ctx, nil, *partId, bytes.NewReader(payload)); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				for b.Loop() {
					getPart(b, store, *partId)
				}
			})
		}
	}
}
