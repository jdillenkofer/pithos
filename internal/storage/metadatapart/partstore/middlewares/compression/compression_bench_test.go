package compression

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type benchmarkPartStore struct {
	mu    sync.Mutex
	parts map[partstore.PartId][]byte
}

func (s *benchmarkPartStore) Start(ctx context.Context) error { return nil }
func (s *benchmarkPartStore) Stop(ctx context.Context) error  { return nil }

func (s *benchmarkPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.parts[partId] = data
	s.mu.Unlock()
	return nil
}

func (s *benchmarkPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.parts[partId]
	s.mu.Unlock()
	if !ok {
		return nil, partstore.ErrPartNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *benchmarkPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	return nil, nil
}

func (s *benchmarkPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	return nil
}

func compressiblePayload(size int) []byte {
	rng := rand.New(rand.NewSource(1))
	var buf bytes.Buffer
	for buf.Len() < size {
		fmt.Fprintf(&buf, "{\"id\":%d,\"name\":\"user-%d\",\"score\":%d}\n", rng.Intn(1_000_000), rng.Intn(1000), rng.Intn(100))
	}
	return buf.Bytes()[:size]
}

func randomPayload(size int) []byte {
	payload := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(payload)
	return payload
}

func BenchmarkCompressionPartStore(b *testing.B) {
	ctx := context.Background()
	sizes := []struct {
		name string
		size int
	}{
		{"4KB", 4 * 1024},
		{"1MB", 1024 * 1024},
	}
	payloads := []struct {
		name string
		gen  func(int) []byte
	}{
		{"compressible", compressiblePayload},
		{"random", randomPayload},
	}

	for _, algorithm := range []Algorithm{AlgorithmZstd, AlgorithmGzip} {
		for _, p := range payloads {
			for _, s := range sizes {
				payload := p.gen(s.size)
				inner := &benchmarkPartStore{parts: map[partstore.PartId][]byte{}}
				store, err := NewWithConfig(inner, Config{Algorithm: algorithm})
				if err != nil {
					b.Fatal(err)
				}
				partId, err := partstore.NewRandomPartId()
				if err != nil {
					b.Fatal(err)
				}
				name := fmt.Sprintf("%s/%s/%s", algorithm, p.name, s.name)

				b.Run("Put/"+name, func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					for b.Loop() {
						if err := store.PutPart(ctx, nil, *partId, bytes.NewReader(payload)); err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run("PutParallel/"+name, func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := store.PutPart(ctx, nil, *partId, bytes.NewReader(payload)); err != nil {
								b.Error(err)
								return
							}
						}
					})
				})

				b.Run("Get/"+name, func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					for b.Loop() {
						rc, err := store.GetPart(ctx, nil, *partId)
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
				})
			}
		}
	}
}
