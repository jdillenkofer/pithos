package tink

import (
	"bytes"
	"context"
	"crypto/mlkem"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// benchmarkPartStore keeps parts in memory. When seekable is false it hides
// io.Seeker so GetPart exercises the sequential (non-seekable) read path.
type benchmarkPartStore struct {
	mu       sync.Mutex
	parts    map[partstore.PartId][]byte
	seekable bool
}

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (readSeekNopCloser) Close() error { return nil }

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
	if s.seekable {
		return readSeekNopCloser{bytes.NewReader(data)}, nil
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *benchmarkPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	return nil, nil
}

func (s *benchmarkPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	return nil
}

func BenchmarkTinkEncryptionPartStore(b *testing.B) {
	ctx := context.Background()
	sizes := []struct {
		name string
		size int
	}{
		{"4KB", 4 * 1024},
		{"1MB", 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	seed := make([]byte, mlkem.SeedSize)
	rand.New(rand.NewSource(2)).Read(seed)
	mlkemKey, err := mlkem.NewDecapsulationKey1024(seed)
	if err != nil {
		b.Fatal(err)
	}
	modes := []struct {
		name     string
		mlkemKey *mlkem.DecapsulationKey1024
	}{
		{"classical", nil},
		{"hybrid", mlkemKey},
	}

	for _, mode := range modes {
		inner := &benchmarkPartStore{parts: map[partstore.PartId][]byte{}}
		// NewWithLocalKMS runs scrypt, so build one store per mode.
		store, err := NewWithLocalKMS("benchmark-password", inner, mode.mlkemKey)
		if err != nil {
			b.Fatal(err)
		}
		for _, s := range sizes {
			payload := make([]byte, s.size)
			rand.New(rand.NewSource(1)).Read(payload)
			partId, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("%s/%s", mode.name, s.name)

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

			for _, seekable := range []bool{true, false} {
				getName := "GetSequential/"
				if seekable {
					getName = "GetSeekable/"
				}
				b.Run(getName+name, func(b *testing.B) {
					inner.seekable = seekable
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					for b.Loop() {
						rc, err := store.GetPart(ctx, nil, *partId)
						if err != nil {
							b.Fatal(err)
						}
						n, err := io.Copy(io.Discard, rc)
						if err != nil {
							b.Fatal(err)
						}
						if n != int64(len(payload)) {
							b.Fatalf("read %d bytes, want %d", n, len(payload))
						}
						if err := rc.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}

			const tailSize = 4 * 1024
			b.Run("GetTail4KB/"+name, func(b *testing.B) {
				inner.seekable = true
				b.SetBytes(tailSize)
				b.ReportAllocs()
				for b.Loop() {
					rc, err := store.GetPart(ctx, nil, *partId)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := rc.(io.Seeker).Seek(-tailSize, io.SeekEnd); err != nil {
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
