package tape

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
	"github.com/jdillenkofer/pithos/internal/tape/simulator"
)

const productionBenchmarkPartBytes = 64 << 20
const productionBenchmarkLargeSegmentBytes = 256 << 20
const productionBenchmarkTargetSegmentBytes = 1 << 30

type benchmarkZeroReader struct{}

func (benchmarkZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// BenchmarkTapeProductionLTO9 measures the actual part-store path with the
// simulator's LTO-9 transfer, filemark, and seek timings. Setup and cartridge
// load are excluded. Run with:
//
//	go test ./internal/storage/metadatapart/partstore/tape -run '^$' \
//	  -bench '^BenchmarkTapeProductionLTO9$' -benchtime=1x
func BenchmarkTapeProductionLTO9(b *testing.B) {
	profile, ok := simulator.LTOProfile(9)
	if !ok {
		b.Fatal("LTO-9 profile unavailable")
	}
	profile.LoadTime = 0
	content := bytes.Repeat([]byte{0x5a}, productionBenchmarkPartBytes)

	b.Run("journal-put-64MiB", func(b *testing.B) {
		root := b.TempDir()
		store := openBenchmarkTapeStore(b, root, simulator.LatencyProfile{})
		defer store.Stop(context.Background())
		b.SetBytes(productionBenchmarkPartBytes)
		b.ResetTimer()
		for b.Loop() {
			partID, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			if err := store.PutPart(context.Background(), nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("migrate-segment-64MiB", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			root := b.TempDir()
			initializeBenchmarkVolume(b, root)
			store := openBenchmarkTapeStore(b, root, profile)
			partID, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			if err := store.PutPart(context.Background(), nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(productionBenchmarkPartBytes)
			b.StartTimer()
			store.runMigration(context.Background(), true)
			b.StopTimer()
			if store.index[*partID].location != locationTape {
				b.Fatal("part did not migrate")
			}
			if err := store.Stop(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("migrate-segment-256MiB", func(b *testing.B) {
		largeContent := bytes.Repeat([]byte{0xa5}, productionBenchmarkLargeSegmentBytes)
		for range b.N {
			b.StopTimer()
			root := b.TempDir()
			initializeBenchmarkVolume(b, root)
			store := openBenchmarkTapeStore(b, root, profile)
			partID, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			if err := store.PutPart(context.Background(), nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(largeContent)); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(productionBenchmarkLargeSegmentBytes)
			b.StartTimer()
			store.runMigration(context.Background(), true)
			b.StopTimer()
			if store.index[*partID].location != locationTape {
				b.Fatal("part did not migrate")
			}
			if err := store.Stop(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("migrate-segment-1GiB", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			root := b.TempDir()
			initializeBenchmarkVolume(b, root)
			store := openBenchmarkTapeStore(b, root, profile)
			partID, err := partstore.NewRandomPartId()
			if err != nil {
				b.Fatal(err)
			}
			content := io.LimitReader(benchmarkZeroReader{}, productionBenchmarkTargetSegmentBytes)
			if err := store.PutPart(context.Background(), nil, *partID, partstore.PutPartOptions{}, content); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(productionBenchmarkTargetSegmentBytes)
			b.StartTimer()
			store.runMigration(context.Background(), true)
			b.StopTimer()
			if store.index[*partID].location != locationTape {
				b.Fatal("part did not migrate")
			}
			if err := store.Stop(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("recall-miss-64MiB", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			root := b.TempDir()
			partID := prepareBenchmarkTapePart(b, root, content)
			store := openBenchmarkTapeStore(b, root, profile)
			b.SetBytes(productionBenchmarkPartBytes)
			b.StartTimer()
			reader, err := store.GetPart(context.Background(), nil, partID)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, reader); err != nil {
				b.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := store.Stop(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cache-hit-64MiB", func(b *testing.B) {
		root := b.TempDir()
		partID := prepareBenchmarkTapePart(b, root, content)
		store := openBenchmarkTapeStore(b, root, profile)
		defer store.Stop(context.Background())
		reader, err := store.GetPart(context.Background(), nil, partID)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(productionBenchmarkPartBytes)
		b.ResetTimer()
		for b.Loop() {
			reader, err := store.GetPart(context.Background(), nil, partID)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, reader); err != nil {
				b.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func initializeBenchmarkVolume(b *testing.B, root string) {
	b.Helper()
	store := openBenchmarkTapeStore(b, root, simulator.LatencyProfile{})
	if err := store.Stop(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func prepareBenchmarkTapePart(b *testing.B, root string, content []byte) partstore.PartId {
	b.Helper()
	store := openBenchmarkTapeStore(b, root, simulator.LatencyProfile{})
	partID, err := partstore.NewRandomPartId()
	if err != nil {
		b.Fatal(err)
	}
	if err := store.PutPart(context.Background(), nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)); err != nil {
		b.Fatal(err)
	}
	store.runMigration(context.Background(), true)
	if err := store.Stop(context.Background()); err != nil {
		b.Fatal(err)
	}
	return *partID
}

func openBenchmarkTapeStore(b *testing.B, root string, latency simulator.LatencyProfile) *tapePartStore {
	b.Helper()
	policy := DefaultPackingPolicy()
	policy.MaxBytes = 4 << 30
	policy.TargetBytes = policy.MaxBytes
	policy.MaxWait = time.Hour
	value, err := New(func(ctx context.Context) (tapedev.Device, error) {
		return simulator.Open(ctx, filepath.Join(root, "tape.sim"), simulator.Options{Latency: latency})
	}, WithJournalDir(filepath.Join(root, "journal")), WithReadCacheDir(filepath.Join(root, "cache")), WithRecordSize(1<<20), WithPackingPolicy(policy))
	if err != nil {
		b.Fatal(err)
	}
	store := value.(*tapePartStore)
	if err := store.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	return store
}
