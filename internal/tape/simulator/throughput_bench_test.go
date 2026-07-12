package simulator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/tape"
)

const benchmarkRangeSize = 8 << 20

// BenchmarkLTOThroughputMatrix performs real I/O through the tape simulator.
// It uses at most 128 MiB of fixture data at a time. PITHOS_TAPE_BENCH_DIR can
// point at a mounted RAM disk; Linux uses /dev/shm automatically. All fixtures
// are removed after their sub-benchmark. Record sizes of 256 KiB, 1 MiB and
// 4 MiB are measured. Run the complete matrix:
//
//	go test ./internal/tape/simulator -run '^$' \
//	  -bench '^BenchmarkLTOThroughputMatrix$' -benchtime=1x
//
// The benchmark uses an 8 MiB file/range size. Drive load time is excluded:
// each result measures an already loaded/mounted tape.
func BenchmarkLTOThroughputMatrix(b *testing.B) {
	ctx := context.Background()
	sizes := []int64{8 << 20, 32 << 20, 64 << 20}
	recordSizes := []int{256 << 10, 1 << 20, 4 << 20}

	for _, recordSize := range recordSizes {
		b.Run(fmt.Sprintf("record-%dKiB", recordSize>>10), func(b *testing.B) {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("data-%dMiB", size>>20), func(b *testing.B) {
					fixturePath := filepath.Join(benchmarkTempDir(b), "read.ptape")
					writeTapeFixture(b, fixturePath, size, recordSize)

					for generation := 1; generation <= 10; generation++ {
						profile, ok := LTOProfile(generation)
						if !ok {
							b.Fatalf("missing LTO-%d profile", generation)
						}
						// Loading is a mount-time cost, not transfer throughput.
						profile.LoadTime = 0

						b.Run(fmt.Sprintf("LTO-%02d", generation), func(b *testing.B) {
							b.Run("read", func(b *testing.B) {
								dev := openBenchmarkDevice(b, fixturePath, profile)
								benchmarkReadFull(b, ctx, dev, size, recordSize)
							})
							b.Run("read-range-seq", func(b *testing.B) {
								dev := openBenchmarkDevice(b, fixturePath, profile)
								order := makeRangeOrder(size, false)
								benchmarkReadRanges(b, ctx, dev, size, recordSize, order)
							})
							b.Run("read-range-random", func(b *testing.B) {
								dev := openBenchmarkDevice(b, fixturePath, profile)
								order := makeRangeOrder(size, true)
								benchmarkReadRanges(b, ctx, dev, size, recordSize, order)
							})
							b.Run("write", func(b *testing.B) {
								writePath := filepath.Join(benchmarkTempDir(b), "write.ptape")
								dev := openBenchmarkDevice(b, writePath, profile)
								benchmarkWrite(b, ctx, dev, size, recordSize)
							})
						})
					}
				})
			}
		})
	}
}

func benchmarkTempDir(b *testing.B) string {
	b.Helper()
	base := os.Getenv("PITHOS_TAPE_BENCH_DIR")
	if base == "" {
		if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
			base = "/dev/shm"
		}
	}
	if base == "" {
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(base, "pithos-tape-bench-")
	if err != nil {
		b.Fatalf("creating benchmark directory under %s: %v", base, err)
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Errorf("removing benchmark directory %s: %v", dir, err)
		}
	})
	return dir
}

func writeTapeFixture(b *testing.B, path string, size int64, recordSize int) {
	b.Helper()
	dev, err := Open(context.Background(), path, Options{})
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, recordSize)
	for written := int64(0); written < size; {
		partBytes := min(int64(benchmarkRangeSize), size-written)
		for partWritten := int64(0); partWritten < partBytes; {
			n := min(int64(len(buf)), partBytes-partWritten)
			if err := dev.WriteRecord(context.Background(), buf[:n]); err != nil {
				b.Fatal(err)
			}
			partWritten += n
			written += n
		}
		if err := dev.WriteFilemarks(context.Background(), 1); err != nil {
			b.Fatal(err)
		}
	}
	if err := dev.Close(); err != nil {
		b.Fatal(err)
	}
}

func openBenchmarkDevice(b *testing.B, path string, profile LatencyProfile) *Device {
	b.Helper()
	dev, err := Open(context.Background(), path, Options{Latency: profile})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := dev.Close(); err != nil {
			b.Error(err)
		}
	})
	return dev
}

func benchmarkReadFull(b *testing.B, ctx context.Context, dev *Device, size int64, recordSize int) {
	b.Helper()
	buf := make([]byte, recordSize)
	b.SetBytes(size)
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		if err := dev.Rewind(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		var read int64
		for read < size {
			n, err := dev.ReadRecord(ctx, buf)
			if errors.Is(err, tape.ErrFilemark) {
				continue
			}
			if err != nil {
				b.Fatal(err)
			}
			read += int64(n)
		}
	}
}

func benchmarkReadRanges(b *testing.B, ctx context.Context, dev *Device, size int64, recordSize int, order []int) {
	b.Helper()
	buf := make([]byte, recordSize)
	b.SetBytes(size)
	b.ResetTimer()
	for b.Loop() {
		for _, rangeIndex := range order {
			// Each complete 8 MiB range occupies eight records followed by one
			// filemark in the fixture.
			startBlock := uint64(rangeIndex * (benchmarkRangeSize/recordSize + 1))
			if err := dev.LocateBlock(ctx, startBlock); err != nil {
				b.Fatal(err)
			}
			rangeStart := int64(rangeIndex * benchmarkRangeSize)
			rangeBytes := min(int64(benchmarkRangeSize), size-rangeStart)
			var read int64
			for read < rangeBytes {
				n, err := dev.ReadRecord(ctx, buf)
				if err != nil {
					b.Fatal(err)
				}
				read += int64(n)
			}
		}
	}
}

func benchmarkWrite(b *testing.B, ctx context.Context, dev *Device, size int64, recordSize int) {
	b.Helper()
	buf := make([]byte, recordSize)
	b.SetBytes(size)
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		if err := dev.Rewind(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for written := int64(0); written < size; {
			partBytes := min(int64(benchmarkRangeSize), size-written)
			for partWritten := int64(0); partWritten < partBytes; {
				n := min(int64(len(buf)), partBytes-partWritten)
				if err := dev.WriteRecord(ctx, buf[:n]); err != nil {
					b.Fatal(err)
				}
				partWritten += n
				written += n
			}
			if err := dev.WriteFilemarks(ctx, 1); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func makeRangeOrder(size int64, random bool) []int {
	count := int((size + benchmarkRangeSize - 1) / benchmarkRangeSize)
	if random {
		order := rand.New(rand.NewSource(1)).Perm(count)
		sequential := true
		for i, value := range order {
			if value != i {
				sequential = false
				break
			}
		}
		// Small seeded permutations can occasionally be the identity. Force a
		// non-sequential order so the random-range case always measures seeks.
		if sequential && count > 2 {
			order[1], order[count-1] = order[count-1], order[1]
		}
		return order
	}
	order := make([]int, count)
	for i := range order {
		order[i] = i
	}
	return order
}
