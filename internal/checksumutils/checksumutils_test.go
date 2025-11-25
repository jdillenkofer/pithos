package checksumutils

import (
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

const numTestRuns = 100

func TestCrc32Combine(t *testing.T) {
	testutils.SkipIfIntegration(t)
	r := rand.New(rand.NewSource(int64(1337)))
	t.Parallel()
	for i := range numTestRuns {
		size := r.Intn(999) + 1
		testData := make([]byte, size)
		r.Read(testData)

		split := r.Intn(size)
		testDataSlice1 := testData[0:split]
		testDataSlice2 := testData[split:]

		t.Run("TestCrc32Combine "+strconv.Itoa(i), func(t *testing.T) {
			crc32Hash := crc32.NewIEEE()
			crc32Hash.Write(testData)
			fullDigest := crc32Hash.Sum([]byte{})

			crc32Hash.Reset()
			crc32Hash.Write(testDataSlice1)
			slice1Digest := crc32Hash.Sum([]byte{})

			crc32Hash.Reset()
			crc32Hash.Write(testDataSlice2)
			slice2Digest := crc32Hash.Sum([]byte{})

			combinedFullDigest := CombineCrc32(slice1Digest, slice2Digest, int64(len(testDataSlice2)))

			assert.Equal(t, fullDigest, combinedFullDigest)
		})
	}
}

func TestCrc32cCombine(t *testing.T) {
	testutils.SkipIfIntegration(t)
	r := rand.New(rand.NewSource(int64(1337)))
	t.Parallel()
	for i := range numTestRuns {
		size := r.Intn(999) + 1
		testData := make([]byte, size)
		r.Read(testData)

		split := r.Intn(size)
		testDataSlice1 := testData[0:split]
		testDataSlice2 := testData[split:]

		t.Run("TestCrc32cCombine "+strconv.Itoa(i), func(t *testing.T) {
			crc32cHash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
			crc32cHash.Write(testData)
			fullDigest := crc32cHash.Sum([]byte{})

			crc32cHash.Reset()
			crc32cHash.Write(testDataSlice1)
			slice1Digest := crc32cHash.Sum([]byte{})

			crc32cHash.Reset()
			crc32cHash.Write(testDataSlice2)
			slice2Digest := crc32cHash.Sum([]byte{})

			combinedFullDigest := CombineCrc32c(slice1Digest, slice2Digest, int64(len(testDataSlice2)))

			assert.Equal(t, fullDigest, combinedFullDigest)
		})
	}
}

func TestCrc64NvmeCombine(t *testing.T) {
	testutils.SkipIfIntegration(t)
	r := rand.New(rand.NewSource(int64(1337)))
	t.Parallel()
	for i := range numTestRuns {
		size := r.Intn(999) + 1
		testData := make([]byte, size)
		r.Read(testData)

		split := r.Intn(size)
		testDataSlice1 := testData[0:split]
		testDataSlice2 := testData[split:]

		t.Run("TestCrc64NvmeCombine "+strconv.Itoa(i), func(t *testing.T) {
			crc64NvmeHash := crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
			crc64NvmeHash.Write(testData)
			fullDigest := crc64NvmeHash.Sum([]byte{})

			crc64NvmeHash.Reset()
			crc64NvmeHash.Write(testDataSlice1)
			slice1Digest := crc64NvmeHash.Sum([]byte{})

			crc64NvmeHash.Reset()
			crc64NvmeHash.Write(testDataSlice2)
			slice2Digest := crc64NvmeHash.Sum([]byte{})

			combinedFullDigest := CombineCrc64Nvme(slice1Digest, slice2Digest, int64(len(testDataSlice2)))

			assert.Equal(t, fullDigest, combinedFullDigest)
		})
	}
}

// DelayedReader wraps an io.Reader and sleeps after each Read.
type DelayedReader struct {
	R     io.Reader
	Delay time.Duration
}

func (d *DelayedReader) Read(p []byte) (int, error) {
	n, err := d.R.Read(p)
	time.Sleep(d.Delay)
	return n, err
}

// DelayedWriter wraps an io.Writer and sleeps after each Write.
type DelayedWriter struct {
	W     io.Writer
	Delay time.Duration
}

func (d *DelayedWriter) Write(p []byte) (int, error) {
	n, err := d.W.Write(p)
	time.Sleep(d.Delay)
	return n, err
}

func SetupBenchmark(dataSize int64, readerDelay time.Duration, writerDelay time.Duration) (io.Reader, func(io.Reader) error, int64) {
	baseReader := io.LimitReader(rand.New(rand.NewSource(1337)), dataSize)
	inputReader := &DelayedReader{R: baseReader, Delay: readerDelay}
	discardReadCallback := func(reader io.Reader) error {
		writer := &DelayedWriter{W: io.Discard, Delay: writerDelay}
		ioutils.Copy(writer, reader)
		return nil
	}
	return inputReader, discardReadCallback, dataSize
}

func BenchmarkCalculateChecksumsStreaming(b *testing.B) {
	const dataSize = int64(100 * 1024 * 1024)
	const readerDelay = 5 * time.Millisecond
	const writerDelay = 2 * time.Millisecond

	b.Run("CalculateChecksumsStreamingUsingSingleHashWriter", func(b *testing.B) {
		ctx := b.Context()
		inputReader, discardReadCallback, dataSize := SetupBenchmark(dataSize, readerDelay, writerDelay)
		b.SetBytes(dataSize)
		for b.Loop() {
			CalculateChecksumsStreamingUsingSingleHashWriter(ctx, inputReader, discardReadCallback)
		}
	})

	b.Run("CalculateChecksumsStreamingUsingPipe", func(b *testing.B) {
		ctx := b.Context()
		inputReader, discardReadCallback, dataSize := SetupBenchmark(dataSize, readerDelay, writerDelay)
		b.SetBytes(dataSize)
		for b.Loop() {
			CalculateChecksumsStreamingUsingPipe(ctx, inputReader, discardReadCallback)
		}
	})
}
