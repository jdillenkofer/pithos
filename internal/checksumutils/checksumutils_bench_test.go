package checksumutils

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

func benchmarkCalculateChecksumsStreaming(b *testing.B, size int64) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	b.SetBytes(size)
	b.ResetTimer()
	for b.Loop() {
		_, _, err := CalculateChecksumsStreaming(context.Background(), bytes.NewReader(data), func(reader io.Reader) error {
			_, err := ioutils.Copy(io.Discard, reader)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCalculateChecksumsStreaming1MB(b *testing.B) {
	benchmarkCalculateChecksumsStreaming(b, 1000*1000)
}

func BenchmarkCalculateChecksumsStreaming64MB(b *testing.B) {
	benchmarkCalculateChecksumsStreaming(b, 64*1000*1000)
}
