package sink

import (
	"bytes"
	"testing"

	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
)

func TestMultiSinkSizeBytesSumsProviders(t *testing.T) {
	first := NewWriterSink(&bytes.Buffer{}, &serialization.TextSerializer{})
	second := NewWriterSink(&bytes.Buffer{}, &serialization.TextSerializer{})
	first.sizeBytes.Store(12)
	second.sizeBytes.Store(30)
	if got := NewMultiSink(first, second).SizeBytes(); got != 42 {
		t.Fatalf("SizeBytes() = %d, want 42", got)
	}
}
