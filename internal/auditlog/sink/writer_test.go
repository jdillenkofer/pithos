package sink

import (
	"bytes"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	_ "github.com/jdillenkofer/pithos/internal/testing"
)

func TestWriterSink_WriteEntry(t *testing.T) {
	var buf bytes.Buffer
	serializer := &serialization.TextSerializer{}
	sink := NewWriterSink(&buf, serializer)

	entry := &auditlog.Entry{
		Version:   1,
		Timestamp: time.Date(2025, 12, 23, 10, 0, 0, 0, time.UTC),
		Operation: auditlog.OpPutObject,
		Phase:     auditlog.PhaseComplete,
		Bucket:    "test-bucket",
		Key:       "test-key",
	}

	if err := sink.WriteEntry(entry); err != nil {
		t.Fatalf("WriteEntry failed: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Error("Output is empty")
	}
	
	// Basic check if output contains expected string
	if !bytes.Contains(buf.Bytes(), []byte("test-bucket")) {
		t.Errorf("Output does not contain bucket name. Got: %s", output)
	}
}
