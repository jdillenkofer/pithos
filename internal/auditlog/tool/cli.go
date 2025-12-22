package tool

import (
	"crypto/sha512"
	"fmt"
	"io"
	"os"

	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
)

type VerificationError struct {
	EntryIndex int
	Reason     string
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("verification failed at entry %d: %s", e.EntryIndex, e.Reason)
}

func RunAuditLogTool(logPath string, verifier signing.Verifier, format string, out io.Writer) error {
	f, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var outputSink sink.Sink
	switch format {
	case "json":
		outputSink = sink.NewWriterSink(out, &serialization.JsonSerializer{Indent: true})
	case "text":
		outputSink = sink.NewWriterSink(out, &serialization.TextSerializer{})
	case "bin":
		outputSink = sink.NewWriterSink(out, &serialization.BinarySerializer{})
	default:
		return fmt.Errorf("unknown format: %s", format)
	}

	var prevHash []byte
	idx := 0
	serializer := &serialization.BinarySerializer{}
	dec := serializer.NewDecoder(f)

	for {
		entry, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			// If it's a real error (unexpected EOF), it might mean corruption
			return fmt.Errorf("failed to read entry %d: %w", idx, err)
		}

		// Verify Genesis
		if idx == 0 {
			if entry.Operation != "GENESIS" {
				return &VerificationError{idx, "first entry is not GENESIS"}
			}
			expectedPrev := sha512.Sum512([]byte("pithos"))
			if string(entry.PreviousHash) != string(expectedPrev[:]) {
				return &VerificationError{idx, "genesis previous hash invalid"}
			}
		} else {
			if string(entry.PreviousHash) != string(prevHash) {
				return &VerificationError{idx, fmt.Sprintf("chain break: expected prev hash %x, got %x", prevHash, entry.PreviousHash)}
			}
		}

		// Verify Signature
		if !entry.Verify(verifier) {
			return &VerificationError{idx, "signature invalid"}
		}
		
		if err := outputSink.WriteEntry(entry); err != nil {
			return err
		}

		prevHash = entry.Hash
		idx++
	}

	return nil
}