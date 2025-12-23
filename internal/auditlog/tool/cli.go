package tool

import (
	"bytes"
	"crypto/sha512"
	"fmt"
	"io"
	"os"

	"github.com/jdillenkofer/pithos/internal/auditlog"
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

func RunAuditLogTool(logPath string, verifier signing.Verifier, mlDsaVerifier signing.Verifier, format string, out io.Writer) error {
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
	var hashBuffer [][]byte
	idx := 0
	serializer := &serialization.BinarySerializer{}
	dec := serializer.NewDecoder(f)

	for {
		entry, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry %d: %w", idx, err)
		}

		// Verify Chain Integrity
		if idx == 0 {
			if entry.Type != auditlog.EntryTypeGenesis {
				return &VerificationError{idx, "first entry is not GENESIS"}
			}
			expectedPrev := sha512.Sum512([]byte("pithos"))
			if !bytes.Equal(entry.PreviousHash, expectedPrev[:]) {
				return &VerificationError{idx, "genesis previous hash invalid"}
			}
		} else {
			if !bytes.Equal(entry.PreviousHash, prevHash) {
				return &VerificationError{idx, fmt.Sprintf("chain break: expected prev hash %x, got %x", prevHash, entry.PreviousHash)}
			}
		}

		// Verify Entry Signature (Ed25519)
		if !entry.Verify(verifier) {
			return &VerificationError{idx, "entry signature invalid"}
		}

		// Grounding Verification
		if entry.Type == auditlog.EntryTypeLog {
			hashBuffer = append(hashBuffer, entry.Hash)
			if len(hashBuffer) > auditlog.GroundingBlockSize {
				return &VerificationError{idx, fmt.Sprintf("too many log entries without grounding: expected grounding after %d entries", auditlog.GroundingBlockSize)}
			}
		} else if entry.Type == auditlog.EntryTypeGrounding {
			if len(hashBuffer) != auditlog.GroundingBlockSize {
				return &VerificationError{idx, fmt.Sprintf("grounding entry appeared at wrong interval: expected %d entries, got %d", auditlog.GroundingBlockSize, len(hashBuffer))}
			}

			details, ok := entry.Details.(*auditlog.GroundingDetails)
			if !ok {
				return &VerificationError{idx, "invalid grounding details structure"}
			}

			// 1. Verify Merkle Root
			calculatedRoot := auditlog.CalculateMerkleRoot(hashBuffer)
			if !bytes.Equal(calculatedRoot, details.MerkleRootHash) {
				return &VerificationError{idx, fmt.Sprintf("merkle root mismatch: expected %x, got %x", details.MerkleRootHash, calculatedRoot)}
			}

			// 2. Verify Ed25519 signature of Merkle Root
			if !verifier.Verify(details.MerkleRootHash, details.SignatureEd25519) {
				return &VerificationError{idx, "merkle root Ed25519 signature invalid"}
			}

			// 3. Verify ML-DSA signature of Merkle Root
			if mlDsaVerifier != nil {
				if !mlDsaVerifier.Verify(details.MerkleRootHash, details.SignatureMlDsa) {
					return &VerificationError{idx, "merkle root ML-DSA signature invalid"}
				}
			}

			hashBuffer = nil // Reset for next block
		}
		
		if err := outputSink.WriteEntry(entry); err != nil {
			return err
		}

		prevHash = entry.Hash
		idx++
	}

	return nil
}
