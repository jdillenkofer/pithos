package tool

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
)

type VerificationError struct {
	EntryIndex int
	Reason     string
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("verification failed at entry %d: %s", e.EntryIndex, e.Reason)
}

type AuditLogTool struct {
	logPath       string
	verifier      signing.Verifier
	mlDsaVerifier signing.Verifier
}

func NewAuditLogTool(logPath string, verifier signing.Verifier, mlDsaVerifier signing.Verifier) *AuditLogTool {
	return &AuditLogTool{
		logPath:       logPath,
		verifier:      verifier,
		mlDsaVerifier: mlDsaVerifier,
	}
}

func (t *AuditLogTool) getDecoder(r io.Reader, format string) (serialization.Decoder, error) {
	switch format {
	case "bin":
		return (&serialization.BinarySerializer{}).NewDecoder(r), nil
	case "json":
		return (&serialization.JsonSerializer{}).NewDecoder(r), nil
	case "text":
		return (&serialization.TextSerializer{}).NewDecoder(r), nil
	default:
		return nil, fmt.Errorf("unknown input format: %s", format)
	}
}

func (t *AuditLogTool) Verify(inputFormat string) error {
	f, err := os.Open(t.logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec, err := t.getDecoder(f, inputFormat)
	if err != nil {
		return err
	}

	var prevHash []byte
	var hashBuffer [][]byte
	idx := 0

	for {
		entry, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry %d: %w", idx, err)
		}

		if err := t.verifyEntry(entry, idx, &prevHash, &hashBuffer); err != nil {
			return err
		}

		prevHash = entry.Hash
		idx++
	}

	return nil
}

func (t *AuditLogTool) Dump(inputFormat string, outputFormat string, out io.Writer) error {
	f, err := os.Open(t.logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec, err := t.getDecoder(f, inputFormat)
	if err != nil {
		return err
	}

	var outputSink sink.Sink
	switch outputFormat {
	case "json":
		outputSink = sink.NewWriterSink(out, &serialization.JsonSerializer{Indent: true})
	case "text":
		outputSink = sink.NewWriterSink(out, &serialization.TextSerializer{})
	case "bin":
		outputSink = sink.NewWriterSink(out, &serialization.BinarySerializer{})
	default:
		return fmt.Errorf("unknown output format: %s", outputFormat)
	}

	var prevHash []byte
	var hashBuffer [][]byte
	idx := 0

	for {
		entry, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry %d: %w", idx, err)
		}

		if err := t.verifyEntry(entry, idx, &prevHash, &hashBuffer); err != nil {
			return err
		}

		if err := outputSink.WriteEntry(entry); err != nil {
			return err
		}

		prevHash = entry.Hash
		idx++
	}

	return nil
}

type LogStats struct {
	TotalEntries    int
	GenesisEntries  int
	LogEntries      int
	GroundingEntries int
	StartTime       time.Time
	EndTime         time.Time
	Operations      map[auditlog.Operation]int
	Actors          map[string]int
	Errors          int
}

func (t *AuditLogTool) Stats(inputFormat string) (*LogStats, error) {
	f, err := os.Open(t.logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec, err := t.getDecoder(f, inputFormat)
	if err != nil {
		return nil, err
	}

	stats := &LogStats{
		Operations: make(map[auditlog.Operation]int),
		Actors:     make(map[string]int),
	}

	var prevHash []byte
	var hashBuffer [][]byte
	idx := 0

	for {
		entry, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read entry %d: %w", idx, err)
		}

		if err := t.verifyEntry(entry, idx, &prevHash, &hashBuffer); err != nil {
			return nil, err
		}

		stats.TotalEntries++
		if stats.StartTime.IsZero() || entry.Timestamp.Before(stats.StartTime) {
			stats.StartTime = entry.Timestamp
		}
		if entry.Timestamp.After(stats.EndTime) {
			stats.EndTime = entry.Timestamp
		}

		switch entry.Type {
		case auditlog.EntryTypeGenesis:
			stats.GenesisEntries++
		case auditlog.EntryTypeLog:
			stats.LogEntries++
			details := entry.Details.(*auditlog.LogDetails)
			stats.Operations[details.Operation]++
			stats.Actors[details.Actor]++
			if details.Error != "" {
				stats.Errors++
			}
		case auditlog.EntryTypeGrounding:
			stats.GroundingEntries++
		}

		prevHash = entry.Hash
		idx++
	}

	return stats, nil
}

type AuditKeys struct {
	Ed25519Pub  []byte
	Ed25519Priv []byte
	MlDsaPub    []byte
	MlDsaPriv   []byte
}

func (t *AuditLogTool) GenerateAuditKeys() (*AuditKeys, error) {
	// Ed25519
	edPub, edPriv, err := signing.GenerateEd25519KeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Ed25519 key: %w", err)
	}

	// ML-DSA
	mlPub, mlPriv, err := signing.GenerateMlDsaKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ML-DSA key: %w", err)
	}

	return &AuditKeys{
		Ed25519Pub:  edPub,
		Ed25519Priv: edPriv,
		MlDsaPub:    mlPub,
		MlDsaPriv:   mlPriv,
	}, nil
}

func (t *AuditLogTool) Keygen(out io.Writer) error {
	keys, err := t.GenerateAuditKeys()
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "--- Audit Log Key Pairs ---\n\n")
	fmt.Fprintf(out, "Ed25519 (Used for per-entry signing):\n")
	fmt.Fprintf(out, "  Private Key: %s\n", base64.StdEncoding.EncodeToString(keys.Ed25519Priv))
	fmt.Fprintf(out, "  Public Key:  %s\n\n", base64.StdEncoding.EncodeToString(keys.Ed25519Pub))

	fmt.Fprintf(out, "ML-DSA-65 (Used for Post-Quantum grounding):\n")
	fmt.Fprintf(out, "  Private Key: %s\n", base64.StdEncoding.EncodeToString(keys.MlDsaPriv))
	fmt.Fprintf(out, "  Public Key:  %s\n\n", base64.StdEncoding.EncodeToString(keys.MlDsaPub))

	fmt.Fprintf(out, "Keep private keys secure. You need them in your storage.json configuration.\n")
	fmt.Fprintf(out, "Use public keys for verification with 'audit-log verify'.\n")

	return nil
}

func (t *AuditLogTool) WriteKeypair(path string, priv []byte, pub []byte) error {
	privPath := path
	pubPath := path + ".pub"

	if err := os.WriteFile(privPath, []byte(base64.StdEncoding.EncodeToString(priv)+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(pubPath, []byte(base64.StdEncoding.EncodeToString(pub)+"\n"), 0644); err != nil {
		return err
	}
	return nil
}

func (t *AuditLogTool) verifyEntry(entry *auditlog.Entry, idx int, prevHash *[]byte, hashBuffer *[][]byte) error {
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
		if !bytes.Equal(entry.PreviousHash, *prevHash) {
			return &VerificationError{idx, fmt.Sprintf("chain break: expected prev hash %x, got %x", *prevHash, entry.PreviousHash)}
		}
	}

	// Verify Entry Signature (Ed25519)
	if !entry.Verify(t.verifier) {
		return &VerificationError{idx, "entry signature invalid"}
	}

	// Grounding Verification
	switch entry.Type {
	case auditlog.EntryTypeLog:
		*hashBuffer = append(*hashBuffer, entry.Hash)
		if len(*hashBuffer) > auditlog.GroundingBlockSize {
			return &VerificationError{idx, fmt.Sprintf("too many log entries without grounding: expected grounding after %d entries", auditlog.GroundingBlockSize)}
		}
	case auditlog.EntryTypeGrounding:
		if len(*hashBuffer) != auditlog.GroundingBlockSize {
			return &VerificationError{idx, fmt.Sprintf("grounding entry appeared at wrong interval: expected %d entries, got %d", auditlog.GroundingBlockSize, len(*hashBuffer))}
		}

		details, ok := entry.Details.(*auditlog.GroundingDetails)
		if !ok {
			return &VerificationError{idx, "invalid grounding details structure"}
		}

		// 1. Verify Merkle Root
		calculatedRoot := auditlog.CalculateMerkleRoot(*hashBuffer)
		if !bytes.Equal(calculatedRoot, details.MerkleRootHash) {
			return &VerificationError{idx, fmt.Sprintf("merkle root mismatch: expected %x, got %x", details.MerkleRootHash, calculatedRoot)}
		}

		// 2. Verify Ed25519 signature of Merkle Root
		if !t.verifier.Verify(details.MerkleRootHash, details.SignatureEd25519) {
			return &VerificationError{idx, "merkle root Ed25519 signature invalid"}
		}

		// 3. Verify ML-DSA signature of Merkle Root
		if t.mlDsaVerifier != nil {
			if !t.mlDsaVerifier.Verify(details.MerkleRootHash, details.SignatureMlDsa) {
				return &VerificationError{idx, "merkle root ML-DSA signature invalid"}
			}
		}

		*hashBuffer = nil // Reset for next block
	}

	return nil
}

func (t *AuditLogTool) PrintStats(inputFormat string, out io.Writer) error {
	stats, err := t.Stats(inputFormat)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "Audit Log Statistics:\n")
	fmt.Fprintf(out, "  Total Entries:     %d\n", stats.TotalEntries)
	fmt.Fprintf(out, "  Genesis Entries:   %d\n", stats.GenesisEntries)
	fmt.Fprintf(out, "  Log Entries:       %d\n", stats.LogEntries)
	fmt.Fprintf(out, "  Grounding Entries: %d\n", stats.GroundingEntries)
	fmt.Fprintf(out, "  Start Time:        %s\n", stats.StartTime.Format(time.RFC3339))
	fmt.Fprintf(out, "  End Time:          %s\n", stats.EndTime.Format(time.RFC3339))
	fmt.Fprintf(out, "  Duration:          %s\n", stats.EndTime.Sub(stats.StartTime))
	fmt.Fprintf(out, "  Errors:            %d\n", stats.Errors)
	fmt.Fprintf(out, "\nOperations:\n")
	for op, count := range stats.Operations {
		fmt.Fprintf(out, "  %-25s: %d\n", op, count)
	}
	fmt.Fprintf(out, "\nActors:\n")
	for actor, count := range stats.Actors {
		fmt.Fprintf(out, "  %-25s: %d\n", actor, count)
	}

	return nil
}
