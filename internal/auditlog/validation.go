package auditlog

import (
	"bytes"
	"crypto/sha512"
	"fmt"

	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
)

type VerificationError struct {
	EntryIndex int
	Reason     string
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("verification failed at entry %d: %s", e.EntryIndex, e.Reason)
}

type Validator struct {
	Ed25519Verifier signing.Verifier
	MlDsa87Verifier signing.Verifier
	PrevHash        []byte
	HashBuffer      [][]byte
	Index           int
}

func NewValidator(ed25519Verifier, mlDsa87Verifier signing.Verifier) *Validator {
	return &Validator{
		Ed25519Verifier: ed25519Verifier,
		MlDsa87Verifier:  mlDsa87Verifier,
		HashBuffer:       make([][]byte, 0, GroundingBlockSize),
	}
}

func (v *Validator) ValidateEntry(entry *Entry) error {
	// 1. Validate internal hash (detect data corruption)
	if !bytes.Equal(entry.CalculateHash(), entry.Hash) {
		return &VerificationError{v.Index, "entry hash mismatch: data corruption detected"}
	}

	// 2. Verify Chain Integrity
	if v.Index == 0 {
		if entry.Type != EntryTypeGenesis {
			return &VerificationError{v.Index, "first entry is not GENESIS"}
		}
		expectedPrev := sha512.Sum512([]byte("pithos"))
		if !bytes.Equal(entry.PreviousHash, expectedPrev[:]) {
			return &VerificationError{v.Index, "genesis previous hash invalid"}
		}
	} else {
		if !bytes.Equal(entry.PreviousHash, v.PrevHash) {
			return &VerificationError{v.Index, fmt.Sprintf("chain break: expected prev hash %x, got %x", v.PrevHash, entry.PreviousHash)}
		}
	}

	// 3. Verify Entry Signature (Ed25519) if verifier is provided
	if v.Ed25519Verifier != nil {
		if !entry.Verify(v.Ed25519Verifier) {
			return &VerificationError{v.Index, "entry signature invalid"}
		}
	}

	// 4. Grounding Verification
	switch entry.Type {
	case EntryTypeLog:
		v.HashBuffer = append(v.HashBuffer, entry.Hash)
		if len(v.HashBuffer) > GroundingBlockSize {
			return &VerificationError{v.Index, fmt.Sprintf("too many log entries without grounding: expected grounding after %d entries", GroundingBlockSize)}
		}
	case EntryTypeGrounding:
		if len(v.HashBuffer) != GroundingBlockSize {
			return &VerificationError{v.Index, fmt.Sprintf("grounding entry appeared at wrong interval: expected %d entries, got %d", GroundingBlockSize, len(v.HashBuffer))}
		}

		details, ok := entry.Details.(*GroundingDetails)
		if !ok {
			return &VerificationError{v.Index, "invalid grounding details structure"}
		}

		// Verify Merkle Root
		calculatedRoot := CalculateMerkleRoot(v.HashBuffer)
		if !bytes.Equal(calculatedRoot, details.MerkleRootHash) {
			return &VerificationError{v.Index, fmt.Sprintf("merkle root mismatch: expected %x, got %x", details.MerkleRootHash, calculatedRoot)}
		}

		// Verify Ed25519 signature of Merkle Root if verifier is provided
		if v.Ed25519Verifier != nil {
			if !v.Ed25519Verifier.Verify(details.MerkleRootHash, details.SignatureEd25519) {
				return &VerificationError{v.Index, "merkle root Ed25519 signature invalid"}
			}
		}

		// Verify ML-DSA signature of Merkle Root if verifier is provided
		if v.MlDsa87Verifier != nil {
			if !v.MlDsa87Verifier.Verify(details.MerkleRootHash, details.SignatureMlDsa87) {
				return &VerificationError{v.Index, "merkle root ML-DSA-87 signature invalid"}
			}
		}

		v.HashBuffer = nil // Reset for next block
	}

	v.PrevHash = entry.Hash
	v.Index++
	return nil
}