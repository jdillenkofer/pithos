package auditlog

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
)

type Operation string

const (
	OpCreateBucket            Operation = "CreateBucket"
	OpDeleteBucket            Operation = "DeleteBucket"
	OpListBuckets             Operation = "ListBuckets"
	OpHeadBucket              Operation = "HeadBucket"
	OpListObjects             Operation = "ListObjects"
	OpHeadObject              Operation = "HeadObject"
	OpGetObject               Operation = "GetObject"
	OpPutObject               Operation = "PutObject"
	OpDeleteObject            Operation = "DeleteObject"
	OpCreateMultipartUpload   Operation = "CreateMultipartUpload"
	OpUploadPart              Operation = "UploadPart"
	OpCompleteMultipartUpload Operation = "CompleteMultipartUpload"
	OpAbortMultipartUpload    Operation = "AbortMultipartUpload"
	OpListMultipartUploads    Operation = "ListMultipartUploads"
	OpListParts               Operation = "ListParts"
)

type Phase string

const (
	PhaseStart    Phase = "START"
	PhaseComplete Phase = "COMPLETE"
)

const CurrentVersion uint16 = 1

type EntryType string

const (
	EntryTypeGenesis   EntryType = "GENESIS"
	EntryTypeLog       EntryType = "LOG"
	EntryTypeGrounding EntryType = "GROUNDING"
)

const GroundingBlockSize = 1000

type GenesisDetails struct{}

type LogDetails struct {
	Operation  Operation
	Phase      Phase
	Bucket     string
	Key        string
	UploadID   string
	PartNumber int32
	Actor      string
	Error      string
}

type GroundingDetails struct {
	MerkleRootHash   []byte
	SignatureEd25519 []byte
	SignatureMlDsa   []byte
}

type Entry struct {
	Version          uint16
	Timestamp        time.Time
	Type             EntryType
	Details          interface{}
	PreviousHash     []byte // SHA512 - 64 bytes
	Hash             []byte // SHA512 - 64 bytes
	SignatureEd25519 []byte // Ed25519 - 64 bytes
}

func (e *Entry) CalculateHash() []byte {
	buf := new(bytes.Buffer)

	// Write version
	binary.Write(buf, binary.BigEndian, e.Version)

	// Write common fields
	binary.Write(buf, binary.BigEndian, e.Timestamp.UnixNano())
	writeString(buf, string(e.Type))

	// Write details
	switch d := e.Details.(type) {
	case *GenesisDetails:
		// No fields to write
	case *LogDetails:
		writeString(buf, string(d.Operation))
		writeString(buf, string(d.Phase))
		writeString(buf, d.Bucket)
		writeString(buf, d.Key)
		writeString(buf, d.UploadID)
		binary.Write(buf, binary.BigEndian, d.PartNumber)
		writeString(buf, d.Actor)
		writeString(buf, d.Error)
	case *GroundingDetails:
		writeBytes(buf, d.MerkleRootHash)
		writeBytes(buf, d.SignatureEd25519)
		writeBytes(buf, d.SignatureMlDsa)
	}

	// Write previous hash
	buf.Write(e.PreviousHash)

	h := sha512.Sum512(buf.Bytes())
	return h[:]
}

func (e *Entry) Sign(signer signing.Signer) error {
	e.Hash = e.CalculateHash()
	sig, err := signer.Sign(e.Hash)
	if err != nil {
		return err
	}
	e.SignatureEd25519 = sig
	return nil
}

func (e *Entry) Verify(verifier signing.Verifier) bool {
	calculatedHash := e.CalculateHash()
	if !bytes.Equal(calculatedHash, e.Hash) {
		return false
	}
	return verifier.Verify(e.Hash, e.SignatureEd25519)
}

func writeString(w io.Writer, s string) error {
	return writeBytes(w, []byte(s))
}

func writeBytes(w io.Writer, b []byte) error {
	l := uint32(len(b))
	if err := binary.Write(w, binary.BigEndian, l); err != nil {
		return err
	}
	if l > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}
