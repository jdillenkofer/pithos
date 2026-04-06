package auditlog

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/binary"
	"io"
	"time"

	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
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
	OpAppendObject            Operation = "AppendObject"
	OpDeleteObject            Operation = "DeleteObject"
	OpDeleteObjects           Operation = "DeleteObjects"
	OpCreateMultipartUpload   Operation = "CreateMultipartUpload"
	OpUploadPart              Operation = "UploadPart"
	OpCompleteMultipartUpload Operation = "CompleteMultipartUpload"
	OpAbortMultipartUpload    Operation = "AbortMultipartUpload"
	OpListMultipartUploads    Operation = "ListMultipartUploads"
	OpListParts               Operation = "ListParts"
	OpGetBucketCORS           Operation = "GetBucketCORS"
	OpPutBucketCORS           Operation = "PutBucketCORS"
	OpDeleteBucketCORS        Operation = "DeleteBucketCORS"
	OpGetBucketWebsite        Operation = "GetBucketWebsite"
	OpPutBucketWebsite        Operation = "PutBucketWebsite"
	OpDeleteBucketWebsite     Operation = "DeleteBucketWebsite"
)

type Phase string

const (
	PhaseStart    Phase = "START"
	PhaseComplete Phase = "COMPLETE"
)

const CurrentVersion uint16 = 2

type EntryType string

const (
	EntryTypeGenesis   EntryType = "GENESIS"
	EntryTypeLog       EntryType = "LOG"
	EntryTypeGrounding EntryType = "GROUNDING"
)

const GroundingBlockSize = 1000

type GenesisDetails struct{}

type AuthType string

const (
	AuthTypeAnonymous    AuthType = "anonymous"
	AuthTypeSigV4Header  AuthType = "sigv4-header"
	AuthTypeSigV4Presign AuthType = "sigv4-presign"
)

type OutcomeType string

const (
	OutcomePending OutcomeType = "pending"
	OutcomeSuccess OutcomeType = "success"
	OutcomeError   OutcomeType = "error"
	OutcomeDenied  OutcomeType = "denied"
)

type ResourceDetails struct {
	Bucket     string
	Key        string
	UploadID   string
	PartNumber int32
}

type ActorDetails struct {
	CredentialID string
	AuthType     AuthType
}

type RequestDetails struct {
	RequestID string
	TraceID   string
	ClientIP  string
}

type OutcomeDetails struct {
	StatusCode int32
	Outcome    OutcomeType
	ErrorCode  string
	Error      string
	DurationMs int64
}

type LogDetails struct {
	Operation Operation
	Phase     Phase
	Resource  ResourceDetails
	Actor     ActorDetails
	Request   RequestDetails
	Outcome   OutcomeDetails
}

type GroundingDetails struct {
	MerkleRootHash   []byte
	SignatureEd25519 []byte
	SignatureMlDsa87 []byte
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
		writeString(buf, d.Resource.Bucket)
		writeString(buf, d.Resource.Key)
		writeString(buf, d.Resource.UploadID)
		binary.Write(buf, binary.BigEndian, d.Resource.PartNumber)

		if e.Version <= 1 {
			writeString(buf, d.Actor.CredentialID)
			writeString(buf, d.Outcome.Error)
			break
		}

		writeString(buf, d.Actor.CredentialID)
		writeString(buf, string(d.Actor.AuthType))
		writeString(buf, d.Request.RequestID)
		writeString(buf, d.Request.TraceID)
		writeString(buf, d.Request.ClientIP)
		binary.Write(buf, binary.BigEndian, d.Outcome.StatusCode)
		writeString(buf, string(d.Outcome.Outcome))
		writeString(buf, d.Outcome.ErrorCode)
		writeString(buf, d.Outcome.Error)
		binary.Write(buf, binary.BigEndian, d.Outcome.DurationMs)
	case *GroundingDetails:
		writeBytes(buf, d.MerkleRootHash)
		if len(d.SignatureEd25519) != ed25519.SignatureSize {
			panic("invalid Ed25519 signature length")
		}
		buf.Write(d.SignatureEd25519)
		if len(d.SignatureMlDsa87) != mldsa87.SignatureSize {
			panic("invalid ML-DSA-87 signature length")
		}
		buf.Write(d.SignatureMlDsa87)
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
