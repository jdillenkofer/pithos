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

type Entry struct {
	Version      uint16
	Timestamp    time.Time
	Operation    Operation
	Phase        Phase
	Bucket       string
	Key          string
	UploadID     string
	PartNumber   int32
	Actor        string
	Error        string
	PreviousHash []byte // SHA512 - 64 bytes
	Hash         []byte // SHA512 - 64 bytes
	Signature    []byte // Ed25519 - 64 bytes
}

func (e *Entry) CalculateHash() []byte {
	buf := new(bytes.Buffer)

	// Write version
	binary.Write(buf, binary.BigEndian, e.Version)

	// Write content for hashing
	binary.Write(buf, binary.BigEndian, e.Timestamp.UnixNano())
	writeString(buf, string(e.Operation))
	writeString(buf, string(e.Phase))
	writeString(buf, e.Bucket)
	writeString(buf, e.Key)
	writeString(buf, e.UploadID)
	binary.Write(buf, binary.BigEndian, e.PartNumber)
	writeString(buf, e.Actor)
	writeString(buf, e.Error)

	// Write previous hash
	if len(e.PreviousHash) > 0 {
		buf.Write(e.PreviousHash)
	} else {
		// Genesis block or first entry might have a special empty/zero prev hash
		buf.Write(make([]byte, sha512.Size))
	}

	h := sha512.Sum512(buf.Bytes())
	return h[:]
}

func (e *Entry) Sign(signer signing.Signer) error {
	e.Hash = e.CalculateHash()
	sig, err := signer.Sign(e.Hash)
	if err != nil {
		return err
	}
	e.Signature = sig
	return nil
}

func (e *Entry) Verify(verifier signing.Verifier) bool {
	calculatedHash := e.CalculateHash()
	if !bytes.Equal(calculatedHash, e.Hash) {
		return false
	}
	return verifier.Verify(e.Hash, e.Signature)
}

func writeString(w io.Writer, s string) error {
	l := uint32(len(s))
	if err := binary.Write(w, binary.BigEndian, l); err != nil {
		return err
	}
	if l > 0 {
		_, err := w.Write([]byte(s))
		return err
	}
	return nil
}
