package serialization

import (
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type BinarySerializer struct{}

func (s *BinarySerializer) Encode(w io.Writer, e *auditlog.Entry) error {
	if err := binary.Write(w, binary.BigEndian, e.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, e.Timestamp.UnixNano()); err != nil {
		return err
	}
	if err := writeString(w, string(e.Type)); err != nil {
		return err
	}

	switch d := e.Details.(type) {
	case *auditlog.GenesisDetails:
		// No fields
	case *auditlog.LogDetails:
		if err := writeString(w, string(d.Operation)); err != nil {
			return err
		}
		if err := writeString(w, string(d.Phase)); err != nil {
			return err
		}
		if err := writeString(w, d.Bucket); err != nil {
			return err
		}
		if err := writeString(w, d.Key); err != nil {
			return err
		}
		if err := writeString(w, d.UploadID); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, d.PartNumber); err != nil {
			return err
		}
		if err := writeString(w, d.Actor); err != nil {
			return err
		}
		if err := writeString(w, d.Error); err != nil {
			return err
		}
	case *auditlog.GroundingDetails:
		if err := writeBytes(w, d.MerkleRootHash); err != nil {
			return err
		}
		if len(d.SignatureEd25519) != ed25519.SignatureSize {
			return errors.New("invalid Ed25519 signature length in grounding details")
		}
		if _, err := w.Write(d.SignatureEd25519); err != nil {
			return err
		}
		if len(d.SignatureMlDsa) != mldsa65.SignatureSize {
			return errors.New("invalid ML-DSA signature length in grounding details")
		}
		if _, err := w.Write(d.SignatureMlDsa); err != nil {
			return err
		}
	}
	
	// Hashes and signatures are fixed length
	if len(e.PreviousHash) != sha512.Size {
		return errors.New("invalid previous hash length")
	}
	if _, err := w.Write(e.PreviousHash); err != nil {
		return err
	}
	
	if len(e.Hash) != sha512.Size {
		return errors.New("invalid hash length")
	}
	if _, err := w.Write(e.Hash); err != nil {
		return err
	}
	
	if len(e.SignatureEd25519) != ed25519.SignatureSize {
		return errors.New("invalid entry signature length")
	}
	if _, err := w.Write(e.SignatureEd25519); err != nil {
		return err
	}
	
	return nil
}

func (s *BinarySerializer) NewDecoder(r io.Reader) Decoder {
	return &BinaryDecoder{r: r}
}

type BinaryDecoder struct {
	r io.Reader
}

func (d *BinaryDecoder) Decode() (*auditlog.Entry, error) {
	e := &auditlog.Entry{}
	
	if err := binary.Read(d.r, binary.BigEndian, &e.Version); err != nil {
		return nil, err
	}

	var ts int64
	if err := binary.Read(d.r, binary.BigEndian, &ts); err != nil {
		return nil, err
	}
	e.Timestamp = time.Unix(0, ts)
	
	typeStr, err := readString(d.r)
	if err != nil {
		return nil, err
	}
	e.Type = auditlog.EntryType(typeStr)

	switch e.Type {
	case auditlog.EntryTypeGenesis:
		e.Details = &auditlog.GenesisDetails{}
	case auditlog.EntryTypeLog:
		dls := &auditlog.LogDetails{}
		op, err := readString(d.r)
		if err != nil {
			return nil, err
		}
		dls.Operation = auditlog.Operation(op)
		
		phaseStr, err := readString(d.r)
		if err != nil {
			return nil, err
		}
		dls.Phase = auditlog.Phase(phaseStr)
		
		if dls.Bucket, err = readString(d.r); err != nil {
			return nil, err
		}
		if dls.Key, err = readString(d.r); err != nil {
			return nil, err
		}
		if dls.UploadID, err = readString(d.r); err != nil {
			return nil, err
		}
		if err := binary.Read(d.r, binary.BigEndian, &dls.PartNumber); err != nil {
			return nil, err
		}
		if dls.Actor, err = readString(d.r); err != nil {
			return nil, err
		}
		if dls.Error, err = readString(d.r); err != nil {
			return nil, err
		}
		e.Details = dls
	case auditlog.EntryTypeGrounding:
		dls := &auditlog.GroundingDetails{}
		if dls.MerkleRootHash, err = readBytes(d.r); err != nil {
			return nil, err
		}
		dls.SignatureEd25519 = make([]byte, ed25519.SignatureSize)
		if _, err := io.ReadFull(d.r, dls.SignatureEd25519); err != nil {
			return nil, err
		}
		dls.SignatureMlDsa = make([]byte, mldsa65.SignatureSize)
		if _, err := io.ReadFull(d.r, dls.SignatureMlDsa); err != nil {
			return nil, err
		}
		e.Details = dls
	}
	
	e.PreviousHash = make([]byte, sha512.Size)
	if _, err := io.ReadFull(d.r, e.PreviousHash); err != nil {
		return nil, err
	}
	
	e.Hash = make([]byte, sha512.Size)
	if _, err := io.ReadFull(d.r, e.Hash); err != nil {
		return nil, err
	}
	
	e.SignatureEd25519 = make([]byte, ed25519.SignatureSize)
	if _, err := io.ReadFull(d.r, e.SignatureEd25519); err != nil {
		return nil, err
	}
	
	return e, nil
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

func readBytes(r io.Reader) ([]byte, error) {
	var l uint32
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return nil, err
	}
	if l == 0 {
		return nil, nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeString(w io.Writer, s string) error {
	return writeBytes(w, []byte(s))
}

func readString(r io.Reader) (string, error) {
	b, err := readBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}