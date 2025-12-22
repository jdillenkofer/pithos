package serialization

import (
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"io"
	"time"

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
	if err := writeString(w, string(e.Operation)); err != nil {
		return err
	}
	if err := writeString(w, string(e.Phase)); err != nil {
		return err
	}
	if err := writeString(w, e.Bucket); err != nil {
		return err
	}
	if err := writeString(w, e.Key); err != nil {
		return err
	}
	if err := writeString(w, e.UploadID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, e.PartNumber); err != nil {
		return err
	}
	if err := writeString(w, e.Actor); err != nil {
		return err
	}
	if err := writeString(w, e.Error); err != nil {
		return err
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
	
	if err := writeBytes(w, e.Signature); err != nil {
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
	
	op, err := readString(d.r)
	if err != nil {
		return nil, err
	}
	e.Operation = auditlog.Operation(op)
	
	phaseStr, err := readString(d.r)
	if err != nil {
		return nil, err
	}
	e.Phase = auditlog.Phase(phaseStr)
	
	if e.Bucket, err = readString(d.r); err != nil {
		return nil, err
	}
	if e.Key, err = readString(d.r); err != nil {
		return nil, err
	}
	if e.UploadID, err = readString(d.r); err != nil {
		return nil, err
	}
	if err := binary.Read(d.r, binary.BigEndian, &e.PartNumber); err != nil {
		return nil, err
	}
	if e.Actor, err = readString(d.r); err != nil {
		return nil, err
	}
	if e.Error, err = readString(d.r); err != nil {
		return nil, err
	}
	
	e.PreviousHash = make([]byte, sha512.Size)
	if _, err := io.ReadFull(d.r, e.PreviousHash); err != nil {
		return nil, err
	}
	
	e.Hash = make([]byte, sha512.Size)
	if _, err := io.ReadFull(d.r, e.Hash); err != nil {
		return nil, err
	}
	
	e.Signature, err = readBytes(d.r)
	if err != nil {
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
