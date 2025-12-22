package serialization

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type JsonSerializer struct {
	Indent bool
}

type jsonEntry struct {
	Version      uint16 `json:"version"`
	Timestamp    string `json:"timestamp"`
	Operation    string `json:"operation"`
	Phase        string `json:"phase,omitempty"`
	Bucket       string `json:"bucket"`
	Key          string `json:"key,omitempty"`
	UploadID     string `json:"upload_id,omitempty"`
	PartNumber   int32  `json:"part_number,omitempty"`
	Actor        string `json:"actor,omitempty"`
	Error        string `json:"error,omitempty"`
	PreviousHash string `json:"previous_hash"`
	Hash         string `json:"hash"`
	Signature    string `json:"signature"`
}

func (s *JsonSerializer) Encode(w io.Writer, e *auditlog.Entry) error {
	output := jsonEntry{
		Version:      e.Version,
		Timestamp:    e.Timestamp.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		Operation:    string(e.Operation),
		Phase:        string(e.Phase),
		Bucket:       e.Bucket,
		Key:          e.Key,
		UploadID:     e.UploadID,
		PartNumber:   e.PartNumber,
		Actor:        e.Actor,
		Error:        e.Error,
		PreviousHash: fmt.Sprintf("%x", e.PreviousHash),
		Hash:         fmt.Sprintf("%x", e.Hash),
		Signature:    fmt.Sprintf("%x", e.Signature),
	}

	enc := json.NewEncoder(w)
	if s.Indent {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(output)
}

func (s *JsonSerializer) NewDecoder(r io.Reader) Decoder {
	return &JsonDecoder{dec: json.NewDecoder(r)}
}

type JsonDecoder struct {
	dec *json.Decoder
}

func (d *JsonDecoder) Decode() (*auditlog.Entry, error) {
	var je jsonEntry
	if err := d.dec.Decode(&je); err != nil {
		return nil, err
	}

	ts, err := time.Parse(time.RFC3339Nano, je.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	prevHash, err := hex.DecodeString(je.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode previous_hash: %w", err)
	}

	hash, err := hex.DecodeString(je.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hash: %w", err)
	}

	sig, err := hex.DecodeString(je.Signature)
	if err != nil {
		return nil, fmt.Errorf("failed to decode signature: %w", err)
	}

	return &auditlog.Entry{
		Version:      je.Version,
		Timestamp:    ts,
		Operation:    auditlog.Operation(je.Operation),
		Phase:        auditlog.Phase(je.Phase),
		Bucket:       je.Bucket,
		Key:          je.Key,
		UploadID:     je.UploadID,
		PartNumber:   je.PartNumber,
		Actor:        je.Actor,
		Error:        je.Error,
		PreviousHash: prevHash,
		Hash:         hash,
		Signature:    sig,
	}, nil
}
