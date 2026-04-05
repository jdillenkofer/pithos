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
	Version          uint16          `json:"version"`
	Timestamp        string          `json:"timestamp"`
	Type             string          `json:"type"`
	Details          json.RawMessage `json:"details"`
	PreviousHash     string          `json:"previous_hash"`
	Hash             string          `json:"hash"`
	SignatureEd25519 string          `json:"signature_ed25519"`
}

type jsonLogDetails struct {
	Operation string `json:"operation"`
	Phase     string `json:"phase"`
	Resource  struct {
		Bucket     string `json:"bucket"`
		Key        string `json:"key,omitempty"`
		UploadID   string `json:"upload_id,omitempty"`
		PartNumber int32  `json:"part_number,omitempty"`
	} `json:"resource"`
	Actor struct {
		CredentialID string `json:"credential_id,omitempty"`
		AuthType     string `json:"auth_type,omitempty"`
	} `json:"actor"`
	Request struct {
		RequestID string `json:"request_id,omitempty"`
		TraceID   string `json:"trace_id,omitempty"`
		ClientIP  string `json:"client_ip,omitempty"`
	} `json:"request"`
	Outcome struct {
		StatusCode int32  `json:"status_code"`
		Outcome    string `json:"outcome,omitempty"`
		ErrorCode  string `json:"error_code,omitempty"`
		Error      string `json:"error,omitempty"`
		DurationMs int64  `json:"duration_ms"`
	} `json:"outcome"`
}

type jsonLogDetailsV1 struct {
	Operation  string `json:"operation"`
	Phase      string `json:"phase"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key,omitempty"`
	UploadID   string `json:"upload_id,omitempty"`
	PartNumber int32  `json:"part_number,omitempty"`
	Actor      string `json:"actor"`
	Error      string `json:"error,omitempty"`
}

type jsonGroundingDetails struct {
	MerkleRootHash   string `json:"merkle_root_hash"`
	SignatureEd25519 string `json:"signature_ed25519"`
	SignatureMlDsa87 string `json:"signature_ml_dsa_87"`
}

func (s *JsonSerializer) Encode(w io.Writer, e *auditlog.Entry) error {
	var details json.RawMessage
	var err error

	switch d := e.Details.(type) {
	case *auditlog.GenesisDetails:
		details, _ = json.Marshal(struct{}{})
	case *auditlog.LogDetails:
		if e.Version <= 1 {
			details, err = json.Marshal(jsonLogDetailsV1{
				Operation:  string(d.Operation),
				Phase:      string(d.Phase),
				Bucket:     d.Resource.Bucket,
				Key:        d.Resource.Key,
				UploadID:   d.Resource.UploadID,
				PartNumber: d.Resource.PartNumber,
				Actor:      d.Actor.CredentialID,
				Error:      d.Outcome.Error,
			})
		} else {
			payload := jsonLogDetails{
				Operation: string(d.Operation),
				Phase:     string(d.Phase),
			}
			payload.Resource.Bucket = d.Resource.Bucket
			payload.Resource.Key = d.Resource.Key
			payload.Resource.UploadID = d.Resource.UploadID
			payload.Resource.PartNumber = d.Resource.PartNumber
			payload.Actor.CredentialID = d.Actor.CredentialID
			payload.Actor.AuthType = string(d.Actor.AuthType)
			payload.Request.RequestID = d.Request.RequestID
			payload.Request.TraceID = d.Request.TraceID
			payload.Request.ClientIP = d.Request.ClientIP
			payload.Outcome.StatusCode = d.Outcome.StatusCode
			payload.Outcome.Outcome = string(d.Outcome.Outcome)
			payload.Outcome.ErrorCode = d.Outcome.ErrorCode
			payload.Outcome.Error = d.Outcome.Error
			payload.Outcome.DurationMs = d.Outcome.DurationMs
			details, err = json.Marshal(payload)
		}
	case *auditlog.GroundingDetails:
		details, err = json.Marshal(jsonGroundingDetails{
			MerkleRootHash:   hex.EncodeToString(d.MerkleRootHash),
			SignatureEd25519: hex.EncodeToString(d.SignatureEd25519),
			SignatureMlDsa87: hex.EncodeToString(d.SignatureMlDsa87),
		})
	}

	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	output := jsonEntry{
		Version:          e.Version,
		Timestamp:        e.Timestamp.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		Type:             string(e.Type),
		Details:          details,
		PreviousHash:     hex.EncodeToString(e.PreviousHash),
		Hash:             hex.EncodeToString(e.Hash),
		SignatureEd25519: hex.EncodeToString(e.SignatureEd25519),
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

	sig, err := hex.DecodeString(je.SignatureEd25519)
	if err != nil {
		return nil, fmt.Errorf("failed to decode signature: %w", err)
	}

	e := &auditlog.Entry{
		Version:          je.Version,
		Timestamp:        ts,
		Type:             auditlog.EntryType(je.Type),
		PreviousHash:     prevHash,
		Hash:             hash,
		SignatureEd25519: sig,
	}

	switch e.Type {
	case auditlog.EntryTypeGenesis:
		e.Details = &auditlog.GenesisDetails{}
	case auditlog.EntryTypeLog:
		if e.Version <= 1 {
			var jd jsonLogDetailsV1
			if err := json.Unmarshal(je.Details, &jd); err != nil {
				return nil, fmt.Errorf("failed to unmarshal legacy log details: %w", err)
			}
			outcomeType := auditlog.OutcomeSuccess
			statusCode := int32(200)
			if jd.Error != "" {
				outcomeType = auditlog.OutcomeError
				statusCode = 500
			}
			e.Details = &auditlog.LogDetails{
				Operation: auditlog.Operation(jd.Operation),
				Phase:     auditlog.Phase(jd.Phase),
				Resource: auditlog.ResourceDetails{
					Bucket:     jd.Bucket,
					Key:        jd.Key,
					UploadID:   jd.UploadID,
					PartNumber: jd.PartNumber,
				},
				Actor: auditlog.ActorDetails{
					CredentialID: jd.Actor,
					AuthType:     auditlog.AuthTypeAnonymous,
				},
				Outcome: auditlog.OutcomeDetails{
					StatusCode: statusCode,
					Outcome:    outcomeType,
					Error:      jd.Error,
				},
			}
			break
		}

		var jd jsonLogDetails
		if err := json.Unmarshal(je.Details, &jd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal log details: %w", err)
		}
		e.Details = &auditlog.LogDetails{
			Operation: auditlog.Operation(jd.Operation),
			Phase:     auditlog.Phase(jd.Phase),
			Resource: auditlog.ResourceDetails{
				Bucket:     jd.Resource.Bucket,
				Key:        jd.Resource.Key,
				UploadID:   jd.Resource.UploadID,
				PartNumber: jd.Resource.PartNumber,
			},
			Actor: auditlog.ActorDetails{
				CredentialID: jd.Actor.CredentialID,
				AuthType:     auditlog.AuthType(jd.Actor.AuthType),
			},
			Request: auditlog.RequestDetails{
				RequestID: jd.Request.RequestID,
				TraceID:   jd.Request.TraceID,
				ClientIP:  jd.Request.ClientIP,
			},
			Outcome: auditlog.OutcomeDetails{
				StatusCode: jd.Outcome.StatusCode,
				Outcome:    auditlog.OutcomeType(jd.Outcome.Outcome),
				ErrorCode:  jd.Outcome.ErrorCode,
				Error:      jd.Outcome.Error,
				DurationMs: jd.Outcome.DurationMs,
			},
		}
	case auditlog.EntryTypeGrounding:
		var jd jsonGroundingDetails
		if err := json.Unmarshal(je.Details, &jd); err != nil {
			return nil, fmt.Errorf("failed to unmarshal grounding details: %w", err)
		}
		root, _ := hex.DecodeString(jd.MerkleRootHash)
		sigEd, _ := hex.DecodeString(jd.SignatureEd25519)
		sigMl, _ := hex.DecodeString(jd.SignatureMlDsa87)
		e.Details = &auditlog.GroundingDetails{
			MerkleRootHash:   root,
			SignatureEd25519: sigEd,
			SignatureMlDsa87: sigMl,
		}
	}

	return e, nil
}
