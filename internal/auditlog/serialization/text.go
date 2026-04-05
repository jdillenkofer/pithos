package serialization

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type TextSerializer struct{}

const textTimestampFormat = "2006-01-02 15:04:05.000000000"

func (s *TextSerializer) Encode(w io.Writer, e *auditlog.Entry) error {
	timestamp := e.Timestamp.UTC().Format(textTimestampFormat)
	base := fmt.Sprintf("V%d [%s] TYPE: %s", e.Version, timestamp, e.Type)

	switch d := e.Details.(type) {
	case *auditlog.GenesisDetails:
		base += " | GENESIS"
	case *auditlog.LogDetails:
		base += fmt.Sprintf(" | Op: %s", escape(string(d.Operation)))
		base += fmt.Sprintf(" | Phase: %s", escape(string(d.Phase)))
		base += fmt.Sprintf(" | Bucket: %s", escape(d.Resource.Bucket))
		if d.Resource.Key != "" {
			base += fmt.Sprintf(" | Key: %s", escape(d.Resource.Key))
		}
		if d.Resource.UploadID != "" {
			base += fmt.Sprintf(" | UploadID: %s", escape(d.Resource.UploadID))
		}
		if d.Resource.PartNumber != 0 {
			base += fmt.Sprintf(" | Part: %d", d.Resource.PartNumber)
		}
		if d.Actor.CredentialID != "" {
			base += fmt.Sprintf(" | CredentialID: %s", escape(d.Actor.CredentialID))
		}
		if d.Actor.AuthType != "" {
			base += fmt.Sprintf(" | AuthType: %s", escape(string(d.Actor.AuthType)))
		}
		if d.Request.RequestID != "" {
			base += fmt.Sprintf(" | RequestID: %s", escape(d.Request.RequestID))
		}
		if d.Request.TraceID != "" {
			base += fmt.Sprintf(" | TraceID: %s", escape(d.Request.TraceID))
		}
		if d.Request.ClientIP != "" {
			base += fmt.Sprintf(" | ClientIP: %s", escape(d.Request.ClientIP))
		}
		if d.Outcome.StatusCode != 0 {
			base += fmt.Sprintf(" | StatusCode: %d", d.Outcome.StatusCode)
		}
		if d.Outcome.Outcome != "" {
			base += fmt.Sprintf(" | Outcome: %s", escape(string(d.Outcome.Outcome)))
		}
		if d.Outcome.ErrorCode != "" {
			base += fmt.Sprintf(" | ErrorCode: %s", escape(d.Outcome.ErrorCode))
		}
		if d.Outcome.Error != "" {
			base += fmt.Sprintf(" | Error: %s", escape(d.Outcome.Error))
		}
		if d.Outcome.DurationMs != 0 {
			base += fmt.Sprintf(" | DurationMs: %d", d.Outcome.DurationMs)
		}
	case *auditlog.GroundingDetails:
		base += fmt.Sprintf(" | MerkleRoot: %x | Ed25519: %x | ML-DSA-87: %x", d.MerkleRootHash, d.SignatureEd25519, d.SignatureMlDsa87)
	}

	base += fmt.Sprintf(" | PrevHash: %x", e.PreviousHash)
	base += fmt.Sprintf(" | Hash: %x", e.Hash)
	base += fmt.Sprintf(" | SigEd25519: %x", e.SignatureEd25519)

	_, err := fmt.Fprintln(w, base)
	return err
}

var textLogRegex = regexp.MustCompile(`^V(\d+)\s+\[(.*?)\]\s+TYPE:\s+(.*?)(?:\s+\|.*)?$`)

func (s *TextSerializer) NewDecoder(r io.Reader) Decoder {
	return &TextDecoder{scanner: bufio.NewScanner(r)}
}

type TextDecoder struct {
	scanner *bufio.Scanner
}

func (d *TextDecoder) Decode() (*auditlog.Entry, error) {
	if !d.scanner.Scan() {
		if err := d.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	line := d.scanner.Text()

	matches := textLogRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("failed to parse log line: %s", line)
	}

	version, _ := strconv.Atoi(matches[1])
	ts, err := time.ParseInLocation(textTimestampFormat, matches[2], time.UTC)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	entry := &auditlog.Entry{
		Version:   uint16(version),
		Timestamp: ts,
		Type:      auditlog.EntryType(strings.TrimSpace(matches[3])),
	}

	parts := strings.Split(line, " | ")

	switch entry.Type {
	case auditlog.EntryTypeGenesis:
		entry.Details = &auditlog.GenesisDetails{}
	case auditlog.EntryTypeLog:
		dls := &auditlog.LogDetails{}
		if len(parts) > 1 && !strings.Contains(parts[1], ": ") {
			opPhase := strings.Fields(parts[1])
			if len(opPhase) >= 2 {
				dls.Operation = auditlog.Operation(unescape(opPhase[0]))
				dls.Phase = auditlog.Phase(unescape(opPhase[1]))
			}
			if idx := strings.Index(parts[1], "Bucket: "); idx != -1 {
				dls.Resource.Bucket = unescape(strings.TrimSpace(parts[1][idx+8:]))
			}
		}

		for _, part := range parts[1:] {
			kv := strings.SplitN(part, ": ", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])

			switch key {
			case "Op":
				dls.Operation = auditlog.Operation(unescape(val))
			case "Phase":
				dls.Phase = auditlog.Phase(unescape(val))
			case "Bucket":
				dls.Resource.Bucket = unescape(val)
			case "Key":
				dls.Resource.Key = unescape(val)
			case "UploadID":
				dls.Resource.UploadID = unescape(val)
			case "Part":
				p, _ := strconv.Atoi(val)
				dls.Resource.PartNumber = int32(p)
			case "Actor", "CredentialID":
				dls.Actor.CredentialID = unescape(val)
			case "AuthType":
				dls.Actor.AuthType = auditlog.AuthType(unescape(val))
			case "RequestID":
				dls.Request.RequestID = unescape(val)
			case "TraceID":
				dls.Request.TraceID = unescape(val)
			case "ClientIP":
				dls.Request.ClientIP = unescape(val)
			case "StatusCode":
				s, _ := strconv.Atoi(val)
				dls.Outcome.StatusCode = int32(s)
			case "Outcome":
				dls.Outcome.Outcome = auditlog.OutcomeType(unescape(val))
			case "ErrorCode":
				dls.Outcome.ErrorCode = unescape(val)
			case "Error":
				dls.Outcome.Error = unescape(val)
			case "DurationMs":
				duration, _ := strconv.ParseInt(val, 10, 64)
				dls.Outcome.DurationMs = duration
			}
		}
		if version <= 1 {
			dls.Actor.AuthType = auditlog.AuthTypeAnonymous
			if dls.Outcome.Outcome == "" {
				dls.Outcome.Outcome = auditlog.OutcomeSuccess
				if dls.Outcome.Error != "" {
					dls.Outcome.Outcome = auditlog.OutcomeError
					dls.Outcome.StatusCode = 500
				} else {
					dls.Outcome.StatusCode = 200
				}
			}
		}
		entry.Details = dls
	case auditlog.EntryTypeGrounding:
		dls := &auditlog.GroundingDetails{}
		for _, part := range parts[1:] {
			kv := strings.SplitN(part, ": ", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			switch key {
			case "MerkleRoot":
				h, _ := hex.DecodeString(val)
				dls.MerkleRootHash = h
			case "Ed25519":
				h, _ := hex.DecodeString(val)
				dls.SignatureEd25519 = h
			case "ML-DSA-87":
				h, _ := hex.DecodeString(val)
				dls.SignatureMlDsa87 = h
			}
		}
		entry.Details = dls
	}

	// Parse common envelope fields
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, ": ", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])

		switch key {
		case "PrevHash":
			h, _ := hex.DecodeString(val)
			entry.PreviousHash = h
		case "Hash":
			h, _ := hex.DecodeString(val)
			entry.Hash = h
		case "SigEd25519":
			h, _ := hex.DecodeString(val)
			entry.SignatureEd25519 = h
		}
	}

	return entry, nil
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "|", `\|`)
	return s
}

func unescape(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				buf.WriteByte('\n')
			case 'r':
				buf.WriteByte('\r')
			case '|':
				buf.WriteByte('|')
			case '\\':
				buf.WriteByte('\\')
			default:
				buf.WriteByte('\\')
				buf.WriteByte(s[i+1])
			}
			i++
		} else {
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}
