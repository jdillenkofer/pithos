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
		base += fmt.Sprintf(" | %s %s Bucket: %s", escape(string(d.Operation)), escape(string(d.Phase)), escape(d.Bucket))
		if d.Key != "" {
			base += fmt.Sprintf(" | Key: %s", escape(d.Key))
		}
		if d.UploadID != "" {
			base += fmt.Sprintf(" | UploadID: %s", escape(d.UploadID))
		}
		if d.PartNumber != 0 {
			base += fmt.Sprintf(" | Part: %d", d.PartNumber)
		}
		if d.Actor != "" {
			base += fmt.Sprintf(" | Actor: %s", escape(d.Actor))
		}
		if d.Error != "" {
			base += fmt.Sprintf(" | Error: %s", escape(d.Error))
		}
	case *auditlog.GroundingDetails:
		base += fmt.Sprintf(" | MerkleRoot: %x | SigEd: %x | SigMl87: %x", d.MerkleRootHash, d.SignatureEd25519, d.SignatureMlDsa87)
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
		if len(parts) > 1 {
			opPhase := strings.Fields(parts[1])
			if len(opPhase) >= 2 {
				dls.Operation = auditlog.Operation(unescape(opPhase[0]))
				dls.Phase = auditlog.Phase(unescape(opPhase[1]))
			}
			if idx := strings.Index(parts[1], "Bucket: "); idx != -1 {
				dls.Bucket = unescape(strings.TrimSpace(parts[1][idx+8:]))
			}
		}
		
		for _, part := range parts[2:] {
			kv := strings.SplitN(part, ": ", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])

			switch key {
			case "Key":
				dls.Key = unescape(val)
			case "UploadID":
				dls.UploadID = unescape(val)
			case "Part":
				p, _ := strconv.Atoi(val)
				dls.PartNumber = int32(p)
			case "Actor":
				dls.Actor = unescape(val)
			case "Error":
				dls.Error = unescape(val)
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
			case "SigEd":
				h, _ := hex.DecodeString(val)
				dls.SignatureEd25519 = h
			case "SigMl87":
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
