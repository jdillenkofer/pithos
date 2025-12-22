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

func (s *TextSerializer) Encode(w io.Writer, e *auditlog.Entry) error {
	timestamp := e.Timestamp.UTC().Format("2006-01-02 15:04:05")
	
	base := fmt.Sprintf("V%d [%s] %-25s %-10s Bucket: %s", e.Version, timestamp, e.Operation, e.Phase, e.Bucket)
	
	if e.Key != "" {
		base += fmt.Sprintf(" | Key: %s", e.Key)
	}
	if e.UploadID != "" {
		base += fmt.Sprintf(" | UploadID: %s", e.UploadID)
	}
	if e.PartNumber != 0 {
		base += fmt.Sprintf(" | Part: %d", e.PartNumber)
	}
	if e.Actor != "" {
		base += fmt.Sprintf(" | Actor: %s", e.Actor)
	}
	if e.Error != "" {
		base += fmt.Sprintf(" | Error: %s", e.Error)
	}

	base += fmt.Sprintf(" | PrevHash: %x", e.PreviousHash)
	base += fmt.Sprintf(" | Hash: %x", e.Hash)
	base += fmt.Sprintf(" | Signature: %x", e.Signature)

	_, err := fmt.Fprintln(w, base)
	return err
}

var textLogRegex = regexp.MustCompile(`^V(\d+)\s+\[(.*?)\]\s+(.*?)\s+(.*?)\s+Bucket:\s+(.*?)(?:\s+\|.*)?$`)

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
	ts, err := time.Parse("2006-01-02 15:04:05", matches[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	entry := &auditlog.Entry{
		Version:   uint16(version),
		Timestamp: ts,
		Operation: auditlog.Operation(strings.TrimSpace(matches[3])),
		Phase:     auditlog.Phase(strings.TrimSpace(matches[4])),
		Bucket:    strings.TrimSpace(matches[5]),
	}

	// Parse optional fields separated by " | "
	parts := strings.Split(line, " | ")
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, ": ", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])

		switch key {
		case "Key":
			entry.Key = val
		case "UploadID":
			entry.UploadID = val
		case "Part":
			p, _ := strconv.Atoi(val)
			entry.PartNumber = int32(p)
		case "Actor":
			entry.Actor = val
		case "Error":
			entry.Error = val
		case "PrevHash":
			h, _ := hex.DecodeString(val)
			entry.PreviousHash = h
		case "Hash":
			h, _ := hex.DecodeString(val)
			entry.Hash = h
		case "Signature":
			h, _ := hex.DecodeString(val)
			entry.Signature = h
		}
	}

	return entry, nil
}