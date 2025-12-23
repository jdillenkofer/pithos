package serialization

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	_ "github.com/jdillenkofer/pithos/internal/testing"
)

func TestSerializers(t *testing.T) {
	entries := []*auditlog.Entry{
		{
			Version:          auditlog.CurrentVersion,
			Timestamp:        time.Date(2025, 12, 22, 10, 0, 0, 0, time.UTC),
			Type:             auditlog.EntryTypeGenesis,
			Details:          &auditlog.GenesisDetails{},
			PreviousHash:     make([]byte, sha512.Size),
			Hash:             make([]byte, sha512.Size),
			SignatureEd25519: make([]byte, 64),
		},
		{
			Version:   auditlog.CurrentVersion,
			Timestamp: time.Date(2025, 12, 22, 10, 0, 1, 0, time.UTC),
			Type:      auditlog.EntryTypeLog,
			Details: &auditlog.LogDetails{
				Operation:  auditlog.OpCreateBucket,
				Phase:      auditlog.PhaseStart,
				Bucket:     "test-bucket",
				Key:        "test-key",
				Actor:      "test-actor",
			},
			PreviousHash:     make([]byte, sha512.Size),
			Hash:             make([]byte, sha512.Size),
			SignatureEd25519: make([]byte, 64),
		},
		{
			Version:   auditlog.CurrentVersion,
			Timestamp: time.Date(2025, 12, 22, 11, 0, 0, 0, time.UTC),
			Type:      auditlog.EntryTypeGrounding,
			Details: &auditlog.GroundingDetails{
				MerkleRootHash:   make([]byte, 64),
				SignatureEd25519: make([]byte, 64),
				SignatureMlDsa:   make([]byte, 64),
			},
			PreviousHash:     make([]byte, sha512.Size),
			Hash:             make([]byte, sha512.Size),
			SignatureEd25519: make([]byte, 64),
		},
	}

	for _, entry := range entries {
		for i := range entry.PreviousHash {
			entry.PreviousHash[i] = byte(i)
			entry.Hash[i] = byte(i + 1)
		}
		for i := range entry.SignatureEd25519 {
			entry.SignatureEd25519[i] = byte(i + 2)
		}
		if d, ok := entry.Details.(*auditlog.GroundingDetails); ok {
			for i := range d.MerkleRootHash {
				d.MerkleRootHash[i] = byte(i + 3)
				d.SignatureEd25519[i] = byte(i + 4)
				d.SignatureMlDsa[i] = byte(i + 5)
			}
		}
	}

	testCases := []struct {
		name       string
		serializer Serializer
	}{
		{
			name:       "Binary",
			serializer: &BinarySerializer{},
		},
		{
			name:       "JSON",
			serializer: &JsonSerializer{Indent: true},
		},
		{
			name:       "Text",
			serializer: &TextSerializer{},
		},
	}

	for _, ts := range testCases {
		t.Run(ts.name, func(t *testing.T) {
			for i, entry := range entries {
				t.Run(fmt.Sprintf("EntryType_%s", entry.Type), func(t *testing.T) {
					buf := new(bytes.Buffer)
					if err := ts.serializer.Encode(buf, entry); err != nil {
						t.Fatalf("Encode failed: %v", err)
					}

					dec := ts.serializer.NewDecoder(buf)
					decoded, err := dec.Decode()
					if err != nil {
						t.Fatalf("Decode failed: %v", err)
					}

					// Normalize timestamps to UTC for comparison
					decoded.Timestamp = decoded.Timestamp.UTC()
					expected := *entries[i]
					expected.Timestamp = expected.Timestamp.UTC()

					if !reflect.DeepEqual(expected, *decoded) {
						t.Errorf("Decoded entry mismatch\nExpected: %+v\nGot:      %+v", expected, *decoded)
					}
				})
			}
		})
	}
}

func BenchmarkSerializers(b *testing.B) {
	const batchSize = 1000
	_, priv, _ := ed25519.GenerateKey(rand.Reader)

	entries := make([]*auditlog.Entry, batchSize)
	prevHash := make([]byte, 64)
	for i := 0; i < batchSize; i++ {
		e := &auditlog.Entry{
			Version:   auditlog.CurrentVersion,
			Timestamp: time.Now().UTC(),
			Type:      auditlog.EntryTypeLog,
			Details: &auditlog.LogDetails{
				Operation: auditlog.OpPutObject,
				Phase:     auditlog.PhaseComplete,
				Bucket:    "benchmark-bucket",
				Key:       fmt.Sprintf("object-%d", i),
				Actor:     "admin",
			},
			PreviousHash: prevHash,
		}
		_ = e.Sign(signing.NewEd25519Signer(priv))
		entries[i] = e
		prevHash = e.Hash
	}

	serializers := []struct {
		name string
		s    Serializer
	}{
		{"Binary", &BinarySerializer{}},
		{"JSON", &JsonSerializer{Indent: false}},
		{"JSON-Indent", &JsonSerializer{Indent: true}},
		{"Text", &TextSerializer{}},
	}

	for _, ts := range serializers {
		// Prepare data to calculate batch size
		tmpBuf := new(bytes.Buffer)
		tmpBw := bufio.NewWriter(tmpBuf)
		for _, e := range entries {
			_ = ts.s.Encode(tmpBw, e)
		}
		_ = tmpBw.Flush()
		batchData := tmpBuf.Bytes()
		batchSizeInBytes := int64(len(batchData))

		b.Run(fmt.Sprintf("Encode/%s", ts.name), func(b *testing.B) {
			buf := new(bytes.Buffer)
			bw := bufio.NewWriter(buf)
			b.SetBytes(batchSizeInBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				buf.Reset()
				bw.Reset(buf)
				for _, e := range entries {
					_ = ts.s.Encode(bw, e)
				}
				_ = bw.Flush()
			}
		})

		b.Run(fmt.Sprintf("Decode/%s", ts.name), func(b *testing.B) {
			br := bytes.NewReader(batchData)
			bfr := bufio.NewReader(br)
			b.SetBytes(batchSizeInBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				br.Reset(batchData)
				bfr.Reset(br)
				dec := ts.s.NewDecoder(bfr)
				for range batchSize {
					_, err := dec.Decode()
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}