package tool

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	_ "github.com/jdillenkofer/pithos/internal/testing"
)

func TestRunAuditLogTool(t *testing.T) {
	edPub, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	mlPub, mlPriv, _ := mldsa65.GenerateKey(rand.Reader)

	edSigner := signing.NewEd25519Signer(edPriv)
	edVerifier := signing.NewEd25519Verifier(edPub)
	mlSigner := signing.NewMlDsaSigner(mlPriv)
	mlVerifier := signing.NewMlDsaVerifier(mlPub)

	tmpFile, _ := os.CreateTemp("", "cli_test")
	defer os.Remove(tmpFile.Name())

	serializer := &serialization.BinarySerializer{}
	sink := sink.NewWriterSink(tmpFile, serializer)

	// 1. Genesis
	pithosHash := sha512.Sum512([]byte("pithos"))
	genesis := &auditlog.Entry{
		Version:      1,
		Timestamp:    time.Now().UTC(),
		Type:         auditlog.EntryTypeGenesis,
		Details:      &auditlog.GenesisDetails{},
		PreviousHash: pithosHash[:],
	}
	_ = genesis.Sign(edSigner)
	_ = sink.WriteEntry(genesis)

	// 2. Logs
	var logs []*auditlog.Entry
	prevHash := genesis.Hash
	for i := 0; i < auditlog.GroundingBlockSize; i++ {
		entry := &auditlog.Entry{
			Version:   1,
			Timestamp: time.Now().UTC(),
			Type:      auditlog.EntryTypeLog,
			Details: &auditlog.LogDetails{
				Operation: auditlog.OpPutObject,
				Bucket:    "test",
			},
			PreviousHash: prevHash,
		}
		_ = entry.Sign(edSigner)
		_ = sink.WriteEntry(entry)
		logs = append(logs, entry)
		prevHash = entry.Hash
	}

	// 3. Grounding
	var hashBuffer [][]byte
	for _, l := range logs {
		hashBuffer = append(hashBuffer, l.Hash)
	}
	root := auditlog.CalculateMerkleRoot(hashBuffer)
	sigEd, _ := edSigner.Sign(root)
	sigMl, _ := mlSigner.Sign(root)

	grounding := &auditlog.Entry{
		Version:   1,
		Timestamp: time.Now().UTC(),
		Type:      auditlog.EntryTypeGrounding,
		Details: &auditlog.GroundingDetails{
			MerkleRootHash:   root,
			SignatureEd25519: sigEd,
			SignatureMlDsa:   sigMl,
		},
		PreviousHash: prevHash,
	}
	_ = grounding.Sign(edSigner)
	_ = sink.WriteEntry(grounding)
	tmpFile.Close()

	// Verify
	err := RunAuditLogTool(tmpFile.Name(), edVerifier, mlVerifier, "text", io.Discard)
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
}
