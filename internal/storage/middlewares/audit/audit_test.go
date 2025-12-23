package audit

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"io"
	"os"
	"testing"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	_ "github.com/jdillenkofer/pithos/internal/testing"
)

type mockStorage struct {
	storage.Storage
}

func (m *mockStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	return nil
}

func (m *mockStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	return &storage.PutObjectResult{}, nil
}

func TestAuditLogMiddleware(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	_, mlPriv, err := mldsa65.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp("", "audit_log_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	s, lastHash, err := sink.NewBinaryFileSink(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mock := &mockStorage{}
	middleware := NewAuditLogMiddleware(mock, s, signing.NewEd25519Signer(priv), signing.NewMlDsaSigner(mlPriv), lastHash)

	ctx := context.Background()
	bucketName := metadatastore.MustNewBucketName("test-bucket")
	err = middleware.CreateBucket(ctx, bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Verify log entry
	f, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read genesis
	serializer := &serialization.BinarySerializer{}
	dec := serializer.NewDecoder(f)
	genesis, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if genesis.Type != auditlog.EntryTypeGenesis {
		t.Errorf("expected GENESIS type, got %s", genesis.Type)
	}
	expectedPrevHash := sha512.Sum512([]byte("pithos"))
	if string(genesis.PreviousHash) != string(expectedPrevHash[:]) {
		t.Error("genesis previous hash mismatch")
	}
	if !genesis.Verify(signing.NewEd25519Verifier(pub)) {
		t.Error("genesis signature verification failed")
	}

	// Read entry 1 - START
	entryStart, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if entryStart.Type != auditlog.EntryTypeLog {
		t.Errorf("expected LOG type, got %s", entryStart.Type)
	}
	dStart := entryStart.Details.(*auditlog.LogDetails)
	if dStart.Operation != auditlog.OpCreateBucket {
		t.Errorf("expected OpCreateBucket, got %s", dStart.Operation)
	}
	if dStart.Phase != auditlog.PhaseStart {
		t.Errorf("expected START phase, got %s", dStart.Phase)
	}
	if dStart.Bucket != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %s", dStart.Bucket)
	}
	if !entryStart.Verify(signing.NewEd25519Verifier(pub)) {
		t.Error("entryStart signature verification failed")
	}
	
	// Read entry 2 - COMPLETE
	entryEnd, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if entryEnd.Type != auditlog.EntryTypeLog {
		t.Errorf("expected LOG type, got %s", entryEnd.Type)
	}
	dEnd := entryEnd.Details.(*auditlog.LogDetails)
	if dEnd.Operation != auditlog.OpCreateBucket {
		t.Errorf("expected OpCreateBucket, got %s", dEnd.Operation)
	}
	if dEnd.Phase != auditlog.PhaseComplete {
		t.Errorf("expected COMPLETE phase, got %s", dEnd.Phase)
	}
	if !entryEnd.Verify(signing.NewEd25519Verifier(pub)) {
		t.Error("entryEnd signature verification failed")
	}

	// Check chaining
	if string(entryStart.PreviousHash) != string(genesis.Hash) {
		t.Error("hash chaining broken at start")
	}
	if string(entryEnd.PreviousHash) != string(entryStart.Hash) {
		t.Error("hash chaining broken at end")
	}
}