package audit

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
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

func (m *mockStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	return &storage.PutObjectResult{}, nil
}

func (m *mockStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	return &storage.CopyObjectResult{ETag: "copy-etag", LastModified: time.Now().UTC()}, nil
}

func (m *mockStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	return &storage.UploadPartCopyResult{ETag: "part-copy-etag", LastModified: time.Now().UTC()}, nil
}

func TestAuditLogMiddleware(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	_, mlPriv, err := mldsa87.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp("", "audit_log_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	s, err := sink.NewFileSink(tmpFile.Name(), &serialization.BinarySerializer{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	state, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	var lastHash []byte
	var initialBuffer [][]byte
	if state != nil {
		lastHash = state.LastHash
		initialBuffer = state.HashBuffer
	}

	mock := &mockStorage{}
	middleware := NewAuditLogMiddleware(mock, s, signing.NewEd25519Signer(priv), signing.NewMlDsa87Signer(mlPriv), lastHash, initialBuffer)

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
	if dStart.Resource.Bucket != "test-bucket" {
		t.Errorf("expected bucket test-bucket, got %s", dStart.Resource.Bucket)
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
	if dEnd.Outcome.Outcome != auditlog.OutcomeSuccess {
		t.Errorf("expected success outcome, got %s", dEnd.Outcome.Outcome)
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

func TestAuditLogMiddlewareCopyResources(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	_, mlPriv, err := mldsa87.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp("", "audit_log_copy_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	s, err := sink.NewFileSink(tmpFile.Name(), &serialization.BinarySerializer{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mock := &mockStorage{}
	middleware := NewAuditLogMiddleware(mock, s, signing.NewEd25519Signer(priv), signing.NewMlDsa87Signer(mlPriv), nil, nil)

	ctx := context.Background()
	srcBucket := storage.MustNewBucketName("source-bucket")
	srcKey := storage.MustNewObjectKey("source-key")
	dstBucket := storage.MustNewBucketName("dest-bucket")
	dstKey := storage.MustNewObjectKey("dest-key")

	if _, err := middleware.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := middleware.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, storage.MustNewUploadId("upload-1"), 7, nil); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	dec := (&serialization.BinarySerializer{}).NewDecoder(f)
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}

	assertCopyEntry := func(operation auditlog.Operation, uploadID string, partNumber int32) {
		t.Helper()
		for _, phase := range []auditlog.Phase{auditlog.PhaseStart, auditlog.PhaseComplete} {
			entry, err := dec.Decode()
			if err != nil {
				t.Fatal(err)
			}
			details := entry.Details.(*auditlog.LogDetails)
			if details.Operation != operation {
				t.Fatalf("expected operation %s, got %s", operation, details.Operation)
			}
			if details.Phase != phase {
				t.Fatalf("expected phase %s, got %s", phase, details.Phase)
			}
			if details.Resource.Bucket != "dest-bucket" {
				t.Errorf("expected destination bucket dest-bucket, got %s", details.Resource.Bucket)
			}
			if details.Resource.Key != "dest-key" {
				t.Errorf("expected destination key dest-key, got %s", details.Resource.Key)
			}
			if details.Resource.SourceBucket != "source-bucket" {
				t.Errorf("expected source bucket source-bucket, got %s", details.Resource.SourceBucket)
			}
			if details.Resource.SourceKey != "source-key" {
				t.Errorf("expected source key source-key, got %s", details.Resource.SourceKey)
			}
			if details.Resource.UploadID != uploadID {
				t.Errorf("expected upload id %s, got %s", uploadID, details.Resource.UploadID)
			}
			if details.Resource.PartNumber != partNumber {
				t.Errorf("expected part number %d, got %d", partNumber, details.Resource.PartNumber)
			}
		}
	}

	assertCopyEntry(auditlog.OpCopyObject, "", 0)
	assertCopyEntry(auditlog.OpUploadPartCopy, "upload-1", 7)
}
