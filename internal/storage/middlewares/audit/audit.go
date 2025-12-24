package audit

import (
	"context"
	"io"
	"time"

	"crypto/sha512"
	"sync"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type AuditLogMiddleware struct {
	next        storage.Storage
	sink        sink.Sink
	signer      signing.Signer
	mlDsaSigner signing.Signer
	lastHash    []byte
	hashBuffer  [][]byte
	mu          sync.Mutex
}

func NewAuditLogMiddleware(next storage.Storage, sink sink.Sink, signer signing.Signer, mlDsaSigner signing.Signer, lastHash []byte, initialHashBuffer [][]byte) *AuditLogMiddleware {
	m := &AuditLogMiddleware{
		next:        next,
		sink:        sink,
		signer:      signer,
		mlDsaSigner: mlDsaSigner,
		lastHash:    lastHash,
		hashBuffer:  initialHashBuffer,
	}
	
	if m.hashBuffer == nil {
		m.hashBuffer = make([][]byte, 0, auditlog.GroundingBlockSize)
	}
	
	isZero := true
	for _, b := range lastHash {
		if b != 0 {
			isZero = false
			break
		}
	}
	
	if isZero {
		pithosHash := sha512.Sum512([]byte("pithos"))
		genesis := &auditlog.Entry{
			Version:      auditlog.CurrentVersion,
			Timestamp:    time.Now().UTC(),
			Type:         auditlog.EntryTypeGenesis,
			Details:      &auditlog.GenesisDetails{},
			PreviousHash: pithosHash[:],
		}
		_ = genesis.Sign(signer)
		if err := sink.WriteEntry(genesis); err == nil {
			m.lastHash = genesis.Hash
		}
	}
	
	return m
}

func (m *AuditLogMiddleware) log(ctx context.Context, op auditlog.Operation, phase auditlog.Phase, bucket string, key string, uploadId string, partNumber int32, err error) {
	actor := "anonymous"
	if val := ctx.Value(authentication.AccessKeyIdContextKey{}); val != nil {
		if s, ok := val.(string); ok {
			actor = s
		}
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	entry := &auditlog.Entry{
		Version:   auditlog.CurrentVersion,
		Timestamp: time.Now(),
		Type:      auditlog.EntryTypeLog,
		Details: &auditlog.LogDetails{
			Operation:  op,
			Phase:      phase,
			Bucket:     bucket,
			Key:        key,
			UploadID:   uploadId,
			PartNumber: partNumber,
			Actor:      actor,
			Error:      errMsg,
		},
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry.PreviousHash = m.lastHash
	_ = entry.Sign(m.signer)

	if err := m.sink.WriteEntry(entry); err == nil {
		m.lastHash = entry.Hash
		m.hashBuffer = append(m.hashBuffer, entry.Hash)
		
		if len(m.hashBuffer) >= auditlog.GroundingBlockSize {
			m.emitGrounding()
		}
	}
}

func (m *AuditLogMiddleware) emitGrounding() {
	root := auditlog.CalculateMerkleRoot(m.hashBuffer)
	
	sigEd, _ := m.signer.Sign(root)
	sigMl, _ := m.mlDsaSigner.Sign(root)
	
	grounding := &auditlog.Entry{
		Version:   auditlog.CurrentVersion,
		Timestamp: time.Now().UTC(),
		Type:      auditlog.EntryTypeGrounding,
		Details: &auditlog.GroundingDetails{
			MerkleRootHash:   root,
			SignatureEd25519: sigEd,
			SignatureMlDsa87:   sigMl,
		},
		PreviousHash: m.lastHash,
	}
	
	_ = grounding.Sign(m.signer)
	if err := m.sink.WriteEntry(grounding); err == nil {
		m.lastHash = grounding.Hash
		m.hashBuffer = m.hashBuffer[:0]
	}
}

func (m *AuditLogMiddleware) Start(ctx context.Context) error {
	return m.next.Start(ctx)
}

func (m *AuditLogMiddleware) Stop(ctx context.Context) error {
	_ = m.sink.Close()
	return m.next.Stop(ctx)
}

func (m *AuditLogMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	m.log(ctx, auditlog.OpCreateBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil)
	err := m.next.CreateBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpCreateBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err)
	return err
}

func (m *AuditLogMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	m.log(ctx, auditlog.OpDeleteBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil)
	err := m.next.DeleteBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpDeleteBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err)
	return err
}

func (m *AuditLogMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	m.log(ctx, auditlog.OpListBuckets, auditlog.PhaseStart, "", "", "", 0, nil)
	buckets, err := m.next.ListBuckets(ctx)
	m.log(ctx, auditlog.OpListBuckets, auditlog.PhaseComplete, "", "", "", 0, err)
	return buckets, err
}

func (m *AuditLogMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	m.log(ctx, auditlog.OpHeadBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil)
	bucket, err := m.next.HeadBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpHeadBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err)
	return bucket, err
}

func (m *AuditLogMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	m.log(ctx, auditlog.OpListObjects, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil)
	res, err := m.next.ListObjects(ctx, bucketName, opts)
	m.log(ctx, auditlog.OpListObjects, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err)
	return res, err
}

func (m *AuditLogMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) (*storage.Object, error) {
	m.log(ctx, auditlog.OpHeadObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil)
	obj, err := m.next.HeadObject(ctx, bucketName, key)
	m.log(ctx, auditlog.OpHeadObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err)
	return obj, err
}

func (m *AuditLogMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange) (*storage.Object, []io.ReadCloser, error) {
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil)
	obj, readers, err := m.next.GetObject(ctx, bucketName, key, ranges)
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err)
	return obj, readers, err
}

func (m *AuditLogMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.PutObjectResult, error) {
	m.log(ctx, auditlog.OpPutObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil)
	res, err := m.next.PutObject(ctx, bucketName, key, contentType, data, checksumInput)
	m.log(ctx, auditlog.OpPutObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err)
	return res, err
}

func (m *AuditLogMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey) error {
	m.log(ctx, auditlog.OpDeleteObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil)
	err := m.next.DeleteObject(ctx, bucketName, key)
	m.log(ctx, auditlog.OpDeleteObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err)
	return err
}

func (m *AuditLogMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil)
	res, err := m.next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	uid := ""
	if res != nil {
		uid = res.UploadId.String()
	}
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uid, 0, err)
	return res, err
}

func (m *AuditLogMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	m.log(ctx, auditlog.OpUploadPart, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), partNumber, nil)
	res, err := m.next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	m.log(ctx, auditlog.OpUploadPart, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), partNumber, err)
	return res, err
}

func (m *AuditLogMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput) (*storage.CompleteMultipartUploadResult, error) {
	m.log(ctx, auditlog.OpCompleteMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil)
	res, err := m.next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
	m.log(ctx, auditlog.OpCompleteMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err)
	return res, err
}

func (m *AuditLogMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	m.log(ctx, auditlog.OpAbortMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil)
	err := m.next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	m.log(ctx, auditlog.OpAbortMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err)
	return err
}

func (m *AuditLogMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	m.log(ctx, auditlog.OpListMultipartUploads, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil)
	res, err := m.next.ListMultipartUploads(ctx, bucketName, opts)
	m.log(ctx, auditlog.OpListMultipartUploads, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err)
	return res, err
}

func (m *AuditLogMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	m.log(ctx, auditlog.OpListParts, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil)
	res, err := m.next.ListParts(ctx, bucketName, key, uploadId, opts)
	m.log(ctx, auditlog.OpListParts, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err)
	return res, err
}

// Ensure interface compliance
var _ storage.Storage = (*AuditLogMiddleware)(nil)
var _ lifecycle.Manager = (*AuditLogMiddleware)(nil)