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
	"go.opentelemetry.io/otel/trace"
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

func (m *AuditLogMiddleware) log(ctx context.Context, op auditlog.Operation, phase auditlog.Phase, bucket string, key string, uploadId string, partNumber int32, err error, statusCode int32, durationMs int64) {
	credentialID := ""
	if val := ctx.Value(authentication.AccessKeyIdContextKey{}); val != nil {
		if s, ok := val.(string); ok {
			credentialID = s
		}
	}

	authType := auditlog.AuthTypeAnonymous
	if val := ctx.Value(authentication.AuthTypeContextKey{}); val != nil {
		if s, ok := val.(string); ok {
			authType = auditlog.AuthType(s)
		}
	}

	requestID := ""
	if val := ctx.Value(authentication.RequestIDContextKey{}); val != nil {
		if s, ok := val.(string); ok {
			requestID = s
		}
	}

	clientIP := ""
	if val := ctx.Value(authentication.ClientIPContextKey{}); val != nil {
		if s, ok := val.(string); ok {
			clientIP = s
		}
	}

	traceID := ""
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.HasTraceID() {
		traceID = spanContext.TraceID().String()
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	outcome := auditlog.OutcomePending
	if phase == auditlog.PhaseComplete {
		if err != nil {
			outcome = auditlog.OutcomeError
		} else {
			outcome = auditlog.OutcomeSuccess
		}
	}

	entry := &auditlog.Entry{
		Version:   auditlog.CurrentVersion,
		Timestamp: time.Now(),
		Type:      auditlog.EntryTypeLog,
		Details: &auditlog.LogDetails{
			Operation: op,
			Phase:     phase,
			Resource: auditlog.ResourceDetails{
				Bucket:     bucket,
				Key:        key,
				UploadID:   uploadId,
				PartNumber: partNumber,
			},
			Actor: auditlog.ActorDetails{
				CredentialID: credentialID,
				AuthType:     authType,
			},
			Request: auditlog.RequestDetails{
				RequestID: requestID,
				TraceID:   traceID,
				ClientIP:  clientIP,
			},
			Outcome: auditlog.OutcomeDetails{
				StatusCode: statusCode,
				Outcome:    outcome,
				Error:      errMsg,
				DurationMs: durationMs,
			},
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
			SignatureMlDsa87: sigMl,
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
	start := time.Now()
	m.log(ctx, auditlog.OpCreateBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.CreateBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpCreateBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	start := time.Now()
	m.log(ctx, auditlog.OpDeleteBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.DeleteBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpDeleteBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpListBuckets, auditlog.PhaseStart, "", "", "", 0, nil, 0, 0)
	buckets, err := m.next.ListBuckets(ctx)
	m.log(ctx, auditlog.OpListBuckets, auditlog.PhaseComplete, "", "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return buckets, err
}

func (m *AuditLogMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpHeadBucket, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	bucket, err := m.next.HeadBucket(ctx, bucketName)
	m.log(ctx, auditlog.OpHeadBucket, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return bucket, err
}

func (m *AuditLogMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpGetBucketWebsite, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	config, err := m.next.GetBucketWebsiteConfiguration(ctx, bucketName)
	m.log(ctx, auditlog.OpGetBucketWebsite, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return config, err
}

func (m *AuditLogMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	start := time.Now()
	m.log(ctx, auditlog.OpPutBucketWebsite, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	m.log(ctx, auditlog.OpPutBucketWebsite, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	start := time.Now()
	m.log(ctx, auditlog.OpDeleteBucketWebsite, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	m.log(ctx, auditlog.OpDeleteBucketWebsite, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpGetBucketCORS, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	config, err := m.next.GetBucketCORSConfiguration(ctx, bucketName)
	m.log(ctx, auditlog.OpGetBucketCORS, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return config, err
}

func (m *AuditLogMiddleware) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	start := time.Now()
	m.log(ctx, auditlog.OpPutBucketCORS, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.PutBucketCORSConfiguration(ctx, bucketName, config)
	m.log(ctx, auditlog.OpPutBucketCORS, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	start := time.Now()
	m.log(ctx, auditlog.OpDeleteBucketCORS, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	err := m.next.DeleteBucketCORSConfiguration(ctx, bucketName)
	m.log(ctx, auditlog.OpDeleteBucketCORS, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpListObjects, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	res, err := m.next.ListObjects(ctx, bucketName, opts)
	m.log(ctx, auditlog.OpListObjects, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpHeadObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	obj, err := m.next.HeadObject(ctx, bucketName, key, opts)
	m.log(ctx, auditlog.OpHeadObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return obj, err
}

func (m *AuditLogMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	obj, readers, err := m.next.GetObject(ctx, bucketName, key, ranges, opts)
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return obj, readers, err
}

func (m *AuditLogMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpPutObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	res, err := m.next.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
	m.log(ctx, auditlog.OpPutObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpAppendObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	res, err := m.next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
	m.log(ctx, auditlog.OpAppendObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	start := time.Now()
	m.log(ctx, auditlog.OpDeleteObject, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	err := m.next.DeleteObject(ctx, bucketName, key, opts)
	m.log(ctx, auditlog.OpDeleteObject, auditlog.PhaseComplete, bucketName.String(), key.String(), "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpDeleteObjects, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	result, err := m.next.DeleteObjects(ctx, bucketName, entries)
	m.log(ctx, auditlog.OpDeleteObjects, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return result, err
}

func (m *AuditLogMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), "", 0, nil, 0, 0)
	res, err := m.next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	uid := ""
	if res != nil {
		uid = res.UploadId.String()
	}
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uid, 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpUploadPart, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), partNumber, nil, 0, 0)
	res, err := m.next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	m.log(ctx, auditlog.OpUploadPart, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), partNumber, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpCompleteMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil, 0, 0)
	res, err := m.next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
	m.log(ctx, auditlog.OpCompleteMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	start := time.Now()
	m.log(ctx, auditlog.OpAbortMultipartUpload, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil, 0, 0)
	err := m.next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	m.log(ctx, auditlog.OpAbortMultipartUpload, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func (m *AuditLogMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpListMultipartUploads, auditlog.PhaseStart, bucketName.String(), "", "", 0, nil, 0, 0)
	res, err := m.next.ListMultipartUploads(ctx, bucketName, opts)
	m.log(ctx, auditlog.OpListMultipartUploads, auditlog.PhaseComplete, bucketName.String(), "", "", 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	start := time.Now()
	m.log(ctx, auditlog.OpListParts, auditlog.PhaseStart, bucketName.String(), key.String(), uploadId.String(), 0, nil, 0, 0)
	res, err := m.next.ListParts(ctx, bucketName, key, uploadId, opts)
	m.log(ctx, auditlog.OpListParts, auditlog.PhaseComplete, bucketName.String(), key.String(), uploadId.String(), 0, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func statusCodeFromError(err error) int32 {
	if err != nil {
		return 500
	}
	return 200
}

// Ensure interface compliance
var _ storage.Storage = (*AuditLogMiddleware)(nil)
var _ lifecycle.Manager = (*AuditLogMiddleware)(nil)
