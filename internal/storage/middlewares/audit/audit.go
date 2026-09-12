package audit

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"time"

	"crypto/sha512"
	"sync"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

type AuditLogMiddleware struct {
	delegator.DelegatingStorage
	sink        sink.Sink
	signer      signing.Signer
	mlDsaSigner signing.Signer
	lastHash    []byte
	hashBuffer  [][]byte
	mu          sync.Mutex
	entries     prometheus.Counter
	logSize     prometheus.Gauge
}

var _ storage.TransactionalStorage = (*AuditLogMiddleware)(nil)

func (m *AuditLogMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

type auditResource struct {
	versionID    *string
	objectLock   *auditlog.ObjectLockDetails
	denied       bool
	bucket       string
	key          string
	uploadID     string
	partNumber   int32
	sourceBucket string
	sourceKey    string
}

func (m *AuditLogMiddleware) run(ctx context.Context, op auditlog.Operation, resource auditResource, fn func(context.Context) error) error {
	start := time.Now()
	m.log(ctx, op, auditlog.PhaseStart, resource, nil, 0, 0)
	ctx = storage.WithObjectLockObserver(ctx, func(observation storage.ObjectLockObservation) {
		if observation.VersionID != nil {
			resource.versionID = observation.VersionID
		}
		if resource.objectLock == nil {
			resource.objectLock = &auditlog.ObjectLockDetails{}
		}
		resource.objectLock.Effective = lockValues(observation.Effective)
		if observation.Configuration != nil {
			resource.objectLock.Effective = configurationValues(observation.Configuration)
		}
		resource.objectLock.BypassUsed = observation.BypassUsed
	})
	err := fn(ctx)
	m.log(ctx, op, auditlog.PhaseComplete, resource, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return err
}

func NewAuditLogMiddleware(next storage.Storage, sink sink.Sink, signer signing.Signer, mlDsaSigner signing.Signer, lastHash []byte, initialHashBuffer [][]byte) *AuditLogMiddleware {
	m := &AuditLogMiddleware{
		DelegatingStorage: delegator.Wrap(next),
		sink:              sink,
		signer:            signer,
		mlDsaSigner:       mlDsaSigner,
		lastHash:          lastHash,
		hashBuffer:        initialHashBuffer,
		entries:           prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "audit", Name: "entries_total", Help: "Number of successfully appended audit log entries"}),
		logSize:           prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "audit", Name: "log_size_bytes", Help: "Current audit log size in bytes"}),
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
			m.observeWrite()
		}
	}

	return m
}

func (m *AuditLogMiddleware) observeWrite() {
	m.entries.Inc()
	size := int64(0)
	if provider, ok := m.sink.(sink.SizeProvider); ok {
		size = provider.SizeBytes()
	}
	m.logSize.Set(float64(size))
}

func (m *AuditLogMiddleware) log(ctx context.Context, op auditlog.Operation, phase auditlog.Phase, resource auditResource, err error, statusCode int32, durationMs int64) {
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
		if resource.denied || errors.Is(err, storage.ErrObjectLockAccessDenied) {
			outcome = auditlog.OutcomeDenied
		} else if err != nil {
			outcome = auditlog.OutcomeError
		} else {
			outcome = auditlog.OutcomeSuccess
		}
	}

	versionID := ""
	if resource.versionID != nil {
		versionID = *resource.versionID
	}
	var lock *auditlog.ObjectLockDetails
	if resource.objectLock != nil {
		copy := *resource.objectLock
		lock = &copy
	}
	entry := &auditlog.Entry{
		Version:   auditlog.CurrentVersion,
		Timestamp: time.Now(),
		Type:      auditlog.EntryTypeLog,
		Details: &auditlog.LogDetails{
			ObjectLock: lock,
			Operation:  op,
			Phase:      phase,
			Resource: auditlog.ResourceDetails{
				VersionID:    versionID,
				Bucket:       resource.bucket,
				Key:          resource.key,
				UploadID:     resource.uploadID,
				PartNumber:   resource.partNumber,
				SourceBucket: resource.sourceBucket,
				SourceKey:    resource.sourceKey,
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
		m.observeWrite()
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
		m.observeWrite()
		m.lastHash = grounding.Hash
		m.hashBuffer = m.hashBuffer[:0]
	}
}

func (m *AuditLogMiddleware) Start(ctx context.Context) error {
	pithosmetrics.Register(m.entries, m.logSize)
	return m.Next.Start(ctx)
}

func (m *AuditLogMiddleware) Stop(ctx context.Context) error {
	pithosmetrics.Unregister(m.entries, m.logSize)
	_ = m.sink.Close()
	return m.Next.Stop(ctx)
}

func (m *AuditLogMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName, options ...storage.CreateBucketOptions) error {
	details := &auditlog.ObjectLockDetails{}
	if len(options) == 1 && options[0].ObjectLockEnabled {
		details.Requested.Enabled = "Enabled"
	}
	return m.run(ctx, auditlog.OpCreateBucket, auditResource{objectLock: details, bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.CreateBucket(ctx, bucketName, options...)
	})
}

func (m *AuditLogMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	return m.run(ctx, auditlog.OpDeleteBucket, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.DeleteBucket(ctx, bucketName)
	})
}

func (m *AuditLogMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	var result []storage.Bucket
	err := m.run(ctx, auditlog.OpListBuckets, auditResource{}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.ListBuckets(ctx)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	var result *storage.Bucket
	err := m.run(ctx, auditlog.OpHeadBucket, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.HeadBucket(ctx, bucketName)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	var result *storage.WebsiteConfiguration
	err := m.run(ctx, auditlog.OpGetBucketWebsite, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetBucketWebsiteConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	return m.run(ctx, auditlog.OpPutBucketWebsite, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	})
}

func (m *AuditLogMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return m.run(ctx, auditlog.OpDeleteBucketWebsite, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	})
}

func (m *AuditLogMiddleware) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	var result *storage.BucketCORSConfiguration
	err := m.run(ctx, auditlog.OpGetBucketCORS, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetBucketCORSConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	return m.run(ctx, auditlog.OpPutBucketCORS, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.PutBucketCORSConfiguration(ctx, bucketName, config)
	})
}

func (m *AuditLogMiddleware) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return m.run(ctx, auditlog.OpDeleteBucketCORS, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.DeleteBucketCORSConfiguration(ctx, bucketName)
	})
}

func (m *AuditLogMiddleware) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	var result *storage.BucketLifecycleConfiguration
	err := m.run(ctx, auditlog.OpGetBucketLifecycle, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetBucketLifecycleConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	return m.run(ctx, auditlog.OpPutBucketLifecycle, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.PutBucketLifecycleConfiguration(ctx, bucketName, config)
	})
}

func (m *AuditLogMiddleware) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return m.run(ctx, auditlog.OpDeleteBucketLifecycle, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.DeleteBucketLifecycleConfiguration(ctx, bucketName)
	})
}

func (m *AuditLogMiddleware) GetBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	var result *storage.BucketVersioningConfiguration
	err := m.run(ctx, auditlog.OpGetBucketVersioning, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.GetBucketVersioningConfiguration(ctx, bucketName)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	return m.run(ctx, auditlog.OpPutBucketVersioning, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		return m.Next.PutBucketVersioningConfiguration(ctx, bucketName, config)
	})
}

func (m *AuditLogMiddleware) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	var result *storage.ListObjectVersionsResult
	err := m.run(ctx, auditlog.OpListObjectVersions, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.ListObjectVersions(ctx, bucketName, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	var result *storage.ListBucketResult
	err := m.run(ctx, auditlog.OpListObjects, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.ListObjects(ctx, bucketName, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	resource := auditResource{bucket: bucketName.String(), key: key.String()}
	if opts != nil {
		resource.versionID = opts.VersionID
	}
	var result *storage.Object
	err := m.run(ctx, auditlog.OpHeadObject, resource, func(ctx context.Context) error {
		var err error
		result, err = m.Next.HeadObject(ctx, bucketName, key, opts)
		if err == nil && result != nil {
			storage.ObserveObjectLock(ctx, storage.ObjectLockObservation{Key: key.String(), VersionID: result.VersionID, Effective: result.ObjectLock})
		}
		return err
	})
	return result, err
}
func (m *AuditLogMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	start := time.Now()
	resource := auditResource{bucket: bucketName.String(), key: key.String()}
	if opts != nil {
		resource.versionID = opts.VersionID
	}
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseStart, resource, nil, 0, 0)
	obj, readers, err := m.Next.GetObject(ctx, bucketName, key, ranges, opts)
	if err == nil && obj != nil {
		resource.versionID = obj.VersionID
		resource.objectLock = &auditlog.ObjectLockDetails{Effective: lockValues(obj.ObjectLock)}
	}
	m.log(ctx, auditlog.OpGetObject, auditlog.PhaseComplete, resource, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return obj, readers, err
}

func (m *AuditLogMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	requested := storage.ObjectLock{}
	if opts != nil {
		requested = opts.ObjectLock
	}
	var result *storage.PutObjectResult
	err := m.run(ctx, auditlog.OpPutObject, auditResource{objectLock: lockDetails(requested, false), bucket: bucketName.String(), key: key.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	requested := storage.ObjectLock{}
	if opts != nil {
		requested = opts.ObjectLock
	}
	var result *storage.CopyObjectResult
	err := m.run(ctx, auditlog.OpCopyObject, auditResource{objectLock: lockDetails(requested, false), bucket: dstBucket.String(), key: dstKey.String(), sourceBucket: srcBucket.String(), sourceKey: srcKey.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	var result *storage.AppendObjectResult
	err := m.run(ctx, auditlog.OpAppendObject, auditResource{bucket: bucketName.String(), key: key.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	var versionID *string
	bypass := false
	if opts != nil {
		versionID = opts.VersionID
		bypass = opts.BypassGovernanceRetention
	}
	var result *storage.DeleteObjectResult
	err := m.run(ctx, auditlog.OpDeleteObject, auditResource{versionID: versionID, objectLock: lockDetails(storage.ObjectLock{}, bypass), bucket: bucketName.String(), key: key.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.DeleteObject(ctx, bucketName, key, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	start := time.Now()
	resources := make([]auditResource, len(entries))
	for i, entry := range entries {
		resources[i] = auditResource{bucket: bucketName.String(), key: entry.Key.String(), versionID: entry.VersionID, objectLock: lockDetails(storage.ObjectLock{}, entry.BypassGovernanceRetention)}
		m.log(ctx, auditlog.OpDeleteObjects, auditlog.PhaseStart, resources[i], nil, 0, 0)
	}
	var observations []storage.ObjectLockObservation
	observedCtx := storage.WithObjectLockObserver(ctx, func(o storage.ObjectLockObservation) { observations = append(observations, o) })
	result, err := m.Next.DeleteObjects(observedCtx, bucketName, entries)
	usedResults := make(map[int]bool)
	for i, entry := range entries {
		resource := resources[i]
		for j, observation := range observations {
			if observation.Key != entry.Key.String() {
				continue
			}
			if entry.VersionID != nil && (observation.VersionID == nil || *entry.VersionID != *observation.VersionID) {
				continue
			}
			resource.objectLock.Effective = lockValues(observation.Effective)
			resource.objectLock.BypassUsed = observation.BypassUsed
			resource.versionID = observation.VersionID
			observations = append(observations[:j], observations[j+1:]...)
			break
		}
		entryErr := err
		status := statusCodeFromError(err)
		if err == nil && result != nil {
			var outcome *storage.DeleteObjectsEntry
			for j := range result.Entries {
				candidate := &result.Entries[j]
				if usedResults[j] || candidate.Key.String() != entry.Key.String() {
					continue
				}
				if entry.VersionID != nil && (candidate.VersionID == nil || *candidate.VersionID != *entry.VersionID) {
					continue
				}
				if entry.VersionID == nil && candidate.VersionID != nil && candidate.DeleteMarkerVersionID == nil {
					continue
				}
				outcome = candidate
				usedResults[j] = true
				break
			}
			if outcome == nil {
				m.log(ctx, auditlog.OpDeleteObjects, auditlog.PhaseComplete, resource, errors.New("missing per-version deletion result"), 500, time.Since(start).Milliseconds())
				continue
			}
			if outcome.VersionID != nil {
				resource.versionID = outcome.VersionID
			}
			if outcome.DeleteMarkerVersionID != nil {
				resource.versionID = outcome.DeleteMarkerVersionID
			}
			if !outcome.Deleted {
				entryErr = errors.New(outcome.ErrCode + ": " + outcome.ErrMsg)
				status = 500
				if outcome.ErrCode == "AccessDenied" {
					resource.denied = true
					status = 403
				}
				if outcome.ErrCode == "PreconditionFailed" {
					status = 412
				}
			}
		}
		m.log(ctx, auditlog.OpDeleteObjects, auditlog.PhaseComplete, resource, entryErr, status, time.Since(start).Milliseconds())
	}
	return result, err
}

func (m *AuditLogMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	requested := storage.ObjectLock{}
	if opts != nil {
		requested = opts.ObjectLock
	}
	start := time.Now()
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseStart, auditResource{objectLock: lockDetails(requested, false), bucket: bucketName.String(), key: key.String()}, nil, 0, 0)
	res, err := m.Next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, opts)
	uid := ""
	if res != nil {
		uid = res.UploadId.String()
	}
	m.log(ctx, auditlog.OpCreateMultipartUpload, auditlog.PhaseComplete, auditResource{objectLock: lockDetails(requested, false), bucket: bucketName.String(), key: key.String(), uploadID: uid}, err, statusCodeFromError(err), time.Since(start).Milliseconds())
	return res, err
}

func (m *AuditLogMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	var result *storage.UploadPartResult
	err := m.run(ctx, auditlog.OpUploadPart, auditResource{bucket: bucketName.String(), key: key.String(), uploadID: uploadId.String(), partNumber: partNumber}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	var result *storage.UploadPartCopyResult
	err := m.run(ctx, auditlog.OpUploadPartCopy, auditResource{bucket: dstBucket.String(), key: dstKey.String(), uploadID: uploadId.String(), partNumber: partNumber, sourceBucket: srcBucket.String(), sourceKey: srcKey.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.UploadPartCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, uploadId, partNumber, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	var result *storage.CompleteMultipartUploadResult
	err := m.run(ctx, auditlog.OpCompleteMultipartUpload, auditResource{bucket: bucketName.String(), key: key.String(), uploadID: uploadId.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	return m.run(ctx, auditlog.OpAbortMultipartUpload, auditResource{bucket: bucketName.String(), key: key.String(), uploadID: uploadId.String()}, func(ctx context.Context) error {
		return m.Next.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	})
}

func (m *AuditLogMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	var result *storage.ListMultipartUploadsResult
	err := m.run(ctx, auditlog.OpListMultipartUploads, auditResource{bucket: bucketName.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.ListMultipartUploads(ctx, bucketName, opts)
		return err
	})
	return result, err
}

func (m *AuditLogMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	var result *storage.ListPartsResult
	err := m.run(ctx, auditlog.OpListParts, auditResource{bucket: bucketName.String(), key: key.String(), uploadID: uploadId.String()}, func(ctx context.Context) error {
		var err error
		result, err = m.Next.ListParts(ctx, bucketName, key, uploadId, opts)
		return err
	})
	return result, err
}

func statusCodeFromError(err error) int32 {
	if errors.Is(err, storage.ErrObjectLockAccessDenied) {
		return 403
	}
	if errors.Is(err, storage.ErrInvalidObjectLockConfiguration) {
		return 400
	}
	if errors.Is(err, storage.ErrObjectLockConfigurationNotFound) {
		return 404
	}
	if err != nil {
		return 500
	}
	return 200
}

// Ensure interface compliance
var _ storage.Storage = (*AuditLogMiddleware)(nil)
var _ lifecycle.Manager = (*AuditLogMiddleware)(nil)
