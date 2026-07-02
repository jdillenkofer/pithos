// Package lifecyclereconciler provides a storage middleware that periodically
// enforces bucket lifecycle configurations: it deletes objects whose
// Expiration action is due and aborts multipart uploads whose
// AbortIncompleteMultipartUpload action is due.
//
// The sweep is modeled on the erasure-coding heal scan: a background task is
// started with the storage and re-runs on a fixed interval. All enforcement
// goes through the wrapped storage.Storage interface, so deletions flow
// through the middlewares below (audit, replication, metrics, ...) and the
// reconciler works with any storage backend.
package lifecyclereconciler

import (
	"context"
	"database/sql"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/task"
)

// DefaultReconcileInterval is how often lifecycle rules are evaluated. S3
// evaluates lifecycle rules roughly once per day, but because due times are
// rounded to midnight UTC anyway, a shorter interval only makes enforcement
// more prompt without changing semantics.
const DefaultReconcileInterval = 1 * time.Hour

const listPageSize = 1000

type lifecycleReconcilerStorageMiddleware struct {
	delegator.DelegatingStorage
	reconcileEvery time.Duration
	now            func() time.Time
	reconcileTask  *task.TaskHandle
	tracer         trace.Tracer
}

var _ storage.Storage = (*lifecycleReconcilerStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*lifecycleReconcilerStorageMiddleware)(nil)

type Option func(*lifecycleReconcilerStorageMiddleware)

// WithReconcileInterval overrides how often the reconciler sweeps all buckets.
func WithReconcileInterval(interval time.Duration) Option {
	return func(m *lifecycleReconcilerStorageMiddleware) {
		m.reconcileEvery = interval
	}
}

// WithNow overrides the clock used to decide whether an action is due. It is
// intended for tests.
func WithNow(now func() time.Time) Option {
	return func(m *lifecycleReconcilerStorageMiddleware) {
		m.now = now
	}
}

func NewStorageMiddleware(innerStorage storage.Storage, opts ...Option) storage.Storage {
	m := &lifecycleReconcilerStorageMiddleware{
		DelegatingStorage: delegator.Wrap(innerStorage),
		reconcileEvery:    DefaultReconcileInterval,
		now:               time.Now,
		tracer:            otel.Tracer("internal/storage/middlewares/lifecyclereconciler"),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *lifecycleReconcilerStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

func (m *lifecycleReconcilerStorageMiddleware) Start(ctx context.Context) error {
	if err := m.Next.Start(ctx); err != nil {
		return err
	}
	if m.reconcileEvery > 0 {
		m.reconcileTask = task.Start(func(cancelTask *atomic.Bool) {
			m.reconcileLoop(cancelTask)
		})
	}
	return nil
}

func (m *lifecycleReconcilerStorageMiddleware) Stop(ctx context.Context) error {
	if m.reconcileTask != nil {
		m.reconcileTask.Cancel()
		m.reconcileTask.Join()
		m.reconcileTask = nil
	}
	return m.Next.Stop(ctx)
}

func (m *lifecycleReconcilerStorageMiddleware) reconcileLoop(cancelTask *atomic.Bool) {
	ctx := context.Background()
	m.ReconcileOnce(ctx, cancelTask)
	ticker := time.NewTicker(m.reconcileEvery)
	defer ticker.Stop()
	for {
		if cancelTask.Load() {
			return
		}
		select {
		case <-ticker.C:
			m.ReconcileOnce(ctx, cancelTask)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ReconcileOnce runs a single lifecycle sweep over all buckets. cancelTask may
// be nil when cancellation is not needed.
func (m *lifecycleReconcilerStorageMiddleware) ReconcileOnce(ctx context.Context, cancelTask *atomic.Bool) {
	ctx, span := m.tracer.Start(ctx, "LifecycleReconcilerStorageMiddleware.ReconcileOnce")
	defer span.End()

	buckets, err := m.Next.ListBuckets(ctx)
	if err != nil {
		slog.Warn("lifecycle reconciler failed to list buckets", "err", err)
		return
	}
	for _, bucket := range buckets {
		if isCancelled(cancelTask) {
			return
		}
		config, err := m.Next.GetBucketLifecycleConfiguration(ctx, bucket.Name)
		if err == storage.ErrNoSuchLifecycleConfiguration || err == storage.ErrNoSuchBucket {
			continue
		}
		if err != nil {
			slog.Warn("lifecycle reconciler failed to load lifecycle configuration", "bucket", bucket.Name.String(), "err", err)
			continue
		}
		m.reconcileBucket(ctx, bucket.Name, config, cancelTask)
	}
}

func (m *lifecycleReconcilerStorageMiddleware) reconcileBucket(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration, cancelTask *atomic.Bool) {
	expirationRules := []*storage.LifecycleRule{}
	abortRules := []*storage.LifecycleRule{}
	for i := range config.Rules {
		rule := &config.Rules[i]
		if rule.Status != storage.LifecycleRuleStatusEnabled {
			continue
		}
		if rule.Expiration != nil && (rule.Expiration.Days != nil || rule.Expiration.Date != nil) {
			expirationRules = append(expirationRules, rule)
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			abortRules = append(abortRules, rule)
		}
	}

	if len(expirationRules) > 0 {
		m.expireObjects(ctx, bucketName, expirationRules, cancelTask)
	}
	if len(abortRules) > 0 {
		m.abortIncompleteUploads(ctx, bucketName, abortRules, cancelTask)
	}
}

func (m *lifecycleReconcilerStorageMiddleware) expireObjects(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
	var startAfter *string
	for {
		if isCancelled(cancelTask) {
			return
		}
		listResult, err := m.Next.ListObjects(ctx, bucketName, storage.ListObjectsOptions{
			StartAfter: startAfter,
			MaxKeys:    listPageSize,
		})
		if err != nil {
			slog.Warn("lifecycle reconciler failed to list objects", "bucket", bucketName.String(), "err", err)
			return
		}
		for i := range listResult.Objects {
			if isCancelled(cancelTask) {
				return
			}
			m.expireObjectIfDue(ctx, bucketName, &listResult.Objects[i], rules)
		}
		if !listResult.IsTruncated || len(listResult.Objects) == 0 {
			return
		}
		startAfter = ptrutils.ToPtr(listResult.Objects[len(listResult.Objects)-1].Key.String())
	}
}

func (m *lifecycleReconcilerStorageMiddleware) expireObjectIfDue(ctx context.Context, bucketName storage.BucketName, object *storage.Object, rules []*storage.LifecycleRule) {
	now := m.now()
	tags := object.Tags
	tagsFetched := len(tags) > 0
	for _, rule := range rules {
		dueTime := storage.LifecycleExpirationDueTime(rule, object.LastModified)
		if dueTime == nil || now.Before(*dueTime) {
			continue
		}
		// The listing may not carry the tag set on every backend; fetch it
		// lazily the first time a tag-based rule has to be evaluated.
		if storage.LifecycleRuleNeedsObjectTags(rule) && !tagsFetched {
			fetchedTags, err := m.Next.GetObjectTagging(ctx, bucketName, object.Key)
			if err != nil {
				slog.Warn("lifecycle reconciler failed to fetch object tags", "bucket", bucketName.String(), "key", object.Key.String(), "err", err)
				continue
			}
			tags = fetchedTags
			tagsFetched = true
		}
		if !storage.LifecycleRuleMatchesObject(rule, object.Key.String(), object.Size, tags) {
			continue
		}
		// Guard against the object having been replaced between listing and
		// deletion: only delete the exact version that was evaluated.
		err := m.Next.DeleteObject(ctx, bucketName, object.Key, &storage.DeleteObjectOptions{
			IfMatchETag: ptrutils.ToPtr(object.ETag),
		})
		if err == storage.ErrPreconditionFailed || err == storage.ErrNoSuchKey || err == storage.ErrNoSuchBucket {
			return
		}
		if err != nil {
			slog.Warn("lifecycle reconciler failed to expire object", "bucket", bucketName.String(), "key", object.Key.String(), "err", err)
			return
		}
		slog.Info("lifecycle reconciler expired object", "bucket", bucketName.String(), "key", object.Key.String())
		return
	}
}

func (m *lifecycleReconcilerStorageMiddleware) abortIncompleteUploads(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
	var keyMarker, uploadIdMarker *string
	for {
		if isCancelled(cancelTask) {
			return
		}
		listResult, err := m.Next.ListMultipartUploads(ctx, bucketName, storage.ListMultipartUploadsOptions{
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIdMarker,
			MaxUploads:     listPageSize,
		})
		if err != nil {
			slog.Warn("lifecycle reconciler failed to list multipart uploads", "bucket", bucketName.String(), "err", err)
			return
		}
		for _, upload := range listResult.Uploads {
			if isCancelled(cancelTask) {
				return
			}
			m.abortUploadIfDue(ctx, bucketName, upload, rules)
		}
		if !listResult.IsTruncated {
			return
		}
		keyMarker = ptrutils.ToPtr(listResult.NextKeyMarker)
		uploadIdMarker = ptrutils.ToPtr(listResult.NextUploadIdMarker)
	}
}

func (m *lifecycleReconcilerStorageMiddleware) abortUploadIfDue(ctx context.Context, bucketName storage.BucketName, upload storage.Upload, rules []*storage.LifecycleRule) {
	now := m.now()
	for _, rule := range rules {
		// AbortIncompleteMultipartUpload only supports prefix filtering (tag
		// and size filters are rejected at validation time), so the incomplete
		// upload is matched on its key alone.
		if !storage.LifecycleRuleMatchesObject(rule, upload.Key.String(), 0, nil) {
			continue
		}
		dueTime := storage.LifecycleAbortDueTime(rule, upload.Initiated)
		if dueTime == nil || now.Before(*dueTime) {
			continue
		}
		err := m.Next.AbortMultipartUpload(ctx, bucketName, upload.Key, upload.UploadId)
		if err != nil {
			slog.Warn("lifecycle reconciler failed to abort multipart upload", "bucket", bucketName.String(), "key", upload.Key.String(), "uploadId", upload.UploadId.String(), "err", err)
			return
		}
		slog.Info("lifecycle reconciler aborted incomplete multipart upload", "bucket", bucketName.String(), "key", upload.Key.String(), "uploadId", upload.UploadId.String())
		return
	}
}

func isCancelled(cancelTask *atomic.Bool) bool {
	return cancelTask != nil && cancelTask.Load()
}
