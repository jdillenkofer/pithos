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
	"sort"
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
	expiredObjectDeleteMarkerRules := []*storage.LifecycleRule{}
	noncurrentExpirationRules := []*storage.LifecycleRule{}
	noncurrentTransitionRules := []*storage.LifecycleRule{}
	abortRules := []*storage.LifecycleRule{}
	transitionRules := []*storage.LifecycleRule{}
	for i := range config.Rules {
		rule := &config.Rules[i]
		if rule.Status != storage.LifecycleRuleStatusEnabled {
			continue
		}
		if rule.Expiration != nil && (rule.Expiration.Days != nil || rule.Expiration.Date != nil) {
			expirationRules = append(expirationRules, rule)
		}
		if rule.Expiration != nil && rule.Expiration.ExpiredObjectDeleteMarker != nil && *rule.Expiration.ExpiredObjectDeleteMarker {
			expiredObjectDeleteMarkerRules = append(expiredObjectDeleteMarkerRules, rule)
		}
		if rule.NoncurrentVersionExpiration != nil {
			noncurrentExpirationRules = append(noncurrentExpirationRules, rule)
		}
		if len(rule.NoncurrentVersionTransitions) > 0 {
			noncurrentTransitionRules = append(noncurrentTransitionRules, rule)
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			abortRules = append(abortRules, rule)
		}
		if len(rule.Transitions) > 0 {
			transitionRules = append(transitionRules, rule)
		}
	}

	// Expiration takes precedence over transitions: an object that is both due
	// for expiration and transition is deleted, so run expiration first.
	if len(expirationRules) > 0 {
		m.expireObjects(ctx, bucketName, expirationRules, cancelTask)
	}
	if len(expiredObjectDeleteMarkerRules) > 0 {
		m.expireObjectDeleteMarkers(ctx, bucketName, expiredObjectDeleteMarkerRules, cancelTask)
	}
	if len(noncurrentExpirationRules) > 0 {
		m.expireNoncurrentObjectVersions(ctx, bucketName, noncurrentExpirationRules, cancelTask)
	}
	if len(noncurrentTransitionRules) > 0 {
		m.transitionNoncurrentObjectVersions(ctx, bucketName, noncurrentTransitionRules, cancelTask)
	}
	if len(transitionRules) > 0 {
		m.transitionObjects(ctx, bucketName, transitionRules, cancelTask)
	}
	if len(abortRules) > 0 {
		m.abortIncompleteUploads(ctx, bucketName, abortRules, cancelTask)
	}
}

type deleteMarkerExpirationCandidate struct {
	currentDeleteMarker *storage.ObjectVersion
	hasObjectVersion    bool
}

func (m *lifecycleReconcilerStorageMiddleware) expireObjectDeleteMarkers(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
	candidatesByKey := map[string]*deleteMarkerExpirationCandidate{}
	var keyMarker, versionIDMarker *string
	for {
		if isCancelled(cancelTask) {
			return
		}
		listResult, err := m.Next.ListObjectVersions(ctx, bucketName, storage.ListObjectVersionsOptions{
			KeyMarker:       keyMarker,
			VersionIDMarker: versionIDMarker,
			MaxKeys:         listPageSize,
		})
		if err != nil {
			slog.Warn("lifecycle reconciler failed to list object versions", "bucket", bucketName.String(), "err", err)
			return
		}
		for i := range listResult.Versions {
			version := &listResult.Versions[i]
			key := version.Key.String()
			candidate := candidatesByKey[key]
			if candidate == nil {
				candidate = &deleteMarkerExpirationCandidate{}
				candidatesByKey[key] = candidate
			}
			if version.IsLatest && version.IsDeleteMarker {
				versionCopy := *version
				candidate.currentDeleteMarker = &versionCopy
			}
			if !version.IsDeleteMarker {
				candidate.hasObjectVersion = true
			}
		}
		if !listResult.IsTruncated {
			break
		}
		keyMarker = listResult.NextKeyMarker
		versionIDMarker = listResult.NextVersionIDMarker
		if keyMarker == nil {
			break
		}
	}

	for _, candidate := range candidatesByKey {
		if isCancelled(cancelTask) {
			return
		}
		if candidate.currentDeleteMarker == nil || candidate.hasObjectVersion {
			continue
		}
		m.expireObjectDeleteMarkerIfDue(ctx, bucketName, candidate.currentDeleteMarker, rules)
	}
}

func (m *lifecycleReconcilerStorageMiddleware) expireObjectDeleteMarkerIfDue(ctx context.Context, bucketName storage.BucketName, deleteMarker *storage.ObjectVersion, rules []*storage.LifecycleRule) {
	for _, rule := range rules {
		if !storage.LifecycleRuleMatchesObject(rule, deleteMarker.Key.String(), deleteMarker.Size, nil) {
			continue
		}
		_, err := m.Next.DeleteObject(storage.WithNotificationEventOverride(ctx, "s3:LifecycleExpiration:Delete"), bucketName, deleteMarker.Key, &storage.DeleteObjectOptions{VersionID: &deleteMarker.VersionID})
		if err == storage.ErrNoSuchKey || err == storage.ErrNoSuchBucket {
			return
		}
		if err != nil {
			slog.Warn("lifecycle reconciler failed to expire object delete marker", "bucket", bucketName.String(), "key", deleteMarker.Key.String(), "versionId", deleteMarker.VersionID, "err", err)
			return
		}
		slog.Info("lifecycle reconciler expired object delete marker", "bucket", bucketName.String(), "key", deleteMarker.Key.String(), "versionId", deleteMarker.VersionID)
		return
	}
}

func (m *lifecycleReconcilerStorageMiddleware) expireNoncurrentObjectVersions(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
	versionsByKey := map[string][]storage.ObjectVersion{}
	var keyMarker, versionIDMarker *string
	for {
		if isCancelled(cancelTask) {
			return
		}
		listResult, err := m.Next.ListObjectVersions(ctx, bucketName, storage.ListObjectVersionsOptions{
			KeyMarker:       keyMarker,
			VersionIDMarker: versionIDMarker,
			MaxKeys:         listPageSize,
		})
		if err != nil {
			slog.Warn("lifecycle reconciler failed to list object versions", "bucket", bucketName.String(), "err", err)
			return
		}
		for _, version := range listResult.Versions {
			versionsByKey[version.Key.String()] = append(versionsByKey[version.Key.String()], version)
		}
		if !listResult.IsTruncated {
			break
		}
		keyMarker = listResult.NextKeyMarker
		versionIDMarker = listResult.NextVersionIDMarker
		if keyMarker == nil {
			break
		}
	}

	for _, versions := range versionsByKey {
		if isCancelled(cancelTask) {
			return
		}
		sort.SliceStable(versions, func(i, j int) bool {
			return versions[i].LastModified.After(versions[j].LastModified)
		})
		newerNoncurrentVersions := 0
		for i := range versions {
			if isCancelled(cancelTask) {
				return
			}
			version := &versions[i]
			if version.IsLatest || version.IsDeleteMarker {
				continue
			}
			if i == 0 {
				continue
			}
			noncurrentSince := versions[i-1].LastModified
			m.expireNoncurrentObjectVersionIfDue(ctx, bucketName, version, noncurrentSince, newerNoncurrentVersions, rules)
			newerNoncurrentVersions++
		}
	}
}

func (m *lifecycleReconcilerStorageMiddleware) transitionNoncurrentObjectVersions(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
	versionsByKey := map[string][]storage.ObjectVersion{}
	var keyMarker, versionIDMarker *string
	for {
		if isCancelled(cancelTask) {
			return
		}
		listResult, err := m.Next.ListObjectVersions(ctx, bucketName, storage.ListObjectVersionsOptions{
			KeyMarker:       keyMarker,
			VersionIDMarker: versionIDMarker,
			MaxKeys:         listPageSize,
		})
		if err != nil {
			slog.Warn("lifecycle reconciler failed to list object versions", "bucket", bucketName.String(), "err", err)
			return
		}
		for _, version := range listResult.Versions {
			versionsByKey[version.Key.String()] = append(versionsByKey[version.Key.String()], version)
		}
		if !listResult.IsTruncated {
			break
		}
		keyMarker = listResult.NextKeyMarker
		versionIDMarker = listResult.NextVersionIDMarker
		if keyMarker == nil {
			break
		}
	}

	for _, versions := range versionsByKey {
		if isCancelled(cancelTask) {
			return
		}
		sort.SliceStable(versions, func(i, j int) bool {
			return versions[i].LastModified.After(versions[j].LastModified)
		})
		newerNoncurrentVersions := 0
		for i := range versions {
			if isCancelled(cancelTask) {
				return
			}
			version := &versions[i]
			if version.IsLatest || version.IsDeleteMarker {
				continue
			}
			if i == 0 {
				continue
			}
			noncurrentSince := versions[i-1].LastModified
			m.transitionNoncurrentObjectVersionIfDue(ctx, bucketName, version, noncurrentSince, newerNoncurrentVersions, rules)
			newerNoncurrentVersions++
		}
	}
}

func (m *lifecycleReconcilerStorageMiddleware) transitionNoncurrentObjectVersionIfDue(ctx context.Context, bucketName storage.BucketName, version *storage.ObjectVersion, noncurrentSince time.Time, newerNoncurrentVersions int, rules []*storage.LifecycleRule) {
	now := m.now()
	var tags map[string]string
	tagsFetched := false

	currentClass := storage.EffectiveStorageClass(version.StorageClass)
	var chosenTarget string
	var chosenDue *time.Time
	for _, rule := range rules {
		matches := func() bool {
			if storage.LifecycleRuleNeedsObjectTags(rule) && !tagsFetched {
				fetchedTags, err := m.Next.GetObjectTagging(ctx, bucketName, version.Key, &storage.ObjectTaggingOptions{VersionID: &version.VersionID})
				if err != nil {
					slog.Warn("lifecycle reconciler failed to fetch object version tags", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID, "err", err)
					return false
				}
				tags = fetchedTags
				tagsFetched = true
			}
			return storage.LifecycleRuleMatchesObject(rule, version.Key.String(), version.Size, tags)
		}
		var ruleMatches *bool
		for i := range rule.NoncurrentVersionTransitions {
			transition := &rule.NoncurrentVersionTransitions[i]
			dueTime := storage.LifecycleNoncurrentTransitionDueTime(transition, noncurrentSince)
			if dueTime == nil || now.Before(*dueTime) {
				continue
			}
			if transition.NewerNoncurrentVersions != nil && newerNoncurrentVersions <= int(*transition.NewerNoncurrentVersions) {
				continue
			}
			if transition.StorageClass == currentClass {
				continue
			}
			if ruleMatches == nil {
				m := matches()
				ruleMatches = &m
			}
			if !*ruleMatches {
				break
			}
			if chosenDue == nil || dueTime.After(*chosenDue) {
				chosenDue = dueTime
				chosenTarget = transition.StorageClass
			}
		}
	}
	if chosenDue == nil {
		return
	}

	err := m.Next.TransitionObjectStorageClass(storage.WithNotificationEventOverride(ctx, "s3:LifecycleTransition"), bucketName, version.Key, chosenTarget, &storage.TransitionObjectStorageClassOptions{
		VersionID:   &version.VersionID,
		IfMatchETag: version.ETag,
	})
	if err == storage.ErrPreconditionFailed || err == storage.ErrNoSuchKey || err == storage.ErrNoSuchBucket || err == storage.ErrNotImplemented {
		return
	}
	if err != nil {
		slog.Warn("lifecycle reconciler failed to transition noncurrent object version", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID, "err", err)
		return
	}
	slog.Info("lifecycle reconciler transitioned noncurrent object version", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID, "storageClass", chosenTarget)
}

func (m *lifecycleReconcilerStorageMiddleware) expireNoncurrentObjectVersionIfDue(ctx context.Context, bucketName storage.BucketName, version *storage.ObjectVersion, noncurrentSince time.Time, newerNoncurrentVersions int, rules []*storage.LifecycleRule) {
	now := m.now()
	var tags map[string]string
	tagsFetched := false
	for _, rule := range rules {
		expiration := rule.NoncurrentVersionExpiration
		if expiration == nil {
			continue
		}
		dueTime := storage.LifecycleNoncurrentExpirationDueTime(rule, noncurrentSince)
		if dueTime == nil || now.Before(*dueTime) {
			continue
		}
		if expiration.NewerNoncurrentVersions != nil && newerNoncurrentVersions <= int(*expiration.NewerNoncurrentVersions) {
			continue
		}
		if storage.LifecycleRuleNeedsObjectTags(rule) && !tagsFetched {
			fetchedTags, err := m.Next.GetObjectTagging(ctx, bucketName, version.Key, &storage.ObjectTaggingOptions{VersionID: &version.VersionID})
			if err != nil {
				slog.Warn("lifecycle reconciler failed to fetch object version tags", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID, "err", err)
				continue
			}
			tags = fetchedTags
			tagsFetched = true
		}
		if !storage.LifecycleRuleMatchesObject(rule, version.Key.String(), version.Size, tags) {
			continue
		}
		_, err := m.Next.DeleteObject(storage.WithNotificationEventOverride(ctx, "s3:LifecycleExpiration:Delete"), bucketName, version.Key, &storage.DeleteObjectOptions{VersionID: &version.VersionID})
		if err == storage.ErrNoSuchKey || err == storage.ErrNoSuchBucket {
			return
		}
		if err != nil {
			slog.Warn("lifecycle reconciler failed to expire noncurrent object version", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID, "err", err)
			return
		}
		slog.Info("lifecycle reconciler expired noncurrent object version", "bucket", bucketName.String(), "key", version.Key.String(), "versionId", version.VersionID)
		return
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
			fetchedTags, err := m.Next.GetObjectTagging(ctx, bucketName, object.Key, nil)
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
		_, err := m.Next.DeleteObject(storage.WithNotificationEventOverride(ctx, "s3:LifecycleExpiration:Delete"), bucketName, object.Key, &storage.DeleteObjectOptions{
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

func (m *lifecycleReconcilerStorageMiddleware) transitionObjects(ctx context.Context, bucketName storage.BucketName, rules []*storage.LifecycleRule, cancelTask *atomic.Bool) {
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
			m.transitionObjectIfDue(ctx, bucketName, &listResult.Objects[i], rules)
		}
		if !listResult.IsTruncated || len(listResult.Objects) == 0 {
			return
		}
		startAfter = ptrutils.ToPtr(listResult.Objects[len(listResult.Objects)-1].Key.String())
	}
}

func (m *lifecycleReconcilerStorageMiddleware) transitionObjectIfDue(ctx context.Context, bucketName storage.BucketName, object *storage.Object, rules []*storage.LifecycleRule) {
	now := m.now()
	tags := object.Tags
	tagsFetched := len(tags) > 0

	// Among all due transitions matching this object, pick the one with the
	// latest due time (the most advanced tier the object currently qualifies
	// for). A transition to the object's current class is a no-op and skipped.
	currentClass := storage.EffectiveStorageClass(object.StorageClass)
	var chosenTarget string
	var chosenDue *time.Time
	for _, rule := range rules {
		matches := func() bool {
			if storage.LifecycleRuleNeedsObjectTags(rule) && !tagsFetched {
				fetchedTags, err := m.Next.GetObjectTagging(ctx, bucketName, object.Key, nil)
				if err != nil {
					slog.Warn("lifecycle reconciler failed to fetch object tags", "bucket", bucketName.String(), "key", object.Key.String(), "err", err)
					return false
				}
				tags = fetchedTags
				tagsFetched = true
			}
			return storage.LifecycleRuleMatchesObject(rule, object.Key.String(), object.Size, tags)
		}
		var ruleMatches *bool
		for i := range rule.Transitions {
			transition := &rule.Transitions[i]
			dueTime := storage.LifecycleTransitionDueTime(transition, object.LastModified)
			if dueTime == nil || now.Before(*dueTime) {
				continue
			}
			if transition.StorageClass == currentClass {
				continue
			}
			// Evaluate the filter lazily, at most once per rule, only when a
			// transition of the rule is otherwise due.
			if ruleMatches == nil {
				m := matches()
				ruleMatches = &m
			}
			if !*ruleMatches {
				break
			}
			if chosenDue == nil || dueTime.After(*chosenDue) {
				chosenDue = dueTime
				chosenTarget = transition.StorageClass
			}
		}
	}
	if chosenDue == nil {
		return
	}

	// Guard against the object having been replaced between listing and
	// transition: only transition the exact version that was evaluated.
	err := m.Next.TransitionObjectStorageClass(storage.WithNotificationEventOverride(ctx, "s3:LifecycleTransition"), bucketName, object.Key, chosenTarget, &storage.TransitionObjectStorageClassOptions{
		IfMatchETag: ptrutils.ToPtr(object.ETag),
	})
	if err == storage.ErrPreconditionFailed || err == storage.ErrNoSuchKey || err == storage.ErrNoSuchBucket {
		return
	}
	if err != nil {
		slog.Warn("lifecycle reconciler failed to transition object", "bucket", bucketName.String(), "key", object.Key.String(), "err", err)
		return
	}
	slog.Info("lifecycle reconciler transitioned object", "bucket", bucketName.String(), "key", object.Key.String(), "storageClass", chosenTarget)
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
