package lifecyclereconciler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

// fakeStorage is an in-memory backend recording the deletions and aborts the
// reconciler performs. The mutex makes it safe to inspect the state while the
// background reconcile task is running.
type fakeStorage struct {
	delegator.DelegatingStorage
	mu              sync.Mutex
	buckets         []storage.Bucket
	lifecycleConfig map[string]*storage.BucketLifecycleConfiguration
	objects         map[string][]storage.Object
	versions        map[string][]storage.ObjectVersion
	versionTags     map[string]map[string]string
	uploads         map[string][]storage.Upload
	deletedKeys     []string
	deletedVersions []string
	abortedUploads  []string
	transitions     []transitionCall
}

type transitionCall struct {
	key          string
	storageClass string
	ifMatchETag  *string
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		DelegatingStorage: delegator.Wrap(nil),
		lifecycleConfig:   map[string]*storage.BucketLifecycleConfiguration{},
		objects:           map[string][]storage.Object{},
		versions:          map[string][]storage.ObjectVersion{},
		versionTags:       map[string]map[string]string{},
		uploads:           map[string][]storage.Upload{},
	}
}

func (f *fakeStorage) Start(_ context.Context) error { return nil }

func (f *fakeStorage) Stop(_ context.Context) error { return nil }

func (f *fakeStorage) ListBuckets(_ context.Context) ([]storage.Bucket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buckets, nil
}

func (f *fakeStorage) GetBucketLifecycleConfiguration(_ context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	config, ok := f.lifecycleConfig[bucketName.String()]
	if !ok {
		return nil, storage.ErrNoSuchLifecycleConfiguration
	}
	return config, nil
}

func (f *fakeStorage) ListObjects(_ context.Context, bucketName storage.BucketName, _ storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &storage.ListBucketResult{Objects: f.objects[bucketName.String()]}, nil
}

func (f *fakeStorage) ListObjectVersions(_ context.Context, bucketName storage.BucketName, _ storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &storage.ListObjectVersionsResult{Versions: f.versions[bucketName.String()]}, nil
}

func (f *fakeStorage) GetObjectTagging(_ context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if opts != nil && opts.VersionID != nil {
		tags, ok := f.versionTags[key.String()+"\x00"+*opts.VersionID]
		if !ok {
			return map[string]string{}, nil
		}
		return tags, nil
	}
	for _, object := range f.objects[bucketName.String()] {
		if object.Key.Equals(key) {
			return object.Tags, nil
		}
	}
	return nil, storage.ErrNoSuchKey
}

func (f *fakeStorage) DeleteObject(_ context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if opts != nil && opts.VersionID != nil {
		f.deletedVersions = append(f.deletedVersions, key.String()+"\x00"+*opts.VersionID)
		versions := f.versions[bucketName.String()]
		remainingVersions := make([]storage.ObjectVersion, 0, len(versions))
		for _, version := range versions {
			if !version.Key.Equals(key) || version.VersionID != *opts.VersionID {
				remainingVersions = append(remainingVersions, version)
			}
		}
		f.versions[bucketName.String()] = remainingVersions
		return &storage.DeleteObjectResult{VersionID: opts.VersionID}, nil
	}
	f.deletedKeys = append(f.deletedKeys, key.String())
	objects := f.objects[bucketName.String()]
	remaining := make([]storage.Object, 0, len(objects))
	for _, object := range objects {
		if !object.Key.Equals(key) {
			remaining = append(remaining, object)
		}
	}
	f.objects[bucketName.String()] = remaining
	return &storage.DeleteObjectResult{}, nil
}

func (f *fakeStorage) ListMultipartUploads(_ context.Context, bucketName storage.BucketName, _ storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &storage.ListMultipartUploadsResult{Uploads: f.uploads[bucketName.String()]}, nil
}

func (f *fakeStorage) AbortMultipartUpload(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, uploadId storage.UploadId) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abortedUploads = append(f.abortedUploads, uploadId.String())
	return nil
}

func (f *fakeStorage) TransitionObjectStorageClass(_ context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ifMatchETag *string
	if opts != nil {
		ifMatchETag = opts.IfMatchETag
	}
	f.transitions = append(f.transitions, transitionCall{key: key.String(), storageClass: targetStorageClass, ifMatchETag: ifMatchETag})
	for i := range f.objects[bucketName.String()] {
		if f.objects[bucketName.String()][i].Key.Equals(key) {
			f.objects[bucketName.String()][i].StorageClass = &targetStorageClass
		}
	}
	return nil
}

func (f *fakeStorage) objectCount(bucket string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects[bucket])
}

func (f *fakeStorage) addBucket(name string) storage.BucketName {
	bucketName := storage.MustNewBucketName(name)
	f.buckets = append(f.buckets, storage.Bucket{Name: bucketName, CreationDate: time.Now()})
	return bucketName
}

func (f *fakeStorage) addObject(bucket string, key string, size int64, lastModified time.Time, tags map[string]string) {
	f.objects[bucket] = append(f.objects[bucket], storage.Object{
		Key:          storage.MustNewObjectKey(key),
		LastModified: lastModified,
		ETag:         "etag-" + key,
		Size:         size,
		Tags:         tags,
	})
}

func (f *fakeStorage) addVersion(bucket string, key string, versionID string, isLatest bool, isDeleteMarker bool, size int64, lastModified time.Time, tags map[string]string) {
	f.versions[bucket] = append(f.versions[bucket], storage.ObjectVersion{
		Key:            storage.MustNewObjectKey(key),
		VersionID:      versionID,
		IsLatest:       isLatest,
		IsDeleteMarker: isDeleteMarker,
		LastModified:   lastModified,
		Size:           size,
		ETag:           ptrutils.ToPtr("etag-" + versionID),
	})
	if tags != nil {
		f.versionTags[key+"\x00"+versionID] = tags
	}
}

func reconcile(f *fakeStorage, now time.Time) {
	mw := NewStorageMiddleware(f, WithNow(func() time.Time { return now })).(*lifecycleReconcilerStorageMiddleware)
	mw.ReconcileOnce(context.Background(), nil)
}

func enabledExpirationRule(prefix string, days int32) storage.LifecycleRule {
	return storage.LifecycleRule{
		Status:     storage.LifecycleRuleStatusEnabled,
		Filter:     &storage.LifecycleFilter{Prefix: ptrutils.ToPtr(prefix)},
		Expiration: &storage.LifecycleExpiration{Days: ptrutils.ToPtr(days)},
	}
}

func TestReconcileExpiresDueObjectsMatchingPrefix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -10)
	f.addObject(bucket.String(), "logs/old.log", 10, old, nil)
	f.addObject(bucket.String(), "logs/new.log", 10, now.Add(-time.Hour), nil)
	f.addObject(bucket.String(), "data/old.bin", 10, old, nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledExpirationRule("logs/", 3)},
	}

	reconcile(f, now)

	assert.Equal(t, []string{"logs/old.log"}, f.deletedKeys)
}

func TestReconcileSkipsDisabledRules(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.addObject(bucket.String(), "logs/old.log", 10, now.AddDate(0, 0, -10), nil)
	rule := enabledExpirationRule("logs/", 3)
	rule.Status = storage.LifecycleRuleStatusDisabled
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{rule},
	}

	reconcile(f, now)

	assert.Empty(t, f.deletedKeys)
}

func TestReconcileHonorsDaysRoundingToNextMidnight(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	created := time.Date(2026, 6, 28, 10, 30, 0, 0, time.UTC)
	f.addObject(bucket.String(), "logs/a.log", 10, created, nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledExpirationRule("logs/", 3)},
	}

	// Due at 2026-07-02T00:00Z; one second earlier nothing may be deleted.
	reconcile(f, time.Date(2026, 7, 1, 23, 59, 59, 0, time.UTC))
	assert.Empty(t, f.deletedKeys)

	reconcile(f, time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, []string{"logs/a.log"}, f.deletedKeys)
}

func TestReconcileExpiresObjectsByDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.addObject(bucket.String(), "a", 10, now.Add(-time.Minute), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{{
			Status: storage.LifecycleRuleStatusEnabled,
			Filter: &storage.LifecycleFilter{},
			Expiration: &storage.LifecycleExpiration{
				Date: ptrutils.ToPtr(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)),
			},
		}},
	}

	reconcile(f, now)

	assert.Equal(t, []string{"a"}, f.deletedKeys)
}

func TestReconcileRespectsSizeAndTagFilters(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -10)
	f.addObject(bucket.String(), "big-dev", 500, old, map[string]string{"env": "dev"})
	f.addObject(bucket.String(), "big-prod", 500, old, map[string]string{"env": "prod"})
	f.addObject(bucket.String(), "small-dev", 5, old, map[string]string{"env": "dev"})
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{{
			Status: storage.LifecycleRuleStatusEnabled,
			Filter: &storage.LifecycleFilter{And: &storage.LifecycleFilterAnd{
				Tags:                  []storage.LifecycleTag{{Key: "env", Value: "dev"}},
				ObjectSizeGreaterThan: ptrutils.ToPtr(int64(100)),
			}},
			Expiration: &storage.LifecycleExpiration{Days: ptrutils.ToPtr(int32(3))},
		}},
	}

	reconcile(f, now)

	assert.Equal(t, []string{"big-dev"}, f.deletedKeys)
}

func TestReconcileAbortsStaleMultipartUploads(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	staleUploadId := storage.MustNewUploadId("019817f2-0000-7000-8000-000000000001")
	freshUploadId := storage.MustNewUploadId("019817f2-0000-7000-8000-000000000002")
	otherPrefixUploadId := storage.MustNewUploadId("019817f2-0000-7000-8000-000000000003")
	f.uploads[bucket.String()] = []storage.Upload{
		{Key: storage.MustNewObjectKey("uploads/stale"), UploadId: staleUploadId, Initiated: now.AddDate(0, 0, -10)},
		{Key: storage.MustNewObjectKey("uploads/fresh"), UploadId: freshUploadId, Initiated: now.Add(-time.Hour)},
		{Key: storage.MustNewObjectKey("other/stale"), UploadId: otherPrefixUploadId, Initiated: now.AddDate(0, 0, -10)},
	}
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{{
			Status:                         storage.LifecycleRuleStatusEnabled,
			Filter:                         &storage.LifecycleFilter{Prefix: ptrutils.ToPtr("uploads/")},
			AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(7))},
		}},
	}

	reconcile(f, now)

	assert.Equal(t, []string{staleUploadId.String()}, f.abortedUploads)
	assert.Empty(t, f.deletedKeys)
}

func TestReconcileIgnoresBucketsWithoutLifecycleConfiguration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	f.addObject(bucket.String(), "a", 10, time.Now().AddDate(0, 0, -100), nil)

	reconcile(f, time.Now())

	assert.Empty(t, f.deletedKeys)
}

func TestStartAndStopRunTheBackgroundTask(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Now().UTC()
	f.addObject(bucket.String(), "logs/old.log", 10, now.AddDate(0, 0, -10), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledExpirationRule("logs/", 3)},
	}

	mw := NewStorageMiddleware(f, WithReconcileInterval(time.Hour))
	require.NoError(t, mw.Start(context.Background()))
	defer func() {
		require.NoError(t, mw.Stop(context.Background()))
	}()

	// The initial sweep runs asynchronously right after Start.
	require.Eventually(t, func() bool {
		return f.objectCount(bucket.String()) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func enabledTransitionRule(prefix string, days int32, storageClass string) storage.LifecycleRule {
	return storage.LifecycleRule{
		Status: storage.LifecycleRuleStatusEnabled,
		Filter: &storage.LifecycleFilter{Prefix: ptrutils.ToPtr(prefix)},
		Transitions: []storage.LifecycleTransition{
			{Days: ptrutils.ToPtr(days), StorageClass: storageClass},
		},
	}
}

func TestReconcileTransitionsDueObjects(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -10)
	f.addObject(bucket.String(), "cold/old.bin", 10, old, nil)
	f.addObject(bucket.String(), "cold/new.bin", 10, now.Add(-time.Hour), nil)
	f.addObject(bucket.String(), "hot/old.bin", 10, old, nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledTransitionRule("cold/", 3, "GLACIER")},
	}

	reconcile(f, now)

	require.Len(t, f.transitions, 1)
	assert.Equal(t, "cold/old.bin", f.transitions[0].key)
	assert.Equal(t, "GLACIER", f.transitions[0].storageClass)
	require.NotNil(t, f.transitions[0].ifMatchETag)
	assert.Equal(t, "etag-cold/old.bin", *f.transitions[0].ifMatchETag)
}

func TestReconcileSkipsTransitionWhenAlreadyInTargetClass(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.objects[bucket.String()] = append(f.objects[bucket.String()], storage.Object{
		Key:          storage.MustNewObjectKey("cold/a.bin"),
		LastModified: now.AddDate(0, 0, -10),
		ETag:         "etag-cold/a.bin",
		Size:         10,
		StorageClass: ptrutils.ToPtr("GLACIER"),
	})
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledTransitionRule("cold/", 3, "GLACIER")},
	}

	reconcile(f, now)

	assert.Empty(t, f.transitions)
}

func TestReconcileExpirationTakesPrecedenceOverTransition(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.addObject(bucket.String(), "data/a.bin", 10, now.AddDate(0, 0, -10), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{
			{
				Status:      storage.LifecycleRuleStatusEnabled,
				Filter:      &storage.LifecycleFilter{Prefix: ptrutils.ToPtr("data/")},
				Transitions: []storage.LifecycleTransition{{Days: ptrutils.ToPtr(int32(3)), StorageClass: "GLACIER"}},
				Expiration:  &storage.LifecycleExpiration{Days: ptrutils.ToPtr(int32(5))},
			},
		},
	}

	reconcile(f, now)

	// The object is due for both; expiration runs first and deletes it, so the
	// transition never sees it.
	assert.Equal(t, []string{"data/a.bin"}, f.deletedKeys)
	assert.Empty(t, f.transitions)
}

func TestReconcilePicksMostAdvancedDueTransition(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.addObject(bucket.String(), "cold/a.bin", 10, now.AddDate(0, 0, -100), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{
			enabledTransitionRule("cold/", 30, "STANDARD_IA"),
			enabledTransitionRule("cold/", 90, "GLACIER"),
		},
	}

	reconcile(f, now)

	// Both transitions are due; the later one (GLACIER) wins.
	require.Len(t, f.transitions, 1)
	assert.Equal(t, "GLACIER", f.transitions[0].storageClass)
}

func TestReconcileDoesNotTransitionNotYetDueObjects(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	f.addObject(bucket.String(), "cold/a.bin", 10, now.Add(-time.Hour), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{enabledTransitionRule("cold/", 30, "GLACIER")},
	}

	reconcile(f, now)

	assert.Empty(t, f.transitions)
}

func TestReconcileExpiresEligibleNoncurrentVersions(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	f.addVersion(bucket.String(), "logs/a", "v4", true, false, 10, now.Add(-time.Hour), nil)
	f.addVersion(bucket.String(), "logs/a", "v3", false, false, 10, now.AddDate(0, 0, -10), nil)
	f.addVersion(bucket.String(), "logs/a", "v2", false, false, 10, now.AddDate(0, 0, -20), nil)
	f.addVersion(bucket.String(), "logs/a", "v1", false, false, 10, now.AddDate(0, 0, -30), nil)
	f.addVersion(bucket.String(), "logs/a", "dm", false, true, 0, now.AddDate(0, 0, -40), nil)
	f.addVersion(bucket.String(), "data/a", "data-v1", false, false, 10, now.AddDate(0, 0, -30), nil)
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{{
			Status: storage.LifecycleRuleStatusEnabled,
			Filter: &storage.LifecycleFilter{Prefix: ptrutils.ToPtr("logs/")},
			NoncurrentVersionExpiration: &storage.LifecycleNoncurrentVersionExpiration{
				NoncurrentDays:          ptrutils.ToPtr(int32(3)),
				NewerNoncurrentVersions: ptrutils.ToPtr(int32(1)),
			},
		}},
	}

	reconcile(f, now)

	assert.Equal(t, []string{"logs/a\x00v1"}, f.deletedVersions)
}

func TestReconcileNoncurrentVersionsHonorsTagAndSizeFilters(t *testing.T) {
	testutils.SkipIfIntegration(t)

	f := newFakeStorage()
	bucket := f.addBucket("test-bucket")
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	f.addVersion(bucket.String(), "logs/a", "latest", true, false, 10, now, nil)
	f.addVersion(bucket.String(), "logs/a", "match", false, false, 150, now.AddDate(0, 0, -30), map[string]string{"env": "prod"})
	f.addVersion(bucket.String(), "logs/a", "wrong-tag", false, false, 150, now.AddDate(0, 0, -31), map[string]string{"env": "dev"})
	f.addVersion(bucket.String(), "logs/a", "too-small", false, false, 50, now.AddDate(0, 0, -32), map[string]string{"env": "prod"})
	f.lifecycleConfig[bucket.String()] = &storage.BucketLifecycleConfiguration{
		Rules: []storage.LifecycleRule{{
			Status: storage.LifecycleRuleStatusEnabled,
			Filter: &storage.LifecycleFilter{And: &storage.LifecycleFilterAnd{
				Prefix:                ptrutils.ToPtr("logs/"),
				Tags:                  []storage.LifecycleTag{{Key: "env", Value: "prod"}},
				ObjectSizeGreaterThan: ptrutils.ToPtr(int64(100)),
			}},
			NoncurrentVersionExpiration: &storage.LifecycleNoncurrentVersionExpiration{
				NoncurrentDays: ptrutils.ToPtr(int32(3)),
			},
		}},
	}

	reconcile(f, now)

	assert.Equal(t, []string{"logs/a\x00match"}, f.deletedVersions)
}
