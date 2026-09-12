package metadatapart

import (
	"context"
	"errors"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The backend lifecycles are owned by newTestStorage. Restart only the
// replication coordinator to ensure it reconstructs all state from SQL.
type replicaFaultStorage struct {
	delegator.DelegatingStorage
	blocked           atomic.Bool
	writes            atomic.Int32
	multipartCreates  atomic.Int32
	partUploads       [3]atomic.Int32
	failPart          atomic.Int32
	ambiguousComplete atomic.Bool
}

func (s *replicaFaultStorage) CompleteMultipartUpload(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, checksum *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	result, err := s.Next.CompleteMultipartUpload(ctx, bucket, key, uploadID, checksum, opts)
	if err == nil && s.ambiguousComplete.CompareAndSwap(true, false) {
		return nil, errors.New("connection lost after completing multipart upload")
	}
	return result, err
}

func (s *replicaFaultStorage) CreateMultipartUpload(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	s.multipartCreates.Add(1)
	return s.Next.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType, opts)
}

func (s *replicaFaultStorage) UploadPart(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, partNumber int32, data io.Reader, checksum *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	if partNumber >= 1 && partNumber <= int32(len(s.partUploads)) {
		s.partUploads[partNumber-1].Add(1)
	}
	if s.failPart.CompareAndSwap(partNumber, 0) {
		return nil, errors.New("injected part upload failure")
	}
	return s.Next.UploadPart(ctx, bucket, key, uploadID, partNumber, data, checksum)
}

func (s *replicaFaultStorage) Start(context.Context) error { return nil }
func (s *replicaFaultStorage) Stop(context.Context) error  { return nil }
func (s *replicaFaultStorage) PutObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksum *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	if s.blocked.Load() {
		return nil, errors.New("replica unavailable")
	}
	s.writes.Add(1)
	return s.Next.PutObject(ctx, bucket, key, contentType, data, checksum, opts)
}
func (s *replicaFaultStorage) PutObjectRetention(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	if s.blocked.Load() {
		return errors.New("replica unavailable")
	}
	return s.Next.PutObjectRetention(ctx, bucket, key, retention, opts)
}

func TestReplicationObjectLockRecoveryAndStableIDs(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	first, done1 := newTestStorage(t)
	defer done1()
	second, done2 := newTestStorage(t)
	defer done2()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	a := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(first)}
	b := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(second)}
	ctx := t.Context()
	registry := prometheus.NewRegistry()
	opts := replication.Options{ReplicationID: "test", SecondaryIDs: []string{"a", "b"}, Registerer: registry}
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{a, b}, opts)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	bucket := storage.MustNewBucketName("locked")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	days := int32(1)
	require.NoError(t, coordinator.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &storage.DefaultRetention{Mode: storage.RetentionModeGovernance, Days: &days}}))
	b.blocked.Store(true)
	_, err = coordinator.PutObject(ctx, bucket, key, nil, strings.NewReader("retained journal data"), nil, nil)
	require.Error(t, err)
	primaryObject, err := primary.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	firstObject, err := first.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.NotEqual(t, *primaryObject.VersionID, *firstObject.VersionID)
	require.True(t, primaryObject.ObjectLock.Retention.RetainUntilDate.Equal(firstObject.ObjectLock.Retention.RetainUntilDate))
	require.EqualValues(t, 1, a.writes.Load())
	require.EqualValues(t, 0, b.writes.Load())
	require.NoError(t, coordinator.Stop(ctx))
	// Reordering configuration must not reorder durable version mappings.
	b.blocked.Store(false)
	opts.SecondaryIDs = []string{"b", "a"}
	coordinator, err = replication.NewStorageWithOptions(p, []storage.Storage{b, a}, opts)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	require.EqualValues(t, 1, a.writes.Load())
	require.EqualValues(t, 1, b.writes.Load())
	secondObject, err := second.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.NotEqual(t, *primaryObject.VersionID, *secondObject.VersionID)
	require.True(t, primaryObject.ObjectLock.Retention.RetainUntilDate.Equal(secondObject.ObjectLock.Retention.RetainUntilDate))
	originalVersionID := *primaryObject.VersionID
	offset := primaryObject.Size
	_, err = coordinator.AppendObject(ctx, bucket, key, strings.NewReader(" appended"), nil, &storage.AppendObjectOptions{WriteOffset: &offset})
	require.NoError(t, err)
	primaryObject, err = primary.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	for _, store := range []*metadataPartStorage{first, second} {
		obj, err := store.HeadObject(ctx, bucket, key, nil)
		require.NoError(t, err)
		require.Equal(t, primaryObject.Size, obj.Size)
		require.True(t, primaryObject.ObjectLock.Retention.RetainUntilDate.Equal(obj.ObjectLock.Retention.RetainUntilDate))
	}
	lockOpts := &storage.ObjectLockOptions{VersionID: primaryObject.VersionID}
	require.NoError(t, coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, lockOpts))
	for _, store := range []*metadataPartStorage{primary, first, second} {
		obj, err := store.HeadObject(ctx, bucket, key, nil)
		require.NoError(t, err)
		require.Equal(t, storage.LegalHoldOn, *obj.ObjectLock.LegalHold)
	}
	_, err = coordinator.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: primaryObject.VersionID, BypassGovernanceRetention: true})
	require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
	require.NoError(t, coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOff, lockOpts))
	shorter := &storage.ObjectRetention{Mode: storage.RetentionModeGovernance, RetainUntilDate: time.Now().UTC().Add(time.Hour)}
	require.NoError(t, coordinator.PutObjectRetention(ctx, bucket, key, shorter, &storage.ObjectLockOptions{VersionID: primaryObject.VersionID, BypassGovernanceRetention: true}))
	for _, store := range []*metadataPartStorage{primary, first, second} {
		obj, err := store.HeadObject(ctx, bucket, key, nil)
		require.NoError(t, err)
		require.True(t, obj.ObjectLock.Retention.RetainUntilDate.Equal(shorter.RetainUntilDate.Truncate(time.Microsecond)))
	}
	_, err = coordinator.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: primaryObject.VersionID, BypassGovernanceRetention: true})
	require.NoError(t, err)
	_, err = coordinator.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: &originalVersionID, BypassGovernanceRetention: true})
	require.NoError(t, err)
	for _, store := range []*metadataPartStorage{primary, first, second} {
		require.Empty(t, physicalPartIDs(t, store))
	}
}

func TestReplicationMapsUnversionedNullVersion(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	coordinator, err := replication.NewStorage(p, s)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	bucket := storage.MustNewBucketName("unversioned")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket))
	put, err := coordinator.PutObject(ctx, bucket, key, nil, strings.NewReader("content"), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, put.VersionID)
	require.Equal(t, "null", *put.VersionID)
	require.NoError(t, coordinator.PutObjectTagging(ctx, bucket, key, map[string]string{"state": "mapped"}, nil))
	tags, err := secondary.GetObjectTagging(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"state": "mapped"}, tags)
}

func TestReplicationMultipartSnapshotResumesDurableParts(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	options := replication.Options{ReplicationID: "multipart-resume", SecondaryIDs: []string{"replica"}, Registerer: prometheus.NewRegistry()}
	ctx := t.Context()
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	bucket := storage.MustNewBucketName("multipart-resume")
	source := storage.MustNewObjectKey("source")
	destination := storage.MustNewObjectKey("destination")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket))
	upload, err := coordinator.CreateMultipartUpload(ctx, bucket, source, nil, nil, nil)
	require.NoError(t, err)
	first, err := coordinator.UploadPart(ctx, bucket, source, upload.UploadId, 1, strings.NewReader("first"), nil)
	require.NoError(t, err)
	second, err := coordinator.UploadPart(ctx, bucket, source, upload.UploadId, 2, strings.NewReader("second"), nil)
	require.NoError(t, err)
	_, err = coordinator.CompleteMultipartUpload(ctx, bucket, source, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{{PartNumber: 1, ETag: first.ETag}, {PartNumber: 2, ETag: second.ETag}}})
	require.NoError(t, err)

	createsBefore := s.multipartCreates.Load()
	partOneBefore := s.partUploads[0].Load()
	partTwoBefore := s.partUploads[1].Load()
	s.failPart.Store(2)
	_, err = coordinator.CopyObject(ctx, bucket, source, bucket, destination, nil)
	require.Error(t, err)
	require.EqualValues(t, createsBefore+1, s.multipartCreates.Load())
	require.EqualValues(t, partOneBefore+1, s.partUploads[0].Load())
	require.EqualValues(t, partTwoBefore+1, s.partUploads[1].Load())
	require.NoError(t, coordinator.Stop(ctx))

	coordinator, err = replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	require.EqualValues(t, createsBefore+1, s.multipartCreates.Load(), "retry must reuse the durable upload ID")
	require.EqualValues(t, partOneBefore+1, s.partUploads[0].Load(), "retry must not retransmit an acknowledged part")
	require.EqualValues(t, partTwoBefore+2, s.partUploads[1].Load())
	object, readers, err := secondary.GetObject(ctx, bucket, destination, nil, nil)
	require.NoError(t, err)
	primaryObject, err := primary.HeadObject(ctx, bucket, destination, nil)
	require.NoError(t, err)
	require.Equal(t, primaryObject.ETag, object.ETag)
	data, err := io.ReadAll(readers[0])
	readers[0].Close()
	require.NoError(t, err)
	require.Equal(t, "firstsecond", string(data))
}

func TestReplicationDoesNotInferAmbiguousMultipartCompletion(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	options := replication.Options{ReplicationID: "multipart-ambiguous", SecondaryIDs: []string{"replica"}, Registerer: prometheus.NewRegistry()}
	ctx := t.Context()
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	bucket := storage.MustNewBucketName("multipart-ambiguous")
	source := storage.MustNewObjectKey("source")
	destination := storage.MustNewObjectKey("destination")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket))
	upload, err := coordinator.CreateMultipartUpload(ctx, bucket, source, nil, nil, nil)
	require.NoError(t, err)
	part1, err := coordinator.UploadPart(ctx, bucket, source, upload.UploadId, 1, strings.NewReader("first"), nil)
	require.NoError(t, err)
	part2, err := coordinator.UploadPart(ctx, bucket, source, upload.UploadId, 2, strings.NewReader("second"), nil)
	require.NoError(t, err)
	_, err = coordinator.CompleteMultipartUpload(ctx, bucket, source, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{{PartNumber: 1, ETag: part1.ETag}, {PartNumber: 2, ETag: part2.ETag}}})
	require.NoError(t, err)
	s.ambiguousComplete.Store(true)
	_, err = coordinator.CopyObject(ctx, bucket, source, bucket, destination, nil)
	require.Error(t, err)
	_, err = coordinator.PutObject(ctx, bucket, storage.MustNewObjectKey("later"), nil, strings.NewReader("later"), nil, nil)
	require.ErrorIs(t, err, replication.ErrIndeterminateReplicaCompletion)
	require.NoError(t, replication.ReconcileStorage(ctx, coordinator, options.ReplicationID, []storage.BucketName{bucket}, false))
	primaryObject, err := primary.HeadObject(ctx, bucket, destination, nil)
	require.NoError(t, err)
	replicaObject, err := secondary.HeadObject(ctx, bucket, destination, nil)
	require.NoError(t, err)
	require.Equal(t, primaryObject.ETag, replicaObject.ETag)
}

func TestReplicationReturnsDeleteMarker(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	coordinator, err := replication.NewStorage(p, s)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	bucket := storage.MustNewBucketName("versioned")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket))
	status := storage.BucketVersioningStatusEnabled
	require.NoError(t, coordinator.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &status}))
	_, err = coordinator.PutObject(ctx, bucket, key, nil, strings.NewReader("content"), nil, nil)
	require.NoError(t, err)
	deleted, err := coordinator.DeleteObjects(ctx, bucket, []storage.DeleteObjectsInputEntry{{Key: key}})
	require.NoError(t, err)
	require.Len(t, deleted.Entries, 1)
	require.True(t, *deleted.Entries[0].DeleteMarker)
	require.Nil(t, deleted.Entries[0].VersionID)
	require.NotNil(t, deleted.Entries[0].DeleteMarkerVersionID)
}

func TestReplicationReconcilePreservesHistoryAndResumes(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, done2 := newTestStorage(t)
	defer done2()
	ctx := t.Context()
	bucket := storage.MustNewBucketName("history")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, primary.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	old, err := primary.PutObject(ctx, bucket, key, nil, strings.NewReader("old unprotected"), nil, nil)
	require.NoError(t, err)
	days := int32(1)
	config := &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &storage.DefaultRetention{Mode: storage.RetentionModeCompliance, Days: &days}}
	require.NoError(t, primary.PutObjectLockConfiguration(ctx, bucket, config))
	retained, err := primary.PutObject(ctx, bucket, key, nil, strings.NewReader("retained"), nil, nil)
	require.NoError(t, err)
	_, err = primary.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.NoError(t, secondary.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	require.NoError(t, secondary.PutObjectLockConfiguration(ctx, bucket, config))
	existing, err := secondary.PutObject(ctx, bucket, key, nil, strings.NewReader("existing replica"), nil, nil)
	require.NoError(t, err)
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	options := replication.Options{ReplicationID: "history", SecondaryIDs: []string{"replica"}, Registerer: prometheus.NewRegistry()}
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	// Dry-run cannot create mappings or alter the destination configuration.
	require.NoError(t, replication.ReconcileStorage(ctx, coordinator, "history", []storage.BucketName{bucket}, true))
	require.EqualValues(t, 0, s.writes.Load())
	err = coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, &storage.ObjectLockOptions{VersionID: old.VersionID})
	require.ErrorIs(t, err, replication.ErrMissingMapping)
	s.blocked.Store(true)
	require.Error(t, replication.ReconcileStorage(ctx, coordinator, "history", []storage.BucketName{bucket}, false))
	require.NoError(t, coordinator.Stop(ctx))
	s.blocked.Store(false)
	coordinator, err = replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	// Recovery completes all copies, restores defaults, and leaves the marker latest.
	versions, err := secondary.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 1000})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 4)
	require.True(t, versions.Versions[0].IsLatest)
	require.True(t, versions.Versions[0].IsDeleteMarker)
	_, err = secondary.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: existing.VersionID})
	require.NoError(t, err)
	configAfter, err := secondary.GetObjectLockConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.Equal(t, config, configAfter)
	var oldReplica *storage.Object
	for _, v := range versions.Versions {
		if v.IsDeleteMarker {
			continue
		}
		obj, readers, err := secondary.GetObject(ctx, bucket, key, nil, &storage.GetObjectOptions{VersionID: &v.VersionID})
		require.NoError(t, err)
		data, err := io.ReadAll(readers[0])
		readers[0].Close()
		require.NoError(t, err)
		if string(data) == "old unprotected" {
			oldReplica = obj
			require.Nil(t, obj.ObjectLock.Retention)
		}
	}
	require.NotNil(t, oldReplica)
	require.NoError(t, coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, &storage.ObjectLockOptions{VersionID: old.VersionID}))
	hold, err := secondary.GetObjectLegalHold(ctx, bucket, key, &storage.ObjectLockOptions{VersionID: oldReplica.VersionID})
	require.NoError(t, err)
	require.Equal(t, storage.LegalHoldOn, *hold)
	_, err = coordinator.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: retained.VersionID, BypassGovernanceRetention: true})
	require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
	require.NoError(t, replication.ReconcileStorage(ctx, coordinator, "history", []storage.BucketName{bucket}, false))
	versionsAgain, err := secondary.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 1000})
	require.NoError(t, err)
	require.Len(t, versionsAgain.Versions, 4)
}

type remotePrimaryStorage struct {
	replicaFaultStorage
	ambiguous atomic.Bool
}

func (s *remotePrimaryStorage) Database() database.Database { return nil }
func (s *remotePrimaryStorage) PutObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksum *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	result, err := s.replicaFaultStorage.PutObject(ctx, bucket, key, contentType, data, checksum, opts)
	if err == nil && s.ambiguous.Load() {
		return nil, errors.New("connection lost after accepting write")
	}
	return result, err
}
func TestReplicationRemotePrimaryAmbiguity(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, done2 := newTestStorage(t)
	defer done2()
	journal, done3 := newTestStorage(t)
	defer done3()
	p := &remotePrimaryStorage{replicaFaultStorage: replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	options := replication.Options{ReplicationID: "remote", SecondaryIDs: []string{"replica"}, JournalDatabase: journal.db, Registerer: prometheus.NewRegistry()}
	ctx := t.Context()
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	bucket, key := storage.MustNewBucketName("remote"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	p.ambiguous.Store(true)
	_, err = coordinator.PutObject(ctx, bucket, key, nil, strings.NewReader("retained input"), nil, nil)
	require.ErrorIs(t, err, replication.ErrIndeterminatePrimary)
	require.NoError(t, coordinator.Stop(ctx))
	p.ambiguous.Store(false)
	coordinator, err = replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	versions, err := primary.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1, "an ambiguous remote write must never be replayed")
	_, err = secondary.HeadObject(ctx, bucket, key, nil)
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	require.NoError(t, replication.ReconcileStorage(ctx, coordinator, "remote", []storage.BucketName{bucket}, false))
	target, err := secondary.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	latest := versions.Versions[0].VersionID
	require.NoError(t, coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, &storage.ObjectLockOptions{VersionID: &latest}))
	hold, err := secondary.GetObjectLegalHold(ctx, bucket, key, &storage.ObjectLockOptions{VersionID: target.VersionID})
	require.NoError(t, err)
	require.Equal(t, storage.LegalHoldOn, *hold)
	// A definite rejection must not leave the entire topology blocked forever.
	require.ErrorIs(t, coordinator.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{}), storage.ErrInvalidObjectLockConfiguration)
	require.NoError(t, coordinator.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOff, &storage.ObjectLockOptions{VersionID: &latest}))
}

func TestReplicationReconcileKeepsUnversionedBucketUnversioned(t *testing.T) {
	primary, done := newTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	ctx := t.Context()
	bucket, key := storage.MustNewBucketName("plain-reconcile"), storage.MustNewObjectKey("key")
	require.NoError(t, primary.CreateBucket(ctx, bucket))
	_, err := primary.PutObject(ctx, bucket, key, nil, strings.NewReader("content"), nil, nil)
	require.NoError(t, err)
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	options := replication.Options{ReplicationID: "plain-reconcile", SecondaryIDs: []string{"replica"}, Registerer: prometheus.NewRegistry()}
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, options)
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(ctx))
	defer coordinator.Stop(ctx)
	require.NoError(t, replication.ReconcileStorage(ctx, coordinator, options.ReplicationID, []storage.BucketName{bucket}, false))
	versioning, err := secondary.GetBucketVersioningConfiguration(ctx, bucket)
	require.NoError(t, err)
	require.Nil(t, versioning.Status)
}
