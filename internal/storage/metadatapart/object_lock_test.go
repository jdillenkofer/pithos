package metadatapart

import (
	"bytes"
	"fmt"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
	"io"
	"runtime"
	"testing"
	"time"
)

func TestObjectLockStorage(t *testing.T) {
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{{"sqlite", newTestStorage}, {"postgres", newPostgresTestStorage}} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "postgres" && runtime.GOOS == "windows" {
				t.Skip("the GitHub Windows Docker daemon has no bridge network for testcontainers")
			}
			st, cleanup := backend.new(t)
			defer cleanup()
			ctx := t.Context()
			bucket := storage.MustNewBucketName("locked-bucket")
			key := storage.MustNewObjectKey("object")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			old, err := st.PutObject(ctx, bucket, key, nil, bytes.NewBufferString("old"), nil, nil)
			require.NoError(t, err)
			_, err = st.GetObjectLockConfiguration(ctx, bucket)
			require.ErrorIs(t, err, storage.ErrObjectLockConfigurationNotFound)
			days := int32(1)
			config := &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &storage.DefaultRetention{Mode: storage.RetentionModeGovernance, Days: &days}}
			require.NoError(t, st.PutObjectLockConfiguration(ctx, bucket, config))
			versioning, err := st.GetBucketVersioningConfiguration(ctx, bucket)
			require.NoError(t, err)
			require.Equal(t, storage.BucketVersioningStatusEnabled, *versioning.Status)
			suspended := storage.BucketVersioningStatusSuspended
			require.ErrorIs(t, st.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: &suspended}), storage.ErrInvalidObjectLockConfiguration)
			require.ErrorIs(t, st.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{}), storage.ErrInvalidObjectLockConfiguration)
			oldRetention, err := st.GetObjectRetention(ctx, bucket, key, &storage.ObjectLockOptions{VersionID: old.VersionID})
			require.NoError(t, err)
			require.Nil(t, oldRetention)
			_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: old.VersionID})
			require.NoError(t, err)
			before := time.Now()
			put, err := st.PutObject(ctx, bucket, key, nil, bytes.NewBufferString("protected"), nil, nil)
			require.NoError(t, err)
			opts := &storage.ObjectLockOptions{VersionID: put.VersionID}
			retention, err := st.GetObjectRetention(ctx, bucket, key, opts)
			require.NoError(t, err)
			require.NotNil(t, retention)
			require.Equal(t, storage.RetentionModeGovernance, retention.Mode)
			require.False(t, retention.RetainUntilDate.Before(before.Add(24*time.Hour-time.Microsecond)))
			parts := physicalPartIDs(t, st)
			_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: put.VersionID})
			require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
			require.Equal(t, parts, physicalPartIDs(t, st))
			marker, err := st.DeleteObject(ctx, bucket, key, nil)
			require.NoError(t, err)
			require.True(t, marker.IsDeleteMarker)
			require.Equal(t, "protected", readObjectContent(t, st, bucket, key, put.VersionID))
			require.ErrorIs(t, st.DeleteBucket(ctx, bucket), storage.ErrBucketNotEmpty)
			require.NoError(t, st.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, opts))
			bypassDelete := &storage.DeleteObjectOptions{VersionID: put.VersionID, BypassGovernanceRetention: true}
			_, err = st.DeleteObject(ctx, bucket, key, bypassDelete)
			require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
			shorter := &storage.ObjectRetention{Mode: storage.RetentionModeGovernance, RetainUntilDate: time.Now().Add(time.Hour)}
			require.ErrorIs(t, st.PutObjectRetention(ctx, bucket, key, shorter, opts), storage.ErrObjectLockAccessDenied)
			bypassLock := &storage.ObjectLockOptions{VersionID: put.VersionID, BypassGovernanceRetention: true}
			require.NoError(t, st.PutObjectRetention(ctx, bucket, key, shorter, bypassLock))
			require.NoError(t, st.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOff, opts))
			multi, err := st.DeleteObjects(ctx, bucket, []storage.DeleteObjectsInputEntry{{Key: key, VersionID: put.VersionID}, {Key: key, VersionID: marker.VersionID}})
			require.NoError(t, err)
			require.Len(t, multi.Entries, 2)
			require.Equal(t, "AccessDenied", multi.Entries[0].ErrCode)
			require.Equal(t, put.VersionID, multi.Entries[0].VersionID)
			require.True(t, multi.Entries[1].Deleted)
			_, err = st.DeleteObject(ctx, bucket, key, bypassDelete)
			require.NoError(t, err)
			require.Empty(t, physicalPartIDs(t, st))
			// Explicit compliance retention overrides the bucket's governance default.
			compliance := &storage.ObjectRetention{Mode: storage.RetentionModeCompliance, RetainUntilDate: time.Now().UTC().Add(2 * time.Hour)}
			put, err = st.PutObject(ctx, bucket, key, nil, bytes.NewBufferString("compliance"), nil, &storage.PutObjectOptions{ObjectLock: storage.ObjectLock{Retention: compliance}})
			require.NoError(t, err)
			opts = &storage.ObjectLockOptions{VersionID: put.VersionID, BypassGovernanceRetention: true}
			require.ErrorIs(t, st.PutObjectRetention(ctx, bucket, key, nil, opts), storage.ErrObjectLockAccessDenied)
			_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: put.VersionID, BypassGovernanceRetention: true})
			require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
			longer := *compliance
			longer.RetainUntilDate = longer.RetainUntilDate.Add(time.Hour)
			require.NoError(t, st.PutObjectRetention(ctx, bucket, key, &longer, opts))
			// Removing the default does not remove existing compliance protection.
			require.NoError(t, st.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}))
			retention, err = st.GetObjectRetention(ctx, bucket, key, opts)
			require.NoError(t, err)
			require.True(t, retention.RetainUntilDate.Equal(longer.RetainUntilDate.Truncate(time.Microsecond)))
		})
	}
}

func TestObjectLockMultipartAndCopy(t *testing.T) {
	st, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := t.Context()
	bucket := storage.MustNewBucketName("locked-bucket")
	key := storage.MustNewObjectKey("source")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	require.NoError(t, st.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}))
	hold := storage.LegalHoldOn
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, &storage.CreateMultipartUploadOptions{ObjectLock: storage.ObjectLock{LegalHold: &hold}})
	require.NoError(t, err)
	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewBufferString("multipart"), nil)
	require.NoError(t, err)
	days := int32(1)
	require.NoError(t, st.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &storage.DefaultRetention{Mode: storage.RetentionModeCompliance, Days: &days}}))
	completed, err := st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, nil)
	require.NoError(t, err)
	obj, err := st.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	require.NotNil(t, obj.ObjectLock.Retention)
	require.Equal(t, storage.RetentionModeCompliance, obj.ObjectLock.Retention.Mode)
	require.Equal(t, &hold, obj.ObjectLock.LegalHold)
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: completed.VersionID, BypassGovernanceRetention: true})
	require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
	days = 2
	require.NoError(t, st.PutObjectLockConfiguration(ctx, bucket, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled", DefaultRetention: &storage.DefaultRetention{Mode: storage.RetentionModeGovernance, Days: &days}}))
	dst := storage.MustNewObjectKey("copy")
	_, err = st.CopyObject(ctx, bucket, key, bucket, dst, nil)
	require.NoError(t, err)
	obj, err = st.HeadObject(ctx, bucket, dst, nil)
	require.NoError(t, err)
	require.Nil(t, obj.ObjectLock.LegalHold)
	require.Equal(t, storage.RetentionModeGovernance, obj.ObjectLock.Retention.Mode)
	// A pending upload's hold must not prevent aborting incomplete data.
	upload, err = st.CreateMultipartUpload(ctx, bucket, dst, nil, nil, &storage.CreateMultipartUploadOptions{ObjectLock: storage.ObjectLock{LegalHold: &hold}})
	require.NoError(t, err)
	_, err = st.UploadPart(ctx, bucket, dst, upload.UploadId, 1, bytes.NewBufferString("abort"), nil)
	require.NoError(t, err)
	require.NoError(t, st.AbortMultipartUpload(ctx, bucket, dst, upload.UploadId))
}

func TestObjectLockConcurrentProtectionAndActivation(t *testing.T) {
	for _, backend := range []struct {
		name string
		new  func(*testing.T) (*metadataPartStorage, func())
	}{{"sqlite", newTestStorage}, {"postgres", newPostgresTestStorage}} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "postgres" && runtime.GOOS == "windows" {
				t.Skip("the GitHub Windows Docker daemon has no bridge network for testcontainers")
			}
			st, cleanup := backend.new(t)
			defer cleanup()
			ctx := t.Context()
			bucket := storage.MustNewBucketName("concurrent-lock")
			require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
			for _, protection := range []string{"retention", "hold"} {
				for iteration := 0; iteration < 10; iteration++ {
					key := storage.MustNewObjectKey(fmt.Sprintf("%s-%d", protection, iteration))
					put, err := st.PutObject(ctx, bucket, key, nil, bytes.NewBufferString(key.String()), nil, nil)
					require.NoError(t, err)
					start := make(chan struct{})
					protected := make(chan error, 1)
					deleted := make(chan error, 1)
					go func() {
						<-start
						var err error
						if protection == "hold" {
							err = st.PutObjectLegalHold(ctx, bucket, key, storage.LegalHoldOn, &storage.ObjectLockOptions{VersionID: put.VersionID})
						} else {
							err = st.PutObjectRetention(ctx, bucket, key, &storage.ObjectRetention{Mode: storage.RetentionModeCompliance, RetainUntilDate: time.Now().Add(time.Hour)}, &storage.ObjectLockOptions{VersionID: put.VersionID})
						}
						protected <- err
					}()
					go func() {
						<-start
						_, err := st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: put.VersionID, BypassGovernanceRetention: true})
						deleted <- err
					}()
					close(start)
					protectionErr, deleteErr := <-protected, <-deleted
					if protectionErr == nil {
						require.ErrorIs(t, deleteErr, storage.ErrObjectLockAccessDenied)
						_, readers, err := st.GetObject(ctx, bucket, key, nil, &storage.GetObjectOptions{VersionID: put.VersionID})
						require.NoError(t, err)
						data, err := io.ReadAll(readers[0])
						readers[0].Close()
						require.NoError(t, err)
						require.Equal(t, key.String(), string(data))
					} else {
						require.ErrorIs(t, protectionErr, storage.ErrNoSuchKey)
						require.NoError(t, deleteErr)
					}
				}
			}
			for iteration := 0; iteration < 10; iteration++ {
				name := storage.MustNewBucketName(fmt.Sprintf("activation-%d", iteration))
				key := storage.MustNewObjectKey("key")
				require.NoError(t, st.CreateBucket(ctx, name))
				start := make(chan struct{})
				activated := make(chan error, 1)
				written := make(chan error, 1)
				suspended := make(chan error, 1)
				go func() {
					<-start
					activated <- st.PutObjectLockConfiguration(ctx, name, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"})
				}()
				go func() {
					<-start
					_, err := st.PutObject(ctx, name, key, nil, bytes.NewBufferString("data"), nil, nil)
					written <- err
				}()
				go func() {
					<-start
					status := storage.BucketVersioningStatusSuspended
					suspended <- st.PutBucketVersioningConfiguration(ctx, name, &storage.BucketVersioningConfiguration{Status: &status})
				}()
				close(start)
				require.NoError(t, <-activated)
				require.NoError(t, <-written)
				if err := <-suspended; err != nil {
					require.ErrorIs(t, err, storage.ErrInvalidObjectLockConfiguration)
				}
				versioning, err := st.GetBucketVersioningConfiguration(ctx, name)
				require.NoError(t, err)
				require.Equal(t, storage.BucketVersioningStatusEnabled, *versioning.Status)
			}
		})
	}
}
