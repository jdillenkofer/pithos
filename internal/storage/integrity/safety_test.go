package integrity

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestValidationVerifiedCorruptionDeletesExactVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, failure := range []string{"part-size", "object-size", "object-checksum"} {
		t.Run(failure, func(t *testing.T) {
			s, object, parts := oneObjectFake(t)
			switch failure {
			case "part-size":
				parts[0].Size++
			case "object-size":
				object.Size++
			case "object-checksum":
				object.ChecksumSHA256 = ptrutils.ToPtr("wrong")
			}
			s.inspect = func(visit func(storage.Object, []storage.IntegrityPart) error) error { return visit(object, parts) }
			report, err := NewValidator(s, true, true).ValidateAll(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, report.DeletedObjects)
			require.True(t, report.Results[0].Corrupted)
			require.Len(t, s.deleted, 1)
			require.Equal(t, "null", *s.deleted[0].VersionID)
			require.Equal(t, object.ETag, *s.deleted[0].IfMatchETag)
			require.False(t, s.deleted[0].BypassGovernanceRetention)
		})
	}
}

func TestValidateSuspendedVersioningIncludesNullAndHistoricalVersions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	store, _ := newStorage(t, false)
	ctx := t.Context()
	bucket := storage.MustNewBucketName("suspended-bucket")
	key := storage.MustNewObjectKey("object")
	require.NoError(t, store.CreateBucket(ctx, bucket))
	require.NoError(t, store.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: ptrutils.ToPtr(storage.BucketVersioningStatusEnabled)}))
	old, err := store.PutObject(ctx, bucket, key, nil, strings.NewReader("versioned"), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: ptrutils.ToPtr(storage.BucketVersioningStatusSuspended)}))
	_, err = store.PutObject(ctx, bucket, key, nil, strings.NewReader("null version"), nil, nil)
	require.NoError(t, err)
	report, err := NewValidator(store, false, false).ValidateAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, report.SuccessfulObjects)
	var ids []string
	for _, result := range report.Results {
		ids = append(ids, *result.VersionID)
	}
	require.ElementsMatch(t, []string{*old.VersionID, "null"}, ids)
}

func TestValidationDeletionRespectsObjectLock(t *testing.T) {
	testutils.SkipIfIntegration(t)
	store, root := newStorage(t, false)
	ctx := t.Context()
	bucket := storage.MustNewBucketName("locked-bucket")
	key := storage.MustNewObjectKey("object")
	require.NoError(t, store.CreateBucket(ctx, bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	put, err := store.PutObject(ctx, bucket, key, nil, strings.NewReader("data"), nil, &storage.PutObjectOptions{ObjectLock: storage.ObjectLock{Retention: &storage.ObjectRetention{Mode: storage.RetentionModeCompliance, RetainUntilDate: time.Now().Add(time.Hour)}}})
	require.NoError(t, err)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, os.WriteFile(filepath.Join(root, entries[0].Name()), []byte("oops"), 0600))
	report, err := NewValidator(store, true, true).ValidateAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.FailedObjects)
	require.Zero(t, report.DeletedObjects)
	require.Contains(t, report.Results[0].ActionTaken, "Failed to delete")
	_, err = store.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: put.VersionID})
	require.NoError(t, err)
}

func TestValidationPaginationIncludesVersionsAndSkipsMarkers(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s, _, _ := oneObjectFake(t)
	calls := 0
	s.list = func(opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
		calls++
		version := storage.ObjectVersion{Key: storage.MustNewObjectKey("object"), VersionID: "v1"}
		if calls == 1 {
			return &storage.ListObjectVersionsResult{Versions: []storage.ObjectVersion{version}, IsTruncated: true, NextKeyMarker: ptrutils.ToPtr("object"), NextVersionIDMarker: ptrutils.ToPtr("v1")}, nil
		}
		require.Equal(t, "object", *opts.KeyMarker)
		require.Equal(t, "v1", *opts.VersionIDMarker)
		version.VersionID = "v0"
		marker := storage.ObjectVersion{Key: version.Key, VersionID: "marker", IsDeleteMarker: true}
		return &storage.ListObjectVersionsResult{Versions: []storage.ObjectVersion{version, marker}}, nil
	}
	report, err := NewValidator(s, false, false).ValidateAll(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, 2, report.SuccessfulObjects)
	require.Equal(t, "v1", *report.Results[0].VersionID)
	require.Equal(t, "v0", *report.Results[1].VersionID)
}

func TestValidationRejectsNonAdvancingPagination(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s, _, _ := oneObjectFake(t)
	s.list = func(storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
		return &storage.ListObjectVersionsResult{IsTruncated: true, NextKeyMarker: ptrutils.ToPtr("object"), NextVersionIDMarker: ptrutils.ToPtr("v1")}, nil
	}
	_, err := NewValidator(s, false, false).ValidateAll(context.Background())
	require.ErrorContains(t, err, "did not advance")
}
