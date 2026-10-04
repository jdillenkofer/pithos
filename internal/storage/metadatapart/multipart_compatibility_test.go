package metadatapart

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestListPartsReportsEarliestMatchingLifecycleAbortRule(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	t.Cleanup(cleanup)
	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("logs/key")
	require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{}))
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, nil)
	require.NoError(t, err)
	parts, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 1})
	require.NoError(t, err)
	require.Nil(t, parts.AbortDate)
	require.Nil(t, parts.AbortRuleID)
	config := &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{
		{ID: ptrutils.ToPtr("slow"), Status: "Enabled", Prefix: ptrutils.ToPtr(""), AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(7))}},
		{ID: ptrutils.ToPtr("disabled"), Status: "Disabled", Prefix: ptrutils.ToPtr("logs/"), AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(1))}},
		{ID: ptrutils.ToPtr("unrelated"), Status: "Enabled", Prefix: ptrutils.ToPtr("other/"), AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(1))}},
		{ID: ptrutils.ToPtr("fast"), Status: "Enabled", Filter: &storage.LifecycleFilter{Prefix: ptrutils.ToPtr("logs/")}, AbortIncompleteMultipartUpload: &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: ptrutils.ToPtr(int32(3))}},
	}}
	require.NoError(t, st.PutBucketLifecycleConfiguration(ctx, bucket, config))
	parts, err = st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 0})
	require.NoError(t, err)
	uploads, err := st.ListMultipartUploads(ctx, bucket, storage.ListMultipartUploadsOptions{MaxUploads: 1})
	require.NoError(t, err)
	initiated := uploads.Uploads[0].Initiated.UTC()
	expected := time.Date(initiated.Year(), initiated.Month(), initiated.Day()+4, 0, 0, 0, 0, time.UTC)
	require.Equal(t, &expected, parts.AbortDate)
	require.Equal(t, ptrutils.ToPtr("fast"), parts.AbortRuleID)

	// The current configuration, not the initiation-time configuration, applies.
	require.NoError(t, st.DeleteBucketLifecycleConfiguration(ctx, bucket))
	parts, err = st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 1})
	require.NoError(t, err)
	require.Nil(t, parts.AbortDate)
	require.Nil(t, parts.AbortRuleID)
}

func TestMultipartListingsRetainInitiationChecksums(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	t.Cleanup(cleanup)
	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{}))
	algorithm := "SHA256"
	checksumType := "COMPOSITE"
	key := storage.MustNewObjectKey("key")
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, &checksumType, &storage.CreateMultipartUploadOptions{ChecksumAlgorithm: &algorithm})
	require.NoError(t, err)

	// Metadata must be available before any part exists and on empty pages.
	parts, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 0})
	require.NoError(t, err)
	require.Equal(t, &algorithm, parts.ChecksumAlgorithm)
	require.Equal(t, &checksumType, parts.ChecksumType)
	uploads, err := st.ListMultipartUploads(ctx, bucket, storage.ListMultipartUploadsOptions{MaxUploads: 1})
	require.NoError(t, err)
	require.Len(t, uploads.Uploads, 1)
	require.Equal(t, &algorithm, uploads.Uploads[0].ChecksumAlgorithm)
	require.Equal(t, &checksumType, uploads.Uploads[0].ChecksumType)

	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewReader([]byte("part")), nil)
	require.NoError(t, err)
	parts, err = st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 1})
	require.NoError(t, err)
	require.Len(t, parts.Parts, 1)
	require.Equal(t, &algorithm, parts.ChecksumAlgorithm)
	require.Equal(t, &checksumType, parts.ChecksumType)
}

func TestListPartsZeroLimitRetainsUploadMetadata(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	t.Cleanup(cleanup)
	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{OwnerAccountID: "owner"}))
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, &storage.CreateMultipartUploadOptions{Initiator: &storage.ObjectIdentity{AccountID: "owner", PrincipalID: "writer"}})
	require.NoError(t, err)
	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewReader([]byte("part")), nil)
	require.NoError(t, err)

	result, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 0})
	require.NoError(t, err)
	require.Empty(t, result.Parts)
	require.Zero(t, result.MaxParts)
	require.True(t, result.IsTruncated)
	require.Nil(t, result.NextPartNumberMarker)
	require.Equal(t, "owner", result.Owner.AccountID)
	require.Equal(t, "writer", result.Initiator.PrincipalID)

	marker := "1"
	result, err = st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 0, PartNumberMarker: &marker})
	require.NoError(t, err)
	require.Empty(t, result.Parts)
	require.False(t, result.IsTruncated)
}
