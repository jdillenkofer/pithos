package metadatapart

import (
	"bytes"
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

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
