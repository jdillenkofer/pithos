package metadatapart

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestCompleteMultipartUploadMinimumPartSize(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := t.Context()
	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	const minimum = 5 * 1024 * 1024
	for i, tc := range []struct {
		name      string
		sizes     []int
		manifest  []int32
		wantError bool
	}{
		{"below minimum", []int{minimum - 1, 1}, []int32{1, 2}, true},
		{"decimal megabytes are too small", []int{5_000_000, 1}, []int32{1, 2}, true},
		{"exact minimum and empty last part", []int{minimum, 0}, []int32{1, 2}, false},
		{"single empty part", []int{0}, []int32{1}, false},
		{"small middle part", []int{minimum, 1, 1}, []int32{1, 2, 3}, true},
		{"omitted small part", []int{minimum, 1, 1}, []int32{1, 3}, false},
		{"last selected part need not be last uploaded", []int{minimum, 1, 1}, []int32{1, 2}, false},
		{"without manifest", []int{minimum - 1, 1}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := storage.MustNewObjectKey(fmt.Sprint(i))
			upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, nil)
			require.NoError(t, err)
			uploaded := make([]*storage.UploadPartResult, len(tc.sizes))
			for n, size := range tc.sizes {
				uploaded[n], err = st.UploadPart(ctx, bucket, key, upload.UploadId, int32(n+1), bytes.NewReader(bytes.Repeat([]byte{byte(n)}, size)), nil)
				require.NoError(t, err)
			}
			var opts *storage.CompleteMultipartUploadOptions
			if tc.manifest != nil {
				opts = &storage.CompleteMultipartUploadOptions{}
				for _, n := range tc.manifest {
					opts.Parts = append(opts.Parts, storage.CompleteMultipartUploadPart{PartNumber: n, ETag: uploaded[n-1].ETag})
				}
			}
			_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, opts)
			if !tc.wantError {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, storage.ErrEntityTooSmall)
			listed, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 10})
			require.NoError(t, err)
			require.Len(t, listed.Parts, len(tc.sizes), "rejection must preserve the upload")
			// A corrected manifest containing only the small final part must still work.
			last := len(tc.sizes)
			_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{{PartNumber: int32(last), ETag: uploaded[last-1].ETag}}})
			require.NoError(t, err)
		})
	}
}
