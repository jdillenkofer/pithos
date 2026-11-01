package metadatapart

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestCompleteMultipartUploadChecksumPartNumbering(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, mode := range []struct {
		name, algorithm, checksumType string
	}{
		{name: "no additional checksum"},
		{name: "composite SHA256", algorithm: "SHA256", checksumType: storage.ChecksumTypeComposite},
		{name: "full object CRC32", algorithm: "CRC32", checksumType: storage.ChecksumTypeFullObject},
	} {
		t.Run(mode.name, func(t *testing.T) {
			st, cleanup := newTestStorage(t)
			defer cleanup()
			ctx := context.Background()
			bucket := storage.MustNewBucketName("bucket")
			require.NoError(t, st.CreateBucket(ctx, bucket))

			for _, tt := range []struct {
				numbers        []int32
				nonConsecutive bool
			}{
				{numbers: []int32{1, 2}},
				{numbers: []int32{1, 3, 5}, nonConsecutive: true},
				{numbers: []int32{2, 3}, nonConsecutive: true},
			} {
				numbers := tt.numbers
				t.Run(fmt.Sprint(numbers), func(t *testing.T) {
					key := storage.MustNewObjectKey(fmt.Sprint(numbers))
					var opts *storage.CreateMultipartUploadOptions
					var checksumType *string
					if mode.algorithm != "" {
						opts = &storage.CreateMultipartUploadOptions{ChecksumAlgorithm: &mode.algorithm}
						checksumType = &mode.checksumType
					}
					upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, checksumType, opts)
					require.NoError(t, err)

					// Upload out of order, then replace a part. Unselected parts
					// include gaps and must not invalidate a consecutive manifest.
					parts := make(map[int32]*storage.UploadPartResult)
					for _, number := range []int32{5, 3, 2, 1, 2} {
						parts[number], err = st.UploadPart(ctx, bucket, key, upload.UploadId, number, bytes.NewBufferString(fmt.Sprint(number)), nil)
						require.NoError(t, err)
					}
					buildManifest := func(numbers []int32) []storage.CompleteMultipartUploadPart {
						manifest := make([]storage.CompleteMultipartUploadPart, len(numbers))
						for i, number := range numbers {
							manifest[i] = storage.CompleteMultipartUploadPart{PartNumber: number, ETag: parts[number].ETag}
							if mode.checksumType == storage.ChecksumTypeComposite {
								manifest[i].ChecksumSHA256 = parts[number].ChecksumSHA256
							}
						}
						return manifest
					}
					_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: buildManifest(numbers)})
					if mode.algorithm != "" && tt.nonConsecutive {
						require.ErrorIs(t, err, storage.ErrInvalidPartOrder)
						// Rejection must leave the upload intact for a corrected completion.
						listed, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 10})
						require.NoError(t, err)
						require.Len(t, listed.Parts, 4)
						_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: buildManifest([]int32{1, 2})})
						require.NoError(t, err)
						require.Equal(t, "12", readObjectContent(t, st, bucket, key, nil))
						return
					}
					require.NoError(t, err)
					wantContent := ""
					for _, number := range numbers {
						wantContent += fmt.Sprint(number)
					}
					require.Equal(t, wantContent, readObjectContent(t, st, bucket, key, nil))
				})
			}
		})
	}
}
