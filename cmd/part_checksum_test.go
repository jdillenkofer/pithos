package main

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartNumberChecksums(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should validate part downloads of a composite checksum object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:            bucketName,
				Key:               key,
				ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
				ChecksumType:      types.ChecksumTypeComposite,
			})
			require.NoError(t, err)
			bodies := [][]byte{padToMinPartSize([]byte("part 1")), []byte("part 2")}
			completedParts := make([]types.CompletedPart, 0, len(bodies))
			for i, body := range bodies {
				uploadResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:            bucketName,
					Key:               key,
					UploadId:          createResult.UploadId,
					PartNumber:        aws.Int32(int32(i + 1)),
					ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
					Body:              bytes.NewReader(body),
				})
				require.NoError(t, err)
				completedParts = append(completedParts, types.CompletedPart{
					ETag:           uploadResult.ETag,
					ChecksumSHA256: uploadResult.ChecksumSHA256,
					PartNumber:     aws.Int32(int32(i + 1)),
				})
			}
			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:          bucketName,
				Key:             key,
				UploadId:        createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
			})
			require.NoError(t, err)

			for i, body := range bodies {
				partNumber := aws.Int32(int32(i + 1))
				head, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket:       bucketName,
					Key:          key,
					PartNumber:   partNumber,
					ChecksumMode: types.ChecksumModeEnabled,
				})
				require.NoError(t, err)
				assert.Equal(t, completedParts[i].ChecksumSHA256, head.ChecksumSHA256)
				assert.Equal(t, int64(len(body)), *head.ContentLength)

				// With ChecksumMode enabled the SDK validates the part payload
				// against the returned part checksum while it is read.
				get, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
					Bucket:       bucketName,
					Key:          key,
					PartNumber:   partNumber,
					ChecksumMode: types.ChecksumModeEnabled,
				})
				require.NoError(t, err)
				data, err := io.ReadAll(get.Body)
				require.NoError(t, get.Body.Close())
				require.NoError(t, err)
				assert.Equal(t, body, data)
				assert.Equal(t, completedParts[i].ChecksumSHA256, get.ChecksumSHA256)
				assert.Equal(t, int32(len(bodies)), *get.PartsCount)
			}
		})
	})
}
