package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var allObjectAttributes = []types.ObjectAttributes{
	types.ObjectAttributesEtag,
	types.ObjectAttributesChecksum,
	types.ObjectAttributesObjectParts,
	types.ObjectAttributesStorageClass,
	types.ObjectAttributesObjectSize,
}

func TestGetObjectAttributes(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		setup := func(t *testing.T) *s3.Client {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)
			return s3Client
		}

		t.Run("it should return the attributes of a single part object"+testSuffix, func(t *testing.T) {
			s3Client := setup(t)
			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:            bucketName,
				Key:               key,
				Body:              bytes.NewReader([]byte("hello world")),
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			})
			require.NoError(t, err)

			result, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: allObjectAttributes,
			})
			require.NoError(t, err)
			assert.Equal(t, strings.Trim(*putResult.ETag, `"`), *result.ETag)
			require.NotNil(t, result.Checksum)
			assert.Equal(t, putResult.ChecksumCRC32, result.Checksum.ChecksumCRC32)
			assert.Equal(t, types.ChecksumTypeFullObject, result.Checksum.ChecksumType)
			assert.Nil(t, result.ObjectParts)
			assert.Equal(t, types.StorageClassStandard, result.StorageClass)
			assert.Equal(t, int64(len("hello world")), *result.ObjectSize)
			assert.NotNil(t, result.LastModified)
		})

		t.Run("it should return only the requested attributes"+testSuffix, func(t *testing.T) {
			s3Client := setup(t)
			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("hello world")),
			})
			require.NoError(t, err)

			result, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
			})
			require.NoError(t, err)
			assert.Equal(t, int64(len("hello world")), *result.ObjectSize)
			assert.Nil(t, result.ETag)
			assert.Nil(t, result.Checksum)
			assert.Empty(t, result.StorageClass)
		})

		t.Run("it should list the parts of a composite checksum multipart object"+testSuffix, func(t *testing.T) {
			s3Client := setup(t)
			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:            bucketName,
				Key:               key,
				ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
				ChecksumType:      types.ChecksumTypeComposite,
			})
			require.NoError(t, err)
			bodies := [][]byte{padToMinPartSize([]byte("part 1")), padToMinPartSize([]byte("part 2")), []byte("part 3")}
			completedParts := make([]types.CompletedPart, 0, len(bodies))
			for i, body := range bodies {
				partNumber := int32(i + 1)
				uploadResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:            bucketName,
					Key:               key,
					UploadId:          createResult.UploadId,
					PartNumber:        aws.Int32(partNumber),
					ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
					Body:              bytes.NewReader(body),
				})
				require.NoError(t, err)
				completedParts = append(completedParts, types.CompletedPart{
					ETag:           uploadResult.ETag,
					ChecksumSHA256: uploadResult.ChecksumSHA256,
					PartNumber:     aws.Int32(partNumber),
				})
			}
			completeResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:          bucketName,
				Key:             key,
				UploadId:        createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
			})
			require.NoError(t, err)

			result, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: allObjectAttributes,
			})
			require.NoError(t, err)
			assert.Equal(t, strings.Trim(*completeResult.ETag, `"`), *result.ETag)
			require.NotNil(t, result.Checksum)
			assert.Equal(t, types.ChecksumTypeComposite, result.Checksum.ChecksumType)
			assert.Equal(t, fmt.Sprintf("%s-%d", *result.Checksum.ChecksumSHA256, len(bodies)), *completeResult.ChecksumSHA256)

			require.NotNil(t, result.ObjectParts)
			assert.Equal(t, int32(len(bodies)), *result.ObjectParts.TotalPartsCount)
			assert.False(t, *result.ObjectParts.IsTruncated)
			require.Len(t, result.ObjectParts.Parts, len(bodies))
			var totalSize int64
			for i, part := range result.ObjectParts.Parts {
				assert.Equal(t, int32(i+1), *part.PartNumber)
				assert.Equal(t, int64(len(bodies[i])), *part.Size)
				assert.Equal(t, completedParts[i].ChecksumSHA256, part.ChecksumSHA256)
				// Parts report exactly the algorithms the object checksum reports.
				assert.Equal(t, result.Checksum.ChecksumCRC32 != nil, part.ChecksumCRC32 != nil)
				assert.Equal(t, result.Checksum.ChecksumCRC64NVME != nil, part.ChecksumCRC64NVME != nil)
				totalSize += *part.Size
			}
			assert.Equal(t, totalSize, *result.ObjectSize)

			page, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
				MaxParts:         aws.Int32(1),
				PartNumberMarker: aws.String("1"),
			})
			require.NoError(t, err)
			require.Len(t, page.ObjectParts.Parts, 1)
			assert.Equal(t, int32(2), *page.ObjectParts.Parts[0].PartNumber)
			assert.True(t, *page.ObjectParts.IsTruncated)
			assert.Equal(t, "2", *page.ObjectParts.NextPartNumberMarker)
		})

		t.Run("it should select an explicit version"+testSuffix, func(t *testing.T) {
			s3Client := setup(t)
			_, err := s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			require.NoError(t, err)
			first, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("first"))})
			require.NoError(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("second version"))})
			require.NoError(t, err)

			result, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				VersionId:        first.VersionId,
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
			})
			require.NoError(t, err)
			assert.Equal(t, int64(len("first")), *result.ObjectSize)
			assert.Equal(t, first.VersionId, result.VersionId)

			_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucketName, Key: key})
			require.NoError(t, err)
			_, err = s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
			})
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NoSuchKey", apiErr.ErrorCode())
		})

		t.Run("it should fail for a missing object"+testSuffix, func(t *testing.T) {
			s3Client := setup(t)
			_, err := s3Client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
				Bucket:           bucketName,
				Key:              key,
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag},
			})
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NoSuchKey", apiErr.ErrorCode())
		})
	})
}
