package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestCompleteMultipartUploadPartVerification(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		// setupUpload creates a bucket and a multipart upload with two
		// uploaded parts and returns the parts as the client would declare
		// them in CompleteMultipartUpload.
		setupUpload := func(t *testing.T) (*s3.Client, *string, []types.CompletedPart) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})
			require.NoError(t, err)

			completedParts := make([]types.CompletedPart, 0, 2)
			for partNumber := int32(1); partNumber <= 2; partNumber++ {
				uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createResult.UploadId,
					PartNumber: aws.Int32(partNumber),
					Body:       bytes.NewReader([]byte(fmt.Sprintf("part %d content", partNumber))),
				})
				require.NoError(t, err)
				completedParts = append(completedParts, types.CompletedPart{
					ETag:       uploadPartResult.ETag,
					PartNumber: aws.Int32(partNumber),
				})
			}
			return s3Client, createResult.UploadId, completedParts
		}

		completeUpload := func(s3Client *s3.Client, uploadId *string, parts []types.CompletedPart) error {
			_, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: parts,
				},
			})
			return err
		}

		assertApiError := func(t *testing.T, err error, expectedCode string) {
			assert.Error(t, err)
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				assert.Equal(t, expectedCode, apiErr.ErrorCode())
			} else {
				assert.Fail(t, "Expected API error", "err %v", err)
			}
		}

		t.Run("it should reject completing a multipart upload with a wrong part etag"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].ETag = aws.String("\"00000000000000000000000000000000\"")
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload with a wrong part checksum"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].ChecksumCRC32 = aws.String("AAAAAA==")
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload referencing a part that was never uploaded"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			completedParts[1].PartNumber = aws.Int32(5)
			err := completeUpload(s3Client, uploadId, completedParts)
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload that omits an uploaded part"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, completedParts[:1])
			assertApiError(t, err, "InvalidPart")
		})

		t.Run("it should reject completing a multipart upload with parts out of order"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, []types.CompletedPart{completedParts[1], completedParts[0]})
			assertApiError(t, err, "InvalidPartOrder")
		})

		t.Run("it should complete a multipart upload with matching etags and checksums"+testSuffix, func(t *testing.T) {
			s3Client, uploadId, completedParts := setupUpload(t)
			err := completeUpload(s3Client, uploadId, completedParts)
			assert.NoError(t, err)
		})
	})
}

func TestMultipartUpload(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err = s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(1)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		// We need a large enough payload to force a multipart upload
		var largePayload []byte = make([]byte, manager.MinUploadPartSize*2)
		r := rand.New(rand.NewSource(int64(1337)))
		r.Read(largePayload)

		for _, checksumAlgorithm := range []types.ChecksumAlgorithm{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
			t.Run("it should allow multipart uploads using checksumAlgorithm "+string(checksumAlgorithm)+testSuffix, func(t *testing.T) {
				s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
				t.Cleanup(cleanup)
				createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
					Bucket: bucketName,
				})
				if err != nil {
					assert.Fail(t, "CreateBucket failed", "err %v", err)
				}
				assert.NotNil(t, createBucketResult)

				uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
					u.Concurrency = 1
					u.PartSize = manager.MinUploadPartSize
				})
				uploadOutput, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
					Bucket:            bucketName,
					Key:               key,
					Body:              bytes.NewReader(largePayload),
					ChecksumAlgorithm: checksumAlgorithm,
				})
				if err != nil {
					assert.Fail(t, "Upload failed", "err %v", err)
				}
				assert.NotNil(t, uploadOutput)
				assert.Equal(t, key, uploadOutput.Key)
				assert.Equal(t, 2, len(uploadOutput.CompletedParts))

				firstPart := uploadOutput.CompletedParts[0]
				assert.Equal(t, "\"32571c347a10c52443114d7adccdda7e\"", *firstPart.ETag)
				assert.Equal(t, "ofCoFg==", *firstPart.ChecksumCRC32)
				assert.Equal(t, "Abkn5A==", *firstPart.ChecksumCRC32C)
				assert.Equal(t, "aNFz0eBLamw=", *firstPart.ChecksumCRC64NVME)
				assert.Equal(t, "vaP4YsHAheW1JNwC9QjaFR+Mgxs=", *firstPart.ChecksumSHA1)
				assert.Equal(t, "U6bAqRNZMqgKtspwYKBPvggn3jo8VmDB1YzZ+0Vf+KM=", *firstPart.ChecksumSHA256)
				assert.Equal(t, int32(1), *firstPart.PartNumber)

				secondPart := uploadOutput.CompletedParts[1]
				assert.Equal(t, "\"ddcce51e31a68a3268905f2794b0bbb4\"", *secondPart.ETag)
				assert.Equal(t, "HUzpnQ==", *secondPart.ChecksumCRC32)
				assert.Equal(t, "2FK6Vg==", *secondPart.ChecksumCRC32C)
				assert.Equal(t, "gBCc/dz5qRU=", *secondPart.ChecksumCRC64NVME)
				assert.Equal(t, "/OkGPn2ayY9pA2v/JEo+B0hsUpg=", *secondPart.ChecksumSHA1)
				assert.Equal(t, "drUohCpA8EjWtWwhvmxeNp6G/cb8/Y3X6h6FnCZs3Bk=", *secondPart.ChecksumSHA256)
				assert.Equal(t, int32(2), *secondPart.PartNumber)

				assert.Equal(t, "\"b676ed737ae82cda0bc622cd80116002-2\"", *uploadOutput.ETag)
				assert.Equal(t, "ICnSTA==", *uploadOutput.ChecksumCRC32)
				assert.Equal(t, "wHOQSg==", *uploadOutput.ChecksumCRC32C)
				assert.Equal(t, "hJdk5JLZLJk=", *uploadOutput.ChecksumCRC64NVME)
				assert.Nil(t, uploadOutput.ChecksumSHA1)
				assert.Nil(t, uploadOutput.ChecksumSHA256)

				getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: bucketName,
					Key:    key,
				})
				if err != nil {
					assert.Fail(t, "GetObject failed", "err %v", err)
				}
				assert.NotNil(t, getObjectResult)
				assert.NotNil(t, getObjectResult.Body)
				objectBytes, err := io.ReadAll(getObjectResult.Body)
				assert.Nil(t, err)
				assert.Equal(t, largePayload, objectBytes)
			})
		}

		t.Run("it should allow listing multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListMultipartUploads failed", "err %v", err)
			}

			assert.NotNil(t, listMultipartUploadsResult)
			assert.Equal(t, *bucketName, *listMultipartUploadsResult.Bucket)
			assert.Equal(t, *keyPrefix, *listMultipartUploadsResult.Prefix)
			assert.Len(t, listMultipartUploadsResult.Uploads, 1)
			firstUpload := listMultipartUploadsResult.Uploads[0]
			assert.Equal(t, *key, *firstUpload.Key)
			assert.Equal(t, *uploadId, *firstUpload.UploadId)
			assert.NotNil(t, firstUpload.Initiated)
		})

		t.Run("it should allow listing two multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key2,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload2 failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key2, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListMultipartUploads failed", "err %v", err)
			}

			assert.NotNil(t, listMultipartUploadsResult)
			assert.Equal(t, *bucketName, *listMultipartUploadsResult.Bucket)
			assert.Equal(t, *keyPrefix, *listMultipartUploadsResult.Prefix)
			assert.Len(t, listMultipartUploadsResult.Uploads, 2)
			firstUpload := listMultipartUploadsResult.Uploads[0]
			assert.Equal(t, *key, *firstUpload.Key)
			assert.Equal(t, *uploadId, *firstUpload.UploadId)
			assert.NotNil(t, firstUpload.Initiated)
			secondUpload := listMultipartUploadsResult.Uploads[1]
			assert.Equal(t, *key2, *secondUpload.Key)
			assert.Equal(t, *uploadId2, *secondUpload.UploadId)
			assert.NotNil(t, secondUpload.Initiated)
		})

		t.Run("it should filter listed multipart uploads via authorizer hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListMultipartUpload(request, key, uploadId)
			  return string.find(key, "allowed/") == 1
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: aws.String("allowed/file.txt")})
			assert.Nil(t, err)
			_, err = s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: aws.String("denied/file.txt")})
			assert.Nil(t, err)

			listMultipartUploadsResult, err := s3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Len(t, listMultipartUploadsResult.Uploads, 1)
			assert.Equal(t, "allowed/file.txt", *listMultipartUploadsResult.Uploads[0].Key)
		})

		t.Run("it should allow multipart uploads with two parts"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow listing parts of multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			// listParts (but empty)
			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Len(t, listPartsResult.Parts, 0)
			assert.False(t, *listPartsResult.IsTruncated)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart := listPartsResult.Parts[0]
			assert.Equal(t, int32(2), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *firstPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *firstPart.ETag)
			assert.Equal(t, "2GpoVg==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *firstPart.ChecksumSHA256)
			assert.False(t, *listPartsResult.IsTruncated)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			// listParts with limit
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(1), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[0:2])), *firstPart.Size)
			assert.Equal(t, "\"a64cf5823262686e1a28b2245be34ce0\"", *firstPart.ETag)
			assert.Equal(t, "RKFCJQ==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "x1X1EA==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "MhUdjJvefpY=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "U6QXeWx3eFEAOz8kMeju9WJewVs=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "MO/ftS/2f4Dat8uJ3P4O7IQSlmz+WDJJk2dLRhbWvRE=", *firstPart.ChecksumSHA256)
			assert.True(t, *listPartsResult.IsTruncated)

			// listParts with partNumberMarker offset
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:           bucketName,
				Key:              key,
				UploadId:         uploadId,
				PartNumberMarker: aws.String("1"),
				MaxParts:         aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, "1", *listPartsResult.PartNumberMarker)
			assert.Equal(t, int32(1), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 1)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(2), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *firstPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *firstPart.ETag)
			assert.Equal(t, "2GpoVg==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *firstPart.ChecksumSHA256)
			assert.False(t, *listPartsResult.IsTruncated)

			// listParts (all)
			listPartsResult, err = s3Client.ListParts(context.Background(), &s3.ListPartsInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
				MaxParts: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListParts failed", "err %v", err)
			}
			assert.NotNil(t, listPartsResult)
			assert.Equal(t, *bucketName, *listPartsResult.Bucket)
			assert.Equal(t, *key, *listPartsResult.Key)
			assert.Equal(t, *uploadId, *listPartsResult.UploadId)
			assert.Equal(t, int32(2), *listPartsResult.MaxParts)
			assert.Len(t, listPartsResult.Parts, 2)
			assert.False(t, *listPartsResult.IsTruncated)
			firstPart = listPartsResult.Parts[0]
			assert.Equal(t, int32(1), *firstPart.PartNumber)
			assert.Equal(t, int64(len(body[0:2])), *firstPart.Size)
			assert.Equal(t, "\"a64cf5823262686e1a28b2245be34ce0\"", *firstPart.ETag)
			assert.Equal(t, "RKFCJQ==", *firstPart.ChecksumCRC32)
			assert.Equal(t, "x1X1EA==", *firstPart.ChecksumCRC32C)
			assert.Equal(t, "MhUdjJvefpY=", *firstPart.ChecksumCRC64NVME)
			assert.Equal(t, "U6QXeWx3eFEAOz8kMeju9WJewVs=", *firstPart.ChecksumSHA1)
			assert.Equal(t, "MO/ftS/2f4Dat8uJ3P4O7IQSlmz+WDJJk2dLRhbWvRE=", *firstPart.ChecksumSHA256)
			secondPart := listPartsResult.Parts[1]
			assert.Equal(t, int32(2), *secondPart.PartNumber)
			assert.Equal(t, int64(len(body[2:])), *secondPart.Size)
			assert.Equal(t, "\"ea0cfed76183b9faf2e87ca949d9c4b8\"", *secondPart.ETag)
			assert.Equal(t, "2GpoVg==", *secondPart.ChecksumCRC32)
			assert.Equal(t, "80iBtg==", *secondPart.ChecksumCRC32C)
			assert.Equal(t, "Iy5Z/rXq8uI=", *secondPart.ChecksumCRC64NVME)
			assert.Equal(t, "h1jfWItGBQfcDNMMI6FANuZJam4=", *secondPart.ChecksumSHA1)
			assert.Equal(t, "QzRXKSwYas0BDNnAMkDZMLlliJd9xDozckIiOuCoaao=", *secondPart.ChecksumSHA256)
		})

		t.Run("it should filter listed parts via authorizer hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListPart(request, partNumber)
			  return partNumber == 2
			end
			`
			requestAuthorizer, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}

			s3Client, _, cleanup := setupTestServerWithAuthorizer(requestAuthorizer, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			createMultipartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			uploadId := createMultipartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: bucketName, Key: key, UploadId: uploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("part-1"))})
			assert.Nil(t, err)
			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: bucketName, Key: key, UploadId: uploadId, PartNumber: aws.Int32(2), Body: bytes.NewReader([]byte("part-2"))})
			assert.Nil(t, err)

			listPartsResult, err := s3Client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: bucketName, Key: key, UploadId: uploadId})
			assert.Nil(t, err)
			assert.Len(t, listPartsResult.Parts, 1)
			assert.Equal(t, int32(2), *listPartsResult.Parts[0].PartNumber)
		})

		t.Run("it should allow cancellation of multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUpload, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUpload)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should allow two multipart uploads on same key after complete first"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow two multipart uploads on same key after abort first"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUploadResult, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUploadResult)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should reject stale If-Match CompleteMultipartUpload during concurrent completes"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			initialPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("initial-state")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, initialPut.ETag)

			prepareUpload := func(payload []byte) *string {
				createOut, createErr := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
					Bucket: bucketName,
					Key:    key,
				})
				if !assert.Nil(t, createErr) {
					return nil
				}
				if !assert.NotNil(t, createOut.UploadId) {
					return nil
				}

				_, uploadErr := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createOut.UploadId,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(payload),
				})
				if !assert.Nil(t, uploadErr) {
					return nil
				}

				return createOut.UploadId
			}

			uploadIDs := []*string{
				prepareUpload([]byte("payload-a")),
				prepareUpload([]byte("payload-b")),
			}
			if !assert.NotNil(t, uploadIDs[0]) || !assert.NotNil(t, uploadIDs[1]) {
				return
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
						Bucket:   bucketName,
						Key:      key,
						UploadId: uploadIDs[i],
						IfMatch:  initialPut.ETag,
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent stale If-Match CompleteMultipartUpload may succeed")
		})

		t.Run("it should allow at most one concurrent create-if-absent CompleteMultipartUpload"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			prepareUpload := func(payload []byte) *string {
				createOut, createErr := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
					Bucket: bucketName,
					Key:    key,
				})
				if !assert.Nil(t, createErr) {
					return nil
				}
				if !assert.NotNil(t, createOut.UploadId) {
					return nil
				}

				_, uploadErr := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:     bucketName,
					Key:        key,
					UploadId:   createOut.UploadId,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(payload),
				})
				if !assert.Nil(t, uploadErr) {
					return nil
				}

				return createOut.UploadId
			}

			uploadIDs := []*string{
				prepareUpload([]byte("payload-1")),
				prepareUpload([]byte("payload-2")),
			}
			if !assert.NotNil(t, uploadIDs[0]) || !assert.NotNil(t, uploadIDs[1]) {
				return
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
						Bucket:      bucketName,
						Key:         key,
						UploadId:    uploadIDs[i],
						IfNoneMatch: aws.String("*"),
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-None-Match CompleteMultipartUpload may succeed")
		})

		t.Run("it should hit the upload limit when uploading a part that is too large"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			// Use a LimitReader over a repeating source to simulate a large payload without allocating 5GB of RAM
			largePayload := io.LimitReader(ioutils.NewRepeatingReader([]byte("huge")), storage.MaxEntitySize+1)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       largePayload,
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			assert.Nil(t, uploadPartResult)
			if err == nil {
				assert.Fail(t, "UploadPart should have failed due to size limit")
			}
			var smithyOperationError *smithy.OperationError
			if !errors.As(err, &smithyOperationError) {
				assert.Fail(t, "Expected error smithy.OperationError", "err %v", err)
			}
		})
	})
}

func TestUploadPartCopy(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should copy a whole source object into a multipart part"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			copyPartResult, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:     bucketName,
				Key:        key2,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			assert.NotNil(t, copyPartResult.CopyPartResult)
			assert.NotNil(t, copyPartResult.CopyPartResult.ETag)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key2,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: copyPartResult.CopyPartResult.ETag, PartNumber: aws.Int32(1)},
					},
				},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should copy a byte range of the source into a multipart part"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			// Copy "Hello," (bytes 0-5) as part 1 and "world!" (bytes 7-12) as part 2.
			part1, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:          bucketName,
				Key:             key2,
				UploadId:        uploadId,
				PartNumber:      aws.Int32(1),
				CopySource:      aws.String(*bucketName + "/" + *key),
				CopySourceRange: aws.String("bytes=0-5"),
			})
			assert.Nil(t, err)
			part2, err := s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:          bucketName,
				Key:             key2,
				UploadId:        uploadId,
				PartNumber:      aws.Int32(2),
				CopySource:      aws.String(*bucketName + "/" + *key),
				CopySourceRange: aws.String("bytes=7-12"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key2,
				UploadId: uploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{ETag: part1.CopyPartResult.ETag, PartNumber: aws.Int32(1)},
						{ETag: part2.CopyPartResult.ETag, PartNumber: aws.Int32(2)},
					},
				},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, []byte("Hello,world!"), objectBytes)
		})

		t.Run("it should honor copy-source-if-match for UploadPartCopy"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			uploadId := createResult.UploadId

			_, err = s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:            bucketName,
				Key:               key2,
				UploadId:          uploadId,
				PartNumber:        aws.Int32(1),
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})
	})
}
