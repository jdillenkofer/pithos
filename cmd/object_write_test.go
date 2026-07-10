package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPutObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow uploading an object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		for _, checksumAlgorithm := range []types.ChecksumAlgorithm{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
			t.Run("it should allow uploading an object with checksumAlgorithm "+string(checksumAlgorithm)+testSuffix, func(t *testing.T) {
				s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
				t.Cleanup(cleanup)
				createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
					Bucket: bucketName,
				})
				if err != nil {
					assert.Fail(t, "CreateBucket failed", "err %v", err)
				}
				assert.NotNil(t, createBucketResult)

				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket:            bucketName,
					Body:              bytes.NewReader([]byte("Hello, first object!")),
					Key:               key,
					ChecksumAlgorithm: checksumAlgorithm,
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
				assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *putObjectResult.ETag)
				assert.Equal(t, "Bjck3A==", *putObjectResult.ChecksumCRC32)
				assert.Equal(t, "H7cZCA==", *putObjectResult.ChecksumCRC32C)
				assert.Equal(t, "4SEgZkEEyhY=", *putObjectResult.ChecksumCRC64NVME)
				assert.Equal(t, "lMCBYNtqPCnP3avKVUtqfrThqHo=", *putObjectResult.ChecksumSHA1)
				assert.Equal(t, "sctyzI/H+7x/oVR7Gwt7NiQ7kop4Ua/7SrVraELVDpI=", *putObjectResult.ChecksumSHA256)
				assert.Equal(t, types.ChecksumTypeFullObject, putObjectResult.ChecksumType)
			})
		}

		t.Run("it should allow uploading an object with a presigned url"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			presignClient := s3.NewPresignClient(s3Client)
			body := []byte("Hello, first object!")
			presignedRequest, err := presignClient.PresignPutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, bytes.NewReader(body))
			assert.Nil(t, err)
			putObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
			assert.Equal(t, 200, putObjectResult.StatusCode)
		})

		t.Run("it should allow uploading an object with a presigned url and preprovided body"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			presignClient := s3.NewPresignClient(s3Client)
			body := []byte("Hello, first object!")
			presignedRequest, err := presignClient.PresignPutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, bytes.NewReader(body))
			request.Header.Add("Content-Type", presignedRequest.SignedHeader.Get("Content-Type"))
			assert.Nil(t, err)
			putObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
			assert.Equal(t, 200, putObjectResult.StatusCode)
		})

		t.Run("it should allow uploading an object a second time"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should allow uploads with if-none-match when object is absent"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader([]byte("Hello, first object!")),
				Key:         key,
				IfNoneMatch: aws.String("*"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should reject uploads with if-none-match when object already exists"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader([]byte("Hello, first object!")),
				Key:         key,
				IfNoneMatch: aws.String("*"),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-None-Match is set and object exists")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject non-star if-none-match values"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader(body),
				Key:         key,
				IfNoneMatch: aws.String("\"invalid-etag\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-None-Match is not '*'")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidRequest" {
				assert.Fail(t, "Expected aws error InvalidRequest", "err %v", err)
			}
		})

		t.Run("it should allow at most one concurrent create-if-absent upload"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]*s3.PutObjectOutput, 2)
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i], errs[i] = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
						Bucket:      bucketName,
						Body:        bytes.NewReader(body),
						Key:         key,
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
					assert.NotNil(t, results[i])
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent create-if-absent upload may succeed")
		})

		t.Run("it should allow if-match when etag matches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			initialPutObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, initialPutObjectResult)
			assert.NotNil(t, initialPutObjectResult.ETag)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: initialPutObjectResult.ETag,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should reject if-match when etag mismatches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match does not match current object ETag")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject if-match when object is missing"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Body:    bytes.NewReader(body),
				Key:     key,
				IfMatch: aws.String("\"missing-etag\""),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match is set but object does not exist")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				assert.Fail(t, "Expected aws error PreconditionFailed", "err %v", err)
			}
		})

		t.Run("it should reject stale if-match updates during concurrent writes"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			initialPutObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, initialPutObjectResult)
			assert.NotNil(t, initialPutObjectResult.ETag)

			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]*s3.PutObjectOutput, 2)
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i], errs[i] = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
						Bucket:  bucketName,
						Body:    bytes.NewReader(body),
						Key:     key,
						IfMatch: initialPutObjectResult.ETag,
					})
				}(i)
			}

			close(start)
			wg.Wait()

			successCount := 0
			for i := range 2 {
				if errs[i] == nil {
					successCount++
					assert.NotNil(t, results[i])
					continue
				}

				var apiErr smithy.APIError
				if !errors.As(errs[i], &apiErr) {
					assert.Fail(t, "Expected smithy.APIError", "err %v", errs[i])
					continue
				}
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-Match update with the same stale ETag may succeed")
		})

		t.Run("it should reject putobject when if-match and if-none-match are both set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Body:        bytes.NewReader(body),
				Key:         key,
				IfMatch:     aws.String("\"etag\""),
				IfNoneMatch: aws.String("*"),
			})

			if err == nil {
				assert.Fail(t, "PutObject should fail when If-Match and If-None-Match are both set")
			}

			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidRequest" {
				assert.Fail(t, "Expected aws error InvalidRequest", "err %v", err)
			}
		})

		t.Run("it should hit the upload limit when uploading an object that is too large"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Use a LimitReader over a repeating source to simulate a large payload without allocating 5GB of RAM
			largePayload := io.LimitReader(ioutils.NewRepeatingReader([]byte("huge")), storage.MaxEntitySize+1)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   largePayload,
				Key:    key,
			})
			assert.Nil(t, putObjectResult)
			if err == nil {
				assert.Fail(t, "PutObject should have failed due to size limit")
			}
			var smithyOperationError *smithy.OperationError
			if !errors.As(err, &smithyOperationError) {
				assert.Fail(t, "Expected error smithy.OperationError", "err %v", err)
			}
		})
	})
}

func TestPutObjectWithTrailingChecksum(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		payload := []byte("Hello, trailing checksum!")
		checksumBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(checksumBytes, crc32.ChecksumIEEE(payload))
		payloadChecksum := base64.StdEncoding.EncodeToString(checksumBytes)

		// buildTrailerBody encodes the payload in the aws-chunked format used
		// with STREAMING-UNSIGNED-PAYLOAD-TRAILER: a single unsigned chunk,
		// the zero-length chunk, and the checksum trailer.
		buildTrailerBody := func(trailerName string, trailerValue string) string {
			return fmt.Sprintf("%x\r\n%s\r\n0\r\n%s:%s\r\n\r\n", len(payload), payload, trailerName, trailerValue)
		}

		putWithTrailer := func(t *testing.T, listenerAddr string, declaredTrailer string, body string) *http.Response {
			addr, err := net.ResolveTCPAddr("tcp", listenerAddr)
			require.NoError(t, err)
			url := fmt.Sprintf("http://%s:%d/%s/%s", testAPIEndpoint, addr.Port, *bucketName, *key)
			request, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			request.Header.Set("content-encoding", "aws-chunked")
			request.Header.Set("x-amz-decoded-content-length", strconv.Itoa(len(payload)))
			request.Header.Set("x-amz-trailer", declaredTrailer)

			signer := v4.NewSigner()
			err = signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: accessKeyId, SecretAccessKey: secretAccessKey}, request, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "s3", region, time.Now().UTC())
			require.NoError(t, err)

			response, err := buildHttpClient().Do(request)
			require.NoError(t, err)
			return response
		}

		t.Run("it should accept an aws-chunked upload with a valid trailing checksum"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-crc32", payloadChecksum))
			assert.Equal(t, 200, response.StatusCode)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			require.NoError(t, err)
			data, err := io.ReadAll(getObjectResult.Body)
			require.NoError(t, err)
			assert.Equal(t, payload, data)
		})

		t.Run("it should reject an aws-chunked upload with a corrupted trailing checksum"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-crc32", "AAAAAA=="))
			assert.Equal(t, 400, response.StatusCode)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Contains(t, string(responseBody), "BadDigest")
		})

		t.Run("it should reject an aws-chunked upload whose trailer does not match the declared x-amz-trailer"+testSuffix, func(t *testing.T) {
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			response := putWithTrailer(t, listenerAddr, "x-amz-checksum-crc32", buildTrailerBody("x-amz-checksum-sha256", payloadChecksum))
			assert.Equal(t, 400, response.StatusCode)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Contains(t, string(responseBody), "MalformedTrailerError")
		})
	})
}
