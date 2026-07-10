package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestGetObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow downloading the object"+testSuffix, func(t *testing.T) {
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
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *getObjectResult.ETag)
			assert.Equal(t, "6+bG5g==", *getObjectResult.ChecksumCRC32)
			assert.Equal(t, "yKEG5Q==", *getObjectResult.ChecksumCRC32C)
			assert.Equal(t, "n3hVaQAaPTQ=", *getObjectResult.ChecksumCRC64NVME)
			assert.Equal(t, "lDpwLQbzRZmu4fjajvn3KWAx1pk=", *getObjectResult.ChecksumSHA1)
			assert.Equal(t, "MV9b23bQeMQ7isAGTkoBZGErH853yGk0W/yUx1iU7dM=", *getObjectResult.ChecksumSHA256)
			assert.Equal(t, types.ChecksumTypeFullObject, getObjectResult.ChecksumType)
		})

		t.Run("it should allow downloading the object with a presigned url"+testSuffix, func(t *testing.T) {
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

			presignClient := s3.NewPresignClient(s3Client)
			presignedRequest, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			getObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.Equal(t, 200, getObjectResult.StatusCode)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow downloading the object with byte range"+testSuffix, func(t *testing.T) {
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

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:5], objectBytes)
		})

		t.Run("it should allow downloading the object with byte range without end"+testSuffix, func(t *testing.T) {
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

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:], objectBytes)
		})

		t.Run("it should allow downloading the object with suffix byte range"+testSuffix, func(t *testing.T) {
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

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[7:], objectBytes)
		})

		t.Run("it should return 206 and clamp range end when it exceeds object size"+testSuffix, func(t *testing.T) {
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

			// Request a range whose end far exceeds the object size.
			// Per RFC 7233 §2.1 the server must return 206 with the available bytes,
			// not 416 Range Not Satisfiable.
			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=0-5242879"),
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

		t.Run("it should allow downloading the object with multi byte range"+testSuffix, func(t *testing.T) {
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

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4, 5-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.ContentType)
			assert.Contains(t, *getObjectResult.ContentType, "multipart/byteranges; boundary=")
			boundary, _ := strings.CutPrefix(*getObjectResult.ContentType, "multipart/byteranges; boundary=")
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)

			newContent := []byte{}
			/*
				+ 00000000  2d 2d 30 31 48 58 45 52  50 59 56 4a 39 41 47 56  |--01HXERPYVJ9AGV|
				+ 00000010  44 42 34 57 4a 41 44 4a  43 56 59 4e 0d 0a 43 6f  |DB4WJADJCVYN..Co|
				+ 00000020  6e 74 65 6e 74 2d 52 61  6e 67 65 3a 20 62 79 74  |ntent-Range: byt|
				+ 00000030  65 73 20 31 2d 34 2f 31  33 0d 0a 0d 0a 65 6c 6c  |es 1-4/13....ell|
				+ 00000040  6f 0d 0a 2d 2d 30 31 48  58 45 52 50 59 56 4a 39  |o..--01HXERPYVJ9|
				+ 00000050  41 47 56 44 42 34 57 4a  41 44 4a 43 56 59 4e 0d  |AGVDB4WJADJCVYN.|
				+ 00000060  0a 43 6f 6e 74 65 6e 74  2d 52 61 6e 67 65 3a 20  |.Content-Range: |
				+ 00000070  62 79 74 65 73 20 35 2d  36 2f 31 33 0d 0a 0d 0a  |bytes 5-6/13....|
				+ 00000080  2c 20 0d 0a 2d 2d 30 31  48 58 45 52 50 59 56 4a  |, ..--01HXERPYVJ|
				+ 00000090  39 41 47 56 44 42 34 57  4a 41 44 4a 43 56 59 4e  |9AGVDB4WJADJCVYN|
				+ 000000a0  2d 2d 0d 0a                                       |--..|
			*/

			newContent = append(newContent, []byte("--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 1-4/13\r\n\r\n")...)
			newContent = append(newContent, body[1:5]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 5-6/13\r\n\r\n")...)
			newContent = append(newContent, body[5:7]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("--\r\n")...)
			assert.Equal(t, newContent, objectBytes)
		})

		t.Run("it should return 200 for GetObject with matching If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)
		})

		t.Run("it should return 412 for GetObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should return an error for GetObject with matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: putResult.ETag,
			})
			// 304 Not Modified surfaces as an error from the SDK (no body to deserialize).
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 304, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return 200 for GetObject with non-matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: aws.String("\"does-not-match\""),
			})
			assert.Nil(t, err)
		})
	})
}

func TestCopyObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		setupSourceObject := func(s3Client *s3.Client, contentType *string) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:      bucketName,
				Key:         key,
				Body:        bytes.NewReader(body),
				ContentType: contentType,
			})
			assert.Nil(t, err)
		}

		t.Run("it should copy an object preserving content, etag and content type"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			copyResult, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)
			assert.NotNil(t, copyResult.CopyObjectResult)
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *copyResult.CopyObjectResult.ETag)
			assert.NotNil(t, copyResult.CopyObjectResult.LastModified)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "\"6cd3556deb0da54bca060b4c39479839\"", *getResult.ETag)
			assert.Equal(t, "text/plain", *getResult.ContentType)
		})

		t.Run("it should copy an object across buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName2})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName2,
				Key:        key,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName2, Key: key})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should replace the content type when metadata directive is REPLACE"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				ContentType:       aws.String("application/json"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "application/json", *getResult.ContentType)
		})

		t.Run("it should honor copy-source-if-match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"6cd3556deb0da54bca060b4c39479839\""),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				CopySourceIfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should fail copy-source-if-none-match when the etag matches"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:                bucketName,
				Key:                   key2,
				CopySource:            aws.String(*bucketName + "/" + *key),
				CopySourceIfNoneMatch: aws.String("\"6cd3556deb0da54bca060b4c39479839\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}
		})

		t.Run("it should return NoSuchKey when the source does not exist"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/does-not-exist"),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "NoSuchKey", apiErr.ErrorCode())
			}
		})

		t.Run("it should reject a no-op self copy"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, nil)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "InvalidRequest", apiErr.ErrorCode())
			}
		})

		t.Run("it should allow a self copy that replaces metadata"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupSourceObject(s3Client, aws.String("text/plain"))

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				ContentType:       aws.String("application/json"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			objectBytes, err := io.ReadAll(getResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
			assert.Equal(t, "application/json", *getResult.ContentType)
		})

		t.Run("it should forbid a copy when the destination write is denied"+testSuffix, func(t *testing.T) {
			// Allow everything except the copy operation itself.
			authorizationCode := `
			function authorizeRequest(request)
			  return request.operation ~= "CopyObject"
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
			// Authorization is checked before any storage access, so the copy is
			// rejected with 403 regardless of whether the source exists.
			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 403, httpErr.Response.StatusCode)
			}
		})
	})
}

func TestObjectMetadata(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		expires := time.Date(2026, time.October, 21, 7, 28, 0, 0, time.UTC)
		putObjectWithMetadata := func(s3Client *s3.Client) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:                  bucketName,
				Key:                     key,
				Body:                    bytes.NewReader(body),
				ContentType:             aws.String("text/plain"),
				CacheControl:            aws.String("max-age=3600"),
				ContentDisposition:      aws.String(`attachment; filename="hello.txt"`),
				ContentEncoding:         aws.String("identity"),
				ContentLanguage:         aws.String("en-US"),
				Expires:                 aws.Time(expires),
				WebsiteRedirectLocation: aws.String("/redirected.html"),
				Metadata:                map[string]string{"Purpose": "testing", "owner": "storage-team"},
			})
			assert.Nil(t, err)
		}

		assertMetadata := func(t *testing.T, cacheControl, contentDisposition, contentEncoding, contentLanguage, expiresString, websiteRedirectLocation *string, userMetadata map[string]string) {
			assert.NotNil(t, cacheControl)
			assert.Equal(t, "max-age=3600", *cacheControl)
			assert.NotNil(t, contentDisposition)
			assert.Equal(t, `attachment; filename="hello.txt"`, *contentDisposition)
			assert.NotNil(t, contentEncoding)
			assert.Equal(t, "identity", *contentEncoding)
			assert.NotNil(t, contentLanguage)
			assert.Equal(t, "en-US", *contentLanguage)
			assert.NotNil(t, expiresString)
			parsedExpires, err := http.ParseTime(*expiresString)
			assert.Nil(t, err)
			assert.True(t, expires.Equal(parsedExpires))
			assert.NotNil(t, websiteRedirectLocation)
			assert.Equal(t, "/redirected.html", *websiteRedirectLocation)
			// S3 stores user metadata keys lowercase.
			assert.Equal(t, map[string]string{"purpose": "testing", "owner": "storage-team"}, userMetadata)
		}

		t.Run("it should persist and return metadata on HeadObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assertMetadata(t, headResult.CacheControl, headResult.ContentDisposition, headResult.ContentEncoding, headResult.ContentLanguage, headResult.ExpiresString, headResult.WebsiteRedirectLocation, headResult.Metadata)
		})

		t.Run("it should return metadata on GetObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)
			getResult.Body.Close()
			assertMetadata(t, getResult.CacheControl, getResult.ContentDisposition, getResult.ContentEncoding, getResult.ContentLanguage, getResult.ExpiresString, getResult.WebsiteRedirectLocation, getResult.Metadata)
		})

		t.Run("it should replace metadata when the object is overwritten"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Nil(t, headResult.CacheControl)
			assert.Nil(t, headResult.ContentDisposition)
			assert.Nil(t, headResult.ContentEncoding)
			assert.Nil(t, headResult.ContentLanguage)
			assert.Nil(t, headResult.ExpiresString)
			assert.Nil(t, headResult.WebsiteRedirectLocation)
			assert.Len(t, headResult.Metadata, 0)
		})

		t.Run("it should preserve metadata across multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			createResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:                  bucketName,
				Key:                     key,
				CacheControl:            aws.String("max-age=3600"),
				ContentDisposition:      aws.String(`attachment; filename="hello.txt"`),
				ContentEncoding:         aws.String("identity"),
				ContentLanguage:         aws.String("en-US"),
				Expires:                 aws.Time(expires),
				WebsiteRedirectLocation: aws.String("/redirected.html"),
				Metadata:                map[string]string{"purpose": "testing", "owner": "storage-team"},
			})
			assert.Nil(t, err)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createResult.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createResult.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: uploadPartResult.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assertMetadata(t, headResult.CacheControl, headResult.ContentDisposition, headResult.ContentEncoding, headResult.ContentLanguage, headResult.ExpiresString, headResult.WebsiteRedirectLocation, headResult.Metadata)
		})

		t.Run("it should copy metadata by default on CopyObject except the website redirect location"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.NotNil(t, headResult.CacheControl)
			assert.Equal(t, "max-age=3600", *headResult.CacheControl)
			assert.Equal(t, map[string]string{"purpose": "testing", "owner": "storage-team"}, headResult.Metadata)
			// S3 never carries x-amz-website-redirect-location over to the copy.
			assert.Nil(t, headResult.WebsiteRedirectLocation)
		})

		t.Run("it should replace metadata when metadata directive is REPLACE on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			putObjectWithMetadata(s3Client)

			_, err := s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:            bucketName,
				Key:               key2,
				CopySource:        aws.String(*bucketName + "/" + *key),
				MetadataDirective: types.MetadataDirectiveReplace,
				CacheControl:      aws.String("no-store"),
				Metadata:          map[string]string{"replaced": "yes"},
			})
			assert.Nil(t, err)

			headResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.NotNil(t, headResult.CacheControl)
			assert.Equal(t, "no-store", *headResult.CacheControl)
			assert.Nil(t, headResult.ContentDisposition)
			assert.Equal(t, map[string]string{"replaced": "yes"}, headResult.Metadata)
		})

		t.Run("it should reject user metadata over the 2 KB limit"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:   bucketName,
				Key:      key,
				Body:     bytes.NewReader(body),
				Metadata: map[string]string{"big": strings.Repeat("v", storage.MaxUserMetadataSize)},
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "MetadataTooLarge", apiErr.ErrorCode())
		})
	})
}

func TestHeadObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow head an object"+testSuffix, func(t *testing.T) {
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

			headObjectResult, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "HeadObject failed", "err %v", err)
			}
			assert.NotNil(t, headObjectResult)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *headObjectResult.ETag)
			assert.Equal(t, "Bjck3A==", *headObjectResult.ChecksumCRC32)
			assert.Equal(t, "H7cZCA==", *headObjectResult.ChecksumCRC32C)
			assert.Equal(t, "4SEgZkEEyhY=", *headObjectResult.ChecksumCRC64NVME)
			assert.Equal(t, "lMCBYNtqPCnP3avKVUtqfrThqHo=", *headObjectResult.ChecksumSHA1)
			assert.Equal(t, "sctyzI/H+7x/oVR7Gwt7NiQ7kop4Ua/7SrVraELVDpI=", *headObjectResult.ChecksumSHA256)
			assert.Equal(t, types.ChecksumTypeFullObject, headObjectResult.ChecksumType)
		})

		t.Run("it should return 200 for HeadObject with matching If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)
		})

		t.Run("it should return 412 for HeadObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			// HeadObject 412 has no XML body, so the SDK surfaces it as an HTTP response error only.
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 412, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return an error for HeadObject with matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: putResult.ETag,
			})
			// 304 Not Modified surfaces as an error from the SDK (no body to deserialize).
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 304, httpErr.Response.StatusCode)
			}
		})

		t.Run("it should return 200 for HeadObject with non-matching If-None-Match"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
			})
			assert.Nil(t, err)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:      bucketName,
				Key:         key,
				IfNoneMatch: aws.String("\"does-not-match\""),
			})
			assert.Nil(t, err)
		})
	})
}

func TestListObjects(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		// Test ListObjectsV1 (deprecated but still supported)
		t.Run("it should list no objects V1"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list a single object V1"+testSuffix, func(t *testing.T) {
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

			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate when listing objects V1"+testSuffix, func(t *testing.T) {
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
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=1
			listObjectResult, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(1),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.True(t, *listObjectResult.IsTruncated)

			// For ListObjects V1, NextMarker should be set when truncated
			assert.NotNil(t, listObjectResult.NextMarker)
			assert.NotEmpty(t, *listObjectResult.NextMarker)

			// Get next page using Marker
			listObjectResult2, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
				Bucket: bucketName,
				Marker: listObjectResult.NextMarker,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V1 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.False(t, *listObjectResult2.IsTruncated)

			// Verify we got different objects
			assert.NotEqual(t, *listObjectResult.Contents[0].Key, *listObjectResult2.Contents[0].Key)
		})

		t.Run("it should list a single object V2"+testSuffix, func(t *testing.T) {
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

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult.KeyCount)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate and paginate with continuation token V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create multiple objects for pagination testing
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
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Create third object for pagination
			key3 := aws.String(*keyPrefix + "/hello_world3.txt")
			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, third object!")),
				Key:    key3,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=2
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 2)
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.True(t, *listObjectResult.IsTruncated)
			assert.NotEmpty(t, *listObjectResult.NextContinuationToken)

			// Get next page using ContinuationToken
			listObjectResult2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: listObjectResult.NextContinuationToken,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult2.KeyCount)
			assert.False(t, *listObjectResult2.IsTruncated)
			assert.Equal(t, *listObjectResult.NextContinuationToken, *listObjectResult2.ContinuationToken)

			// Verify all objects are unique
			allKeys := make(map[string]bool)
			for _, obj := range listObjectResult.Contents {
				allKeys[*obj.Key] = true
			}
			for _, obj := range listObjectResult2.Contents {
				if allKeys[*obj.Key] {
					assert.Fail(t, "Duplicate key found in pagination", "key: %s", *obj.Key)
				}
				allKeys[*obj.Key] = true
			}
			assert.Len(t, allKeys, 3) // Should have 3 unique keys
		})

		t.Run("it should paginate filtered list objects via authorizer hook V2"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeListObject(request, key)
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
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			keys := []string{
				"allowed/a.txt",
				"denied/a.txt",
				"allowed/b.txt",
				"denied/b.txt",
				"allowed/c.txt",
				"denied/c.txt",
			}
			for i, keyName := range keys {
				_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("obj-%d", i))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
			}

			page1, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 page1 failed", "err %v", err)
			}
			assert.Len(t, page1.Contents, 2)
			assert.True(t, *page1.IsTruncated)
			assert.NotNil(t, page1.NextContinuationToken)

			for _, object := range page1.Contents {
				assert.True(t, strings.HasPrefix(*object.Key, "allowed/"))
			}

			page2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: page1.NextContinuationToken,
				MaxKeys:           aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 page2 failed", "err %v", err)
			}
			assert.Len(t, page2.Contents, 1)
			assert.False(t, *page2.IsTruncated)

			for _, object := range page2.Contents {
				assert.True(t, strings.HasPrefix(*object.Key, "allowed/"))
			}

			allKeys := map[string]bool{}
			for _, object := range page1.Contents {
				allKeys[*object.Key] = true
			}
			for _, object := range page2.Contents {
				allKeys[*object.Key] = true
			}
			assert.Len(t, allKeys, 3)
			assert.True(t, allKeys["allowed/a.txt"])
			assert.True(t, allKeys["allowed/b.txt"])
			assert.True(t, allKeys["allowed/c.txt"])
		})

		t.Run("it should handle StartAfter parameter V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create objects with predictable lexicographic order
			keys := []string{
				"my/test/key/a.txt",
				"my/test/key/b.txt",
				"my/test/key/c.txt",
			}

			for i, keyName := range keys {
				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("Hello, object %d!", i+1))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
			}

			// Test StartAfter functionality
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:     bucketName,
				StartAfter: aws.String("my/test/key/a.txt"), // Should skip a.txt
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 with StartAfter failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.Contents, 2) // Should get b.txt and c.txt
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.False(t, *listObjectResult.IsTruncated)

			// Verify the returned objects are correct
			assert.Equal(t, "my/test/key/b.txt", *listObjectResult.Contents[0].Key)
			assert.Equal(t, "my/test/key/c.txt", *listObjectResult.Contents[1].Key)
		})

		// Continue with existing tests for prefixes and delimiters...
		t.Run("it should list objects with prefix and delimiter V2"+testSuffix, func(t *testing.T) {
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
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    aws.String("my.txt"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)
			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, "my.txt", *listObjectResult.Contents[0].Key)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		// Test ListObjectsV2 (recommended)
		t.Run("it should list no objects V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
			assert.Equal(t, int32(0), *listObjectResult.KeyCount)
		})

		t.Run("it should list a single object V2"+testSuffix, func(t *testing.T) {
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

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult.KeyCount)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate and paginate with continuation token V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create multiple objects for pagination testing
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
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Create third object for pagination
			key3 := aws.String(*keyPrefix + "/hello_world3.txt")
			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, third object!")),
				Key:    key3,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			// Test pagination with MaxKeys=2
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: aws.Int32(2),
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 2)
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.True(t, *listObjectResult.IsTruncated)
			assert.NotEmpty(t, *listObjectResult.NextContinuationToken)

			// Get next page using ContinuationToken
			listObjectResult2, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:            bucketName,
				ContinuationToken: listObjectResult.NextContinuationToken,
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 pagination failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult2.Name)
			assert.Len(t, listObjectResult2.CommonPrefixes, 0)
			assert.Len(t, listObjectResult2.Contents, 1)
			assert.Equal(t, int32(1), *listObjectResult2.KeyCount)
			assert.False(t, *listObjectResult2.IsTruncated)
			assert.Equal(t, *listObjectResult.NextContinuationToken, *listObjectResult2.ContinuationToken)

			// Verify all objects are unique
			allKeys := make(map[string]bool)
			for _, obj := range listObjectResult.Contents {
				allKeys[*obj.Key] = true
			}
			for _, obj := range listObjectResult2.Contents {
				if allKeys[*obj.Key] {
					assert.Fail(t, "Duplicate key found in pagination", "key: %s", *obj.Key)
				}
				allKeys[*obj.Key] = true
			}
			assert.Len(t, allKeys, 3) // Should have 3 unique keys
		})

		t.Run("it should handle StartAfter parameter V2"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			// Create objects with predictable lexicographic order
			keys := []string{
				"my/test/key/a.txt",
				"my/test/key/b.txt",
				"my/test/key/c.txt",
			}

			for i, keyName := range keys {
				putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: bucketName,
					Body:   bytes.NewReader([]byte(fmt.Sprintf("Hello, object %d!", i+1))),
					Key:    aws.String(keyName),
				})
				if err != nil {
					assert.Fail(t, "PutObject failed", "err %v", err)
				}
				assert.NotNil(t, putObjectResult)
			}

			// Test StartAfter functionality
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:     bucketName,
				StartAfter: aws.String("my/test/key/a.txt"), // Should skip a.txt
			})
			if err != nil {
				assert.Fail(t, "ListObjects V2 with StartAfter failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.Contents, 2) // Should get b.txt and c.txt
			assert.Equal(t, int32(2), *listObjectResult.KeyCount)
			assert.False(t, *listObjectResult.IsTruncated)

			// Verify the returned objects are correct
			assert.Equal(t, "my/test/key/b.txt", *listObjectResult.Contents[0].Key)
			assert.Equal(t, "my/test/key/c.txt", *listObjectResult.Contents[1].Key)
		})
	})
}
