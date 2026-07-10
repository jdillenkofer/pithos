package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"testing"
)

func TestDeleteObject(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should allow deleting an object"+testSuffix, func(t *testing.T) {
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

			deleteObjectResult, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
		})

		t.Run("it should allow deleting an object with a presigned url"+testSuffix, func(t *testing.T) {
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

			presignClient := s3.NewPresignClient(s3Client)
			presignedRequest, err := presignClient.PresignDeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			deleteObjectResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
			assert.Equal(t, 204, deleteObjectResult.StatusCode)
		})

		t.Run("it should return 204 for DeleteObject with matching If-Match"+testSuffix, func(t *testing.T) {
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

			_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: putResult.ETag,
			})
			assert.Nil(t, err)

			// Verify the object is gone.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should return 412 for DeleteObject with mismatched If-Match"+testSuffix, func(t *testing.T) {
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

			_, err = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket:  bucketName,
				Key:     key,
				IfMatch: aws.String("\"does-not-match\""),
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			if assert.ErrorAs(t, err, &apiErr) {
				assert.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
			}

			// Verify the object still exists.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})

		t.Run("it should reject stale If-Match deletes during concurrent writes"+testSuffix, func(t *testing.T) {
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

			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make([]error, 2)

			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, errs[i] = s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
						Bucket:  bucketName,
						Key:     key,
						IfMatch: putResult.ETag,
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

			assert.LessOrEqual(t, successCount, 1, "at most one concurrent If-Match delete may succeed")
		})
	})
}

func TestDeleteObjects(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should delete multiple existing objects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key2, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
						{Key: key2},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 2)
			assert.Len(t, result.Errors, 0)

			// Objects must no longer exist
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key2})
			assert.NotNil(t, err)
		})

		t.Run("it should apply per-entry delete authorization hook"+testSuffix, func(t *testing.T) {
			authorizationCode := `
			function authorizeRequest(request)
			  return true
			end

			function authorizeDeleteObjectEntry(request, key)
			  return key ~= "blocked.txt"
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

			allowedKey := aws.String("allowed.txt")
			blockedKey := aws.String("blocked.txt")

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: allowedKey, Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: blockedKey, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: allowedKey}, {Key: blockedKey}}},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 1)
			assert.Equal(t, "blocked.txt", *result.Errors[0].Key)
			assert.Equal(t, "AccessDenied", *result.Errors[0].Code)

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: allowedKey})
			assert.NotNil(t, err)
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: blockedKey})
			assert.Nil(t, err)
		})

		t.Run("it should treat a missing key as successfully deleted"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			nonExistentKey := aws.String("does/not/exist.txt")
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: nonExistentKey},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
		})

		t.Run("it should suppress deleted entries in quiet mode"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			quiet := true
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
					Quiet: &quiet,
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 0)

			// Object must still have been deleted
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should delete an explicit object VersionId"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			firstPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, firstPut.VersionId)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("second")),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, VersionId: firstPut.VersionId},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
			assert.Equal(t, *key, *result.Deleted[0].Key)
			assert.NotNil(t, result.Deleted[0].VersionId)
			assert.Equal(t, *firstPut.VersionId, *result.Deleted[0].VersionId)

			// Latest version must still exist.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})

		t.Run("it should return DeleteMarkerVersionId for key-only deletes on versioned buckets"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)
			assert.Equal(t, *key, *result.Deleted[0].Key)
			if assert.NotNil(t, result.Deleted[0].DeleteMarker) {
				assert.True(t, *result.Deleted[0].DeleteMarker)
			}
			assert.Nil(t, result.Deleted[0].VersionId)
			if assert.NotNil(t, result.Deleted[0].DeleteMarkerVersionId) {
				assert.NotEmpty(t, *result.Deleted[0].DeleteMarkerVersionId)
			}

			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
			var responseErr *awshttp.ResponseError
			if assert.True(t, errors.As(err, &responseErr)) {
				assert.Equal(t, http.StatusNotFound, responseErr.HTTPStatusCode())
				assert.Equal(t, "true", responseErr.HTTPResponse().Header.Get("x-amz-delete-marker"))
				assert.Equal(t, aws.ToString(result.Deleted[0].DeleteMarkerVersionId), responseErr.HTTPResponse().Header.Get("x-amz-version-id"))
			}
		})

		t.Run("it should handle duplicate keys with different VersionIds"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket: bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			assert.Nil(t, err)

			put1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, put1.VersionId)

			put2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, put2.VersionId)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: key, VersionId: put1.VersionId}, {Key: key, VersionId: put2.VersionId}}},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Errors, 0)
			assert.Len(t, result.Deleted, 2)

			deletedVersions := map[string]bool{}
			for _, deleted := range result.Deleted {
				if deleted.VersionId != nil {
					deletedVersions[*deleted.VersionId] = true
				}
			}
			assert.True(t, deletedVersions[*put1.VersionId])
			assert.True(t, deletedVersions[*put2.VersionId])
		})

		t.Run("it should return 404 when the bucket does not exist"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
					},
				},
			})
			assert.NotNil(t, err)
			var apiErr smithy.APIError
			assert.True(t, errors.As(err, &apiErr))
			assert.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
		})

		t.Run("it should handle an empty delete list"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 0)
		})

		t.Run("it should partially succeed when some keys exist and some do not"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			nonExistentKey := aws.String("does/not/exist.txt")
			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key},
						{Key: nonExistentKey},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 2)
			assert.Len(t, result.Errors, 0)

			// Existing object must be gone
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should delete object when ETag matches in DeleteObjects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			putResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)
			assert.NotNil(t, putResult.ETag)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, ETag: putResult.ETag},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 1)
			assert.Len(t, result.Errors, 0)

			// Object must be gone.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should return an error entry when ETag mismatches in DeleteObjects"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName, Key: key, Body: bytes.NewReader(body),
			})
			assert.Nil(t, err)

			result, err := s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
				Bucket: bucketName,
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{Key: key, ETag: aws.String("\"does-not-match\"")},
					},
				},
			})
			assert.Nil(t, err)
			assert.Len(t, result.Deleted, 0)
			assert.Len(t, result.Errors, 1)
			assert.Equal(t, *key, *result.Errors[0].Key)
			assert.Equal(t, "PreconditionFailed", *result.Errors[0].Code)

			// Object must still exist.
			_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
		})
	})
}
