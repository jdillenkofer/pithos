package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestCreateBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should create a bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			assert.Equal(t, bucketName, createBucketResult.Location)
		})

		t.Run("it should not be able to create the same bucket twice"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})

			if err == nil {
				assert.Fail(t, "CreateBucket should failed when reusing the same bucket name")
			}

			var bucketAlreadyExistsError *types.BucketAlreadyExists
			if !errors.As(err, &bucketAlreadyExistsError) {
				assert.Fail(t, "Expected aws error BucketAlreadyExists", "err %v", err)
			}
		})
	})
}

func TestHeadBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should be able to see an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			assert.NotNil(t, createBucketResult)
			headBucketResult, err := s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			if err != nil {
				assert.Fail(t, "HeadBucket failed", "err %v", err)
			}
			assert.NotNil(t, headBucketResult)
		})
	})
}

func TestListBuckets(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should be able to list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listBucketsResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})

			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}

			assert.Len(t, listBucketsResult.Buckets, 1)
			assert.Equal(t, bucketName, listBucketsResult.Buckets[0].Name)
			assert.NotNil(t, listBucketsResult.Buckets[0].CreationDate)
			assert.True(t, listBucketsResult.Buckets[0].CreationDate.Before(time.Now()))
		})

		t.Run("it should list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createBucketResult2, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName2,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult2)

			listBucketResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}
			assert.Len(t, listBucketResult.Buckets, 2)
		})
	})
}

func TestDeleteBucket(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should delete an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
		})

		t.Run("it should delete an existing bucket with a presigned url"+testSuffix, func(t *testing.T) {
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
			presignedRequest, err := presignClient.PresignDeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			assert.Nil(t, err)

			httpClient := buildHttpClient()
			request, err := http.NewRequest(presignedRequest.Method, presignedRequest.URL, nil)
			assert.Nil(t, err)
			deleteBucketResult, err := httpClient.Do(request)
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
			assert.Equal(t, 204, deleteBucketResult.StatusCode)
		})

		t.Run("it should fail when deleting non existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String("test2"),
			})

			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucket" {
				assert.Fail(t, "Expected aws error NoSuchBucket", "err %v", err)
			}
		})

		t.Run("it should not see the bucket after deletion anymore"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)

			_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			var notFoundError *types.NotFound
			if !errors.As(err, &notFoundError) {
				assert.Fail(t, "Expected aws error NotFound", "err %v", err)
			}
		})

		t.Run("it should not allow deleting a bucket with objects in it"+testSuffix, func(t *testing.T) {
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

			_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "BucketNotEmpty" {
				assert.Fail(t, "Expected aws error BucketNotEmpty", "err %v", err)
			}
		})
	})
}

func TestBucketVersioning(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		t.Run("it should keep prior versions and reuse null version after suspend"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))

			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			putNull1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v3"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull1.VersionId))

			putNull2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v4"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull2.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)

			versionIDs := map[string]bool{}
			nullVersionCount := 0
			nullLatest := false
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) != aws.ToString(key) {
					continue
				}
				versionID := aws.ToString(version.VersionId)
				versionIDs[versionID] = true
				if versionID == "null" {
					nullVersionCount++
					nullLatest = aws.ToBool(version.IsLatest)
				}
			}

			assert.True(t, versionIDs[aws.ToString(putV1.VersionId)])
			assert.True(t, versionIDs[aws.ToString(putV2.VersionId)])
			assert.True(t, versionIDs["null"])
			assert.Equal(t, 1, nullVersionCount)
			assert.True(t, nullLatest)
		})

		t.Run("it should resume unique version ids after re-enabling versioning"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			putNull1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("sv1"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull1.VersionId))

			putNull2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("sv2"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(putNull2.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("ev1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))
			assert.NotEqual(t, "null", aws.ToString(putV1.VersionId))

			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("ev2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, "null", aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)

			versionIDs := map[string]bool{}
			latestByID := map[string]bool{}
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) != aws.ToString(key) {
					continue
				}
				versionID := aws.ToString(version.VersionId)
				versionIDs[versionID] = true
				latestByID[versionID] = aws.ToBool(version.IsLatest)
			}

			assert.True(t, versionIDs["null"])
			assert.True(t, versionIDs[aws.ToString(putV1.VersionId)])
			assert.True(t, versionIDs[aws.ToString(putV2.VersionId)])
			assert.True(t, latestByID[aws.ToString(putV2.VersionId)])
		})

		t.Run("it should keep api status and put semantics across enable suspend re-enable transitions"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err := s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			enabledPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("enabled"))})
			assert.Nil(t, err)
			assert.NotNil(t, enabledPut.VersionId)
			assert.NotEmpty(t, aws.ToString(enabledPut.VersionId))
			assert.NotEqual(t, "null", aws.ToString(enabledPut.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusSuspended, versioningOut.Status)

			suspendedPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("suspended"))})
			assert.Nil(t, err)
			assert.Equal(t, "null", aws.ToString(suspendedPut.VersionId))

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			versioningOut, err = s3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOut.Status)

			reenabledPut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("reenabled"))})
			assert.Nil(t, err)
			assert.NotNil(t, reenabledPut.VersionId)
			assert.NotEmpty(t, aws.ToString(reenabledPut.VersionId))
			assert.NotEqual(t, "null", aws.ToString(reenabledPut.VersionId))
			assert.NotEqual(t, aws.ToString(enabledPut.VersionId), aws.ToString(reenabledPut.VersionId))
		})

		t.Run("it should return delete-marker semantics for current and explicit versions"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, listenerAddr, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putOut, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putOut.VersionId)
			assert.NotEmpty(t, aws.ToString(putOut.VersionId))

			deleteOut, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucketName, Key: key})
			if err != nil {
				t.Fatalf("DeleteObject failed: %v", err)
			}
			markerVersionID := ""
			if deleteOut != nil && deleteOut.VersionId != nil {
				markerVersionID = *deleteOut.VersionId
			}

			addr, _ := net.ResolveTCPAddr("tcp", listenerAddr)
			httpClient := buildHttpClient()
			objectURL := fmt.Sprintf("http://%s:%d/%s/%s", testAPIEndpoint, addr.Port, *bucketName, *key)

			headReq, _ := http.NewRequest(http.MethodHead, objectURL, nil)
			headResp, err := httpClient.Do(headReq)
			assert.Nil(t, err)
			defer headResp.Body.Close()
			assert.Equal(t, http.StatusNotFound, headResp.StatusCode)
			assert.Equal(t, "true", headResp.Header.Get("x-amz-delete-marker"))
			if markerVersionID == "" {
				markerVersionID = headResp.Header.Get("x-amz-version-id")
			}
			if markerVersionID == "" {
				t.Fatalf("missing delete marker version id in DeleteObject output and HEAD response")
			}
			assert.Equal(t, markerVersionID, headResp.Header.Get("x-amz-version-id"))

			getReq, _ := http.NewRequest(http.MethodGet, objectURL, nil)
			getResp, err := httpClient.Do(getReq)
			assert.Nil(t, err)
			defer getResp.Body.Close()
			assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
			assert.Equal(t, "true", getResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, getResp.Header.Get("x-amz-version-id"))

			headVersionReq, _ := http.NewRequest(http.MethodHead, objectURL+"?versionId="+markerVersionID, nil)
			headVersionResp, err := httpClient.Do(headVersionReq)
			assert.Nil(t, err)
			defer headVersionResp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, headVersionResp.StatusCode)
			assert.Equal(t, "true", headVersionResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, headVersionResp.Header.Get("x-amz-version-id"))
			assert.NotEmpty(t, headVersionResp.Header.Get("Last-Modified"))

			getVersionReq, _ := http.NewRequest(http.MethodGet, objectURL+"?versionId="+markerVersionID, nil)
			getVersionResp, err := httpClient.Do(getVersionReq)
			assert.Nil(t, err)
			defer getVersionResp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, getVersionResp.StatusCode)
			assert.Equal(t, "true", getVersionResp.Header.Get("x-amz-delete-marker"))
			assert.Equal(t, markerVersionID, getVersionResp.Header.Get("x-amz-version-id"))
			assert.NotEmpty(t, getVersionResp.Header.Get("Last-Modified"))
		})

		t.Run("it should return version id for completed multipart upload in versioned bucket"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			createOut, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.NotNil(t, createOut)
			assert.NotNil(t, createOut.UploadId)

			_, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Key:        key,
				UploadId:   createOut.UploadId,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(body),
			})
			assert.Nil(t, err)

			completeOut, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: createOut.UploadId,
			})
			assert.Nil(t, err)
			assert.NotNil(t, completeOut)
			assert.NotNil(t, completeOut.VersionId)
			assert.NotEmpty(t, aws.ToString(completeOut.VersionId))

			versionsOut, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Prefix: key})
			assert.Nil(t, err)
			assert.NotEmpty(t, versionsOut.Versions)

			versionFound := false
			for _, version := range versionsOut.Versions {
				if aws.ToString(version.Key) == aws.ToString(key) && aws.ToString(version.VersionId) == aws.ToString(completeOut.VersionId) {
					versionFound = true
					assert.True(t, aws.ToBool(version.IsLatest))
					break
				}
			}
			assert.True(t, versionFound)
		})

		t.Run("it should paginate ListObjectVersions with key and version markers"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putV1, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v1"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV1.VersionId)
			assert.NotEmpty(t, aws.ToString(putV1.VersionId))
			putV2, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader([]byte("v2"))})
			assert.Nil(t, err)
			assert.NotNil(t, putV2.VersionId)
			assert.NotEmpty(t, aws.ToString(putV2.VersionId))
			assert.NotEqual(t, aws.ToString(putV1.VersionId), aws.ToString(putV2.VersionId))

			page1, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, MaxKeys: aws.Int32(1)})
			assert.Nil(t, err)
			assert.True(t, aws.ToBool(page1.IsTruncated))
			assert.NotNil(t, page1.NextKeyMarker)
			assert.NotNil(t, page1.NextVersionIdMarker)
			assert.Len(t, page1.Versions, 1)

			page2, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, MaxKeys: aws.Int32(1), KeyMarker: page1.NextKeyMarker, VersionIdMarker: page1.NextVersionIdMarker})
			assert.Nil(t, err)
			assert.Len(t, page2.Versions, 1)
			assert.NotEqual(t, aws.ToString(page1.Versions[0].VersionId), aws.ToString(page2.Versions[0].VersionId))
		})

		t.Run("it should count common prefixes in ListObjectVersions pagination"+testSuffix, func(t *testing.T) {
			t.Parallel()
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
				Bucket:                  bucketName,
				VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
			})
			assert.Nil(t, err)

			putA, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: aws.String("a/one.txt"), Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putA.VersionId)
			assert.NotEmpty(t, aws.ToString(putA.VersionId))
			putB, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: aws.String("b/two.txt"), Body: bytes.NewReader(body)})
			assert.Nil(t, err)
			assert.NotNil(t, putB.VersionId)
			assert.NotEmpty(t, aws.ToString(putB.VersionId))

			page1, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
			assert.Nil(t, err)
			assert.True(t, aws.ToBool(page1.IsTruncated))
			assert.Len(t, page1.CommonPrefixes, 1)

			page2, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: bucketName, Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), KeyMarker: page1.NextKeyMarker, VersionIdMarker: page1.NextVersionIdMarker})
			assert.Nil(t, err)
			assert.Len(t, page2.CommonPrefixes, 1)
			assert.NotEqual(t, aws.ToString(page1.CommonPrefixes[0].Prefix), aws.ToString(page2.CommonPrefixes[0].Prefix))
		})
	})
}
