package main

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestObjectTagging(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		setupObject := func(s3Client *s3.Client) {
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Key:    key,
				Body:   bytes.NewReader(body),
			})
			assert.Nil(t, err)
		}

		tagsToMap := func(tagSet []types.Tag) map[string]string {
			m := map[string]string{}
			for _, tag := range tagSet {
				m[*tag.Key] = *tag.Value
			}
			return m
		}

		t.Run("it should put and get an object tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket: bucketName,
				Key:    key,
				Tagging: &types.Tagging{TagSet: []types.Tag{
					{Key: aws.String("env"), Value: aws.String("prod")},
					{Key: aws.String("team"), Value: aws.String("storage")},
				}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod", "team": "storage"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should replace an existing tag set on a subsequent put"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("b"), Value: aws.String("2")}}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"b": "2"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should clear tags when putting an empty tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{}},
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Len(t, getResult.TagSet, 0)
		})

		t.Run("it should delete the tag set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			setupObject(s3Client)

			_, err := s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Len(t, getResult.TagSet, 0)
		})

		t.Run("it should return NoSuchKey for tagging of a missing object"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.NotNil(t, err)
		})

		t.Run("it should set tags from the x-amz-tagging header on PutObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)

			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod&team=storage"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod", "team": "storage"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should expose the tag count on GetObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("a=1&b=2"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)
			assert.NotNil(t, getResult.TagCount)
			assert.Equal(t, int32(2), *getResult.TagCount)
		})

		t.Run("it should copy tags by default on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:     bucketName,
				Key:        key2,
				CopySource: aws.String(*bucketName + "/" + *key),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, tagsToMap(getResult.TagSet))
		})

		t.Run("it should replace tags when tagging directive is REPLACE on CopyObject"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)
			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  bucketName,
				Key:     key,
				Body:    bytes.NewReader(body),
				Tagging: aws.String("env=prod"),
			})
			assert.Nil(t, err)

			_, err = s3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
				Bucket:           bucketName,
				Key:              key2,
				CopySource:       aws.String(*bucketName + "/" + *key),
				TaggingDirective: types.TaggingDirectiveReplace,
				Tagging:          aws.String("env=staging&region=eu"),
			})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key2})
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"env": "staging", "region": "eu"}, tagsToMap(getResult.TagSet))
		})
	})
}

func TestTagBasedAuthorization(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	t.Parallel()

	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle bool, useReplication bool, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox bool, usePartStoreCompression bool) {
		authorizationCode := `
		function authorizeRequest(request)
		  if request:isOperation("PutObjectTagging") then
		    return request:requestTagEquals("team", "storage")
		  end
		  if request:isOperation("GetObject") then
		    return request:objectTagEquals("team", "storage")
		  end
		  return true
		end

		function authorizeListObject(request, key)
		  return request:objectTagEquals("team", "storage")
		end
		`
		newAuthorizer := func() authorization.RequestAuthorizer {
			ra, err := lua.NewLuaAuthorizer(authorizationCode)
			if err != nil {
				t.Fatalf("Could not create LuaAuthorizer: %v", err)
			}
			return ra
		}

		assertForbidden := func(t *testing.T, err error) {
			assert.NotNil(t, err)
			var httpErr *awshttp.ResponseError
			if assert.ErrorAs(t, err, &httpErr) {
				assert.Equal(t, 403, httpErr.Response.StatusCode)
			}
		}

		t.Run("GetObject is allowed only for the matching existing object tag"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body), Tagging: aws.String("team=storage")})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key2, Body: bytes.NewReader(body), Tagging: aws.String("team=other")})
			assert.Nil(t, err)

			getResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
			assert.Nil(t, err)
			io.ReadAll(getResult.Body)

			_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: bucketName, Key: key2})
			assertForbidden(t, err)
		})

		t.Run("ListObjects is filtered by existing object tag"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body), Tagging: aws.String("team=storage")})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key2, Body: bytes.NewReader(body), Tagging: aws.String("team=other")})
			assert.Nil(t, err)

			list, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: bucketName})
			assert.Nil(t, err)
			assert.Len(t, list.Contents, 1)
			if len(list.Contents) == 1 {
				assert.Equal(t, *key, *list.Contents[0].Key)
			}
		})

		t.Run("PutObjectTagging is gated by the request tag being set"+testSuffix, func(t *testing.T) {
			s3Client, _, cleanup := setupTestServerWithAuthorizer(newAuthorizer(), dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
			t.Cleanup(cleanup)

			_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: bucketName})
			assert.Nil(t, err)
			_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("storage")}}},
			})
			assert.Nil(t, err)

			_, err = s3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
				Bucket:  bucketName,
				Key:     key,
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("other")}}},
			})
			assertForbidden(t, err)
		})
	})
}
