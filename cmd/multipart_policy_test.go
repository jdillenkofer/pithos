package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/stretchr/testify/require"
)

func TestMultipartPolicyRequiredTags(t *testing.T) {
	runIntegrationTest(t, func(t *testing.T, testSuffix string, dbType database.DatabaseType, usePathStyle, useReplication, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox, usePartStoreCompression bool) {
		snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"admin":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}},"p":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:CreateBucket","s3:GetObject","s3:GetObjectTagging","s3:PutObjectTagging"],"Resource":"*"},{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Condition":{"StringEquals":{"s3:RequestObjectTag/team":"storage"}}}]}},"bindings":[{"policy":"admin","subjects":[{"type":"principal","accountId":"test-account","principalId":"test-principal"}]},{"policy":"p","subjects":[{"type":"principal","accountId":"test-account","principalId":"policy-test"}]}]}`))
		require.NoError(t, err)
		_, addr, cleanup := setupTestServerWithAuthorizer(snapshot, dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
		t.Cleanup(cleanup)
		client := setupS3ClientWithCredentials(testAPIEndpoint, addr, usePathStyle, policyTestAccessKeyId, policyTestSecretAccessKey)
		ctx := context.Background()
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucketName})
		require.NoError(t, err)
		_, err = client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: aws.String("denied"), Tagging: aws.String("team=other")})
		require.Error(t, err)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucketName, Key: aws.String("source"), Tagging: aws.String("team=storage"), Body: bytes.NewReader(body)})
		require.NoError(t, err)
		for _, copyPart := range []bool{false, true} {
			upload, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucketName, Key: key, Tagging: aws.String("team=storage")})
			require.NoError(t, err)
			var part types.CompletedPart
			part.PartNumber = aws.Int32(1)
			if copyPart {
				result, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: bucketName, Key: key, UploadId: upload.UploadId, PartNumber: part.PartNumber, CopySource: aws.String(*bucketName + "/source")})
				require.NoError(t, err)
				part.ETag = result.CopyPartResult.ETag
				part.ChecksumCRC32 = result.CopyPartResult.ChecksumCRC32
			} else {
				result, err := client.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucketName, Key: key, UploadId: upload.UploadId, PartNumber: part.PartNumber, Body: bytes.NewReader(body)})
				require.NoError(t, err)
				part.ETag = result.ETag
				part.ChecksumCRC32 = result.ChecksumCRC32
			}
			_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucketName, Key: key, UploadId: upload.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}}})
			require.NoError(t, err)
			tags, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: bucketName, Key: key})
			require.NoError(t, err)
			require.Equal(t, []types.Tag{{Key: aws.String("team"), Value: aws.String("storage")}}, tags.TagSet)
		}
	})
}
