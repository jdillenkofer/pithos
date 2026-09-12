package main

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestObjectLockSDK(t *testing.T) {
	testutils.SkipIfNotIntegration(t)
	runIntegrationTest(t, func(t *testing.T, suffix string, dbType database.DatabaseType, path, replicated, filesystem bool, encryption storageFactory.EncryptionType, outbox, compression bool) {
		t.Run(suffix, func(t *testing.T) {
			client, _, cleanup := setupTestServer(dbType, path, replicated, filesystem, encryption, outbox, compression)
			t.Cleanup(cleanup)
			ctx := t.Context()
			bucket := aws.String("object-lock-sdk")
			key := aws.String("object")
			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket, ObjectLockEnabledForBucket: aws.Bool(true)})
			require.NoError(t, err)
			cfg, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: bucket})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockEnabledEnabled, cfg.ObjectLockConfiguration.ObjectLockEnabled)
			_, err = client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: bucket, ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled, Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionModeGovernance, Days: aws.Int32(1)}}}})
			require.NoError(t, err)
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("missing checksum")})
			require.Error(t, err)
			put, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("locked data"), ChecksumAlgorithm: types.ChecksumAlgorithmSha256})
			require.NoError(t, err)
			require.NotNil(t, put.VersionId)
			retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockRetentionModeGovernance, retention.Retention.Mode)
			head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockModeGovernance, head.ObjectLockMode)
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.Error(t, err)
			_, err = client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: bucket, Key: key, VersionId: put.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
			require.NoError(t, err)
			hold, err := client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockLegalHoldStatusOn, hold.LegalHold.Status)
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId, BypassGovernanceRetention: aws.Bool(true)})
			require.Error(t, err)
			_, err = client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: bucket, Key: key, VersionId: put.VersionId, Retention: &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: aws.Time(time.Now().UTC().Add(time.Hour))}, BypassGovernanceRetention: aws.Bool(true)})
			require.NoError(t, err)
			_, err = client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: bucket, Key: key, VersionId: put.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}})
			require.NoError(t, err)
			marker, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key})
			require.NoError(t, err)
			require.True(t, aws.ToBool(marker.DeleteMarker))
			multi, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: key, VersionId: put.VersionId}, {Key: key, VersionId: marker.VersionId}}}})
			require.NoError(t, err)
			require.Len(t, multi.Errors, 1)
			require.Len(t, multi.Deleted, 1)
			require.Equal(t, put.VersionId, multi.Errors[0].VersionId)
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId, BypassGovernanceRetention: aws.Bool(true)})
			require.NoError(t, err)
			// Explicit headers and multipart settings also cross the SDK boundary.
			until := time.Now().UTC().Add(2 * time.Hour)
			put, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("explicit"), ObjectLockMode: types.ObjectLockModeCompliance, ObjectLockRetainUntilDate: &until, ChecksumAlgorithm: types.ChecksumAlgorithmSha256})
			require.NoError(t, err)
			head, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockModeCompliance, head.ObjectLockMode)
			copied, err := client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: aws.String("copy"), CopySource: aws.String(*bucket + "/" + *key + "?versionId=" + *put.VersionId)})
			require.NoError(t, err)
			copyHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("copy"), VersionId: copied.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockModeGovernance, copyHead.ObjectLockMode)
			// Append produces a fresh version with the bucket default and leaves
			// the compliance-protected source version intact.
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader(" appended"), WriteOffsetBytes: aws.Int64(8)})
			require.NoError(t, err)
			appended, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockModeGovernance, appended.ObjectLockMode)
			require.EqualValues(t, 17, *appended.ContentLength)
			require.NotEqual(t, *put.VersionId, *appended.VersionId)
			source, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key, VersionId: put.VersionId})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockModeCompliance, source.ObjectLockMode)
			upload, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("multipart"), ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn})
			require.NoError(t, err)
			part, err := client.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: aws.String("multipart"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("multipart")})
			require.NoError(t, err)
			_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: aws.String("multipart"), UploadId: upload.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag, ChecksumCRC32: part.ChecksumCRC32}}}})
			require.NoError(t, err)
			head, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("multipart")})
			require.NoError(t, err)
			require.Equal(t, types.ObjectLockLegalHoldStatusOn, head.ObjectLockLegalHoldStatus)
		})
	})
}
