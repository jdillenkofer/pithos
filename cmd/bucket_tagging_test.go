package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/stretchr/testify/require"
)

func TestBucketTaggingSDK(t *testing.T) {
	runIntegrationTest(t, func(t *testing.T, _ string, dbType database.DatabaseType, usePathStyle, useReplication, useFilesystemPartStore bool, encryptionType storageFactory.EncryptionType, wrapPartStoreWithOutbox, usePartStoreCompression bool) {
		client, _, cleanup := setupTestServer(dbType, usePathStyle, useReplication, useFilesystemPartStore, encryptionType, wrapPartStoreWithOutbox, usePartStoreCompression)
		defer cleanup()

		ctx := context.Background()
		bucket := "bucket-tagging"
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)

		_, err = client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("environment"), Value: aws.String("test")}}}})
		require.NoError(t, err)
		result, err := client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Contains(t, result.TagSet, types.Tag{Key: aws.String("environment"), Value: aws.String("test")})

		maxTagCount := 50
		if useReplication {
			// Replicated test variants use an S3 client storage as a secondary.
			// Reserve room for the internal system tags stored on that backend.
			maxTagCount = 45
		}
		maxTags := make([]types.Tag, maxTagCount)
		for i := range maxTags {
			maxTags[i] = types.Tag{Key: aws.String(fmt.Sprintf("key-%02d", i)), Value: aws.String("value")}
		}
		_, err = client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: maxTags}})
		require.NoError(t, err)
		result, err = client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Len(t, result.TagSet, maxTagCount)

		_, err = client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		result, err = client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		require.Empty(t, result.TagSet)
	})
}
