package main

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestAppendOffsetHeaderSDK(t *testing.T) {
	testutils.SkipIfNotIntegration(t)
	runIntegrationTest(t, func(t *testing.T, suffix string, dbType database.DatabaseType, path, replicated, filesystem bool, encryption storageFactory.EncryptionType, outbox, compression bool) {
		t.Run(suffix, func(t *testing.T) {
			client, _, cleanup := setupTestServer(dbType, path, replicated, filesystem, encryption, outbox, compression)
			t.Cleanup(cleanup)
			ctx := t.Context()
			bucket, key := aws.String("append-offset"), aws.String("object")
			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})
			require.NoError(t, err)
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("abc")})
			require.NoError(t, err)
			read := func(want string) {
				object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
				require.NoError(t, err)
				data, err := io.ReadAll(object.Body)
				object.Body.Close()
				require.NoError(t, err)
				require.Equal(t, want, string(data))
			}
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("def"), WriteOffsetBytes: aws.Int64(3)})
			require.NoError(t, err)
			read("abcdef")
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("must not replace"), WriteOffsetBytes: aws.Int64(3)})
			require.Error(t, err)
			read("abcdef")
			// An absent offset keeps ordinary replacement semantics.
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("")})
			require.NoError(t, err)
			_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("zero"), WriteOffsetBytes: aws.Int64(0)})
			require.NoError(t, err)
			read("zero")
		})
	})
}
