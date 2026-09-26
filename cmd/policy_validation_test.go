package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/stretchr/testify/require"
)

func TestPolicyReloadRejectsEmptyConditionBodies(t *testing.T) {
	runIntegrationTest(t, func(t *testing.T, suffix string, db database.DatabaseType, path, replicated, filesystem bool, encryption storageFactory.EncryptionType, outbox, compression bool) {
		t.Run(suffix, func(t *testing.T) {
			condition := `{"s3:ExistingObjectTag/team":"storage"}`
			valid := `{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:CreateBucket","s3:PutObject"],"Resource":"*"},{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":` + condition + `}}]}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"test-account","principalId":"test-principal"}]}]}`
			file := filepath.Join(t.TempDir(), "policy.json")
			require.NoError(t, os.WriteFile(file, []byte(valid), 0600))
			authorizer, err := policy.NewAuthorizer(file, 0)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, authorizer.Close()) })
			client, _, cleanup := setupTestServerWithAuthorizer(authorizer, db, path, replicated, filesystem, encryption, outbox, compression)
			t.Cleanup(cleanup)
			_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)
			_, err = client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			require.NoError(t, err)
			for _, invalid := range []string{`null`, `{}`} {
				require.NoError(t, os.WriteFile(file, []byte(strings.Replace(valid, condition, invalid, 1)), 0600))
				require.Error(t, authorizer.Reload())
				_, err = client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: bucketName, Key: key})
				require.Error(t, err, "invalid reload must not turn a conditional grant into unconditional access")
			}
		})
	})
}
