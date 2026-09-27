package main

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	storageFactory "github.com/jdillenkofer/pithos/internal/storage/factory"
	"github.com/stretchr/testify/require"
)

func TestPolicyListingLimits(t *testing.T) {
	runIntegrationTest(t, func(t *testing.T, suffix string, db database.DatabaseType, path, replicated, filesystem bool, encryption storageFactory.EncryptionType, outbox, compression bool) {
		t.Run(suffix, func(t *testing.T) {
			snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"admin":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}},"list":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],"Resource":"*","Condition":{"NumericLessThanEquals":{"s3:max-keys":"100"}}}}},"bindings":[{"policy":"admin","subjects":[{"type":"principal","accountId":"test-account","principalId":"test-principal"}]},{"policy":"list","subjects":[{"type":"anonymous"}]}]}`))
			require.NoError(t, err)
			client, addr, cleanup := setupTestServerWithAuthorizer(snapshot, db, path, replicated, filesystem, encryption, outbox, compression)
			t.Cleanup(cleanup)
			_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucketName})
			require.NoError(t, err)
			_, err = client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: bucketName, Key: key, Body: bytes.NewReader(body)})
			require.NoError(t, err)
			httpClient := buildWebsiteHttpClient(addr)
			for _, operation := range []string{"", "list-type=2&", "versions&"} {
				for _, limit := range []string{"-1", "1e1", "1.5", "0", "10"} {
					response, err := httpClient.Get("http://" + testAPIEndpoint + "/" + *bucketName + "?" + operation + "max-keys=" + limit)
					require.NoError(t, err)
					data, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.NoError(t, response.Body.Close())
					if limit == "0" || limit == "10" {
						require.Equal(t, http.StatusOK, response.StatusCode, string(data))
						if limit == "0" {
							require.NotContains(t, string(data), "<Key>")
						} else {
							require.Contains(t, string(data), "<Key>"+*key+"</Key>")
						}
					} else {
						require.Equal(t, http.StatusBadRequest, response.StatusCode, string(data))
					}
				}
			}
		})
	})
}
