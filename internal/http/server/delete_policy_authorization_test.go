package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

type policyDeleteStorage struct {
	accountStorage
	deleted []storage.DeleteObjectsInputEntry
}

func (s *policyDeleteStorage) DeleteObjects(_ context.Context, _ storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	s.deleted = append(s.deleted, entries...)
	return &storage.DeleteObjectsResult{}, nil
}

func TestMultiDeletePolicyUsesResourceAccount(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name, statements string
		denied           bool
	}{
		{"explicit deny", `[{"Effect":"Allow","Action":"s3:DeleteObject*","Resource":"*"},{"Effect":"Deny","Action":"s3:DeleteObject*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceAccount":"a"}}}]`, true},
		{"conditional allow", `{"Effect":"Allow","Action":"s3:DeleteObject*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceAccount":"a"}}}`, false},
		{"governance bypass deny", `[{"Effect":"Allow","Action":["s3:DeleteObject*","s3:BypassGovernanceRetention"],"Resource":"*"},{"Effect":"Deny","Action":"s3:BypassGovernanceRetention","Resource":"*","Condition":{"StringEquals":{"aws:ResourceAccount":"a"}}}]`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":` + tc.statements + `}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`))
			require.NoError(t, err)
			backend := &policyDeleteStorage{accountStorage: accountStorage{owners: map[string]string{"bucket": "a"}}}
			s := &Server{storage: backend, requestAuthorizer: snapshot, tracer: otel.Tracer("policy-delete-test")}
			ctx := authentication.WithRequestAuthentication(context.Background(), authentication.RequestAuthentication{
				Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "a", PrincipalID: "p"},
			})
			r := httptest.NewRequest(http.MethodPost, "/bucket?delete", strings.NewReader(`<Delete><Object><Key>current</Key></Object><Object><Key>versioned</Key><VersionId>old-version</VersionId></Object></Delete>`)).WithContext(ctx)
			r.SetPathValue(bucketPath, "bucket")
			if tc.name == "governance bypass deny" {
				r.Header.Set("X-Amz-Bypass-Governance-Retention", "true")
			}
			response := httptest.NewRecorder()
			s.deleteObjectsHandler(response, r)
			require.Equal(t, http.StatusOK, response.Code)
			var result DeleteObjectsResult
			require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
			if tc.denied {
				require.Empty(t, backend.deleted)
				require.Empty(t, result.Deleted)
				require.Len(t, result.Errors, 2)
				for _, entry := range result.Errors {
					require.Equal(t, "AccessDenied", entry.Code)
				}
			} else {
				require.Empty(t, result.Errors)
				require.Len(t, result.Deleted, 2)
				require.Len(t, backend.deleted, 2)
				require.Nil(t, backend.deleted[0].VersionID)
				require.Equal(t, "old-version", *backend.deleted[1].VersionID)
			}
		})
	}
}
