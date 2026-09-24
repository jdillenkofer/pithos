package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestCopyPolicyUsesSourceVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, operation := range []string{authorization.OperationCopyObject, authorization.OperationUploadPartCopy} {
		for _, tc := range []struct {
			name, statements, query string
			version                 *string
			denied                  bool
		}{
			{"deny versioned read", `[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"},{"Effect":"Deny","Action":"s3:GetObjectVersion","Resource":"*"}]`, "", stringPtr("old-version"), true},
			{"allow selected version", `[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"},{"Effect":"Allow","Action":"s3:GetObjectVersion","Resource":"*","Condition":{"StringEquals":{"s3:VersionId":"allowed-version"}}}]`, "?versionId=untrusted", stringPtr("allowed-version"), false},
			{"reject other version", `[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"},{"Effect":"Allow","Action":"s3:GetObjectVersion","Resource":"*","Condition":{"StringEquals":{"s3:VersionId":"allowed-version"}}}]`, "?versionId=allowed-version", stringPtr("other-version"), true},
			{"unversioned source ignores query", `[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"},{"Effect":"Deny","Action":"s3:GetObjectVersion","Resource":"*"}]`, "?versionId=untrusted", nil, false},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":` + tc.statements + `}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`))
				require.NoError(t, err)
				s := &Server{storage: &accountStorage{owners: map[string]string{"source": "a", "destination": "a"}}, requestAuthorizer: snapshot}
				ctx := authentication.WithRequestAuthentication(context.Background(), authentication.RequestAuthentication{
					Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "a", PrincipalID: "p"},
				})
				r := httptest.NewRequest(http.MethodPut, "/destination/key"+tc.query, nil).WithContext(ctx)
				response := httptest.NewRecorder()
				stopped := s.authorizeCopyRequest(ctx, operation, "source", "key", tc.version, "destination", "key", response, r)
				require.Equal(t, tc.denied, stopped)
				if tc.denied {
					require.Equal(t, http.StatusForbidden, response.Code)
				}
			})
		}
	}
}
