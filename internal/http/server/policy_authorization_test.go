package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestPolicyDistinguishesPresignedRequests(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, conditionKey := range []string{"s3:authType", "pithos:AuthType"} {
		t.Run(conditionKey, func(t *testing.T) {
			snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},
				{"Effect":"Deny","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"` + conditionKey + `":"REST-QUERY-STRING"}}}
			]}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`))
			require.NoError(t, err)
			for _, tc := range []struct {
				authType authentication.AuthType
				want     authorization.Effect
			}{
				{authentication.AuthTypeSigV4Header, authorization.Allow},
				{authentication.AuthTypeSigV4Presign, authorization.ExplicitDeny},
			} {
				t.Run(string(tc.authType), func(t *testing.T) {
					ctx := authentication.WithRequestAuthentication(context.Background(), authentication.RequestAuthentication{
						Authenticated: true, Type: tc.authType,
						Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "a", PrincipalID: "p"},
					})
					r := httptest.NewRequest("GET", "/bucket/key", nil).WithContext(ctx)
					request, authenticated := makeAuthorizationRequest(ctx, authorization.OperationGetObject, stringPtr("bucket"), stringPtr("key"), r)
					require.True(t, authenticated)
					decision, err := snapshot.AuthorizeRequest(ctx, request)
					require.NoError(t, err)
					require.Equal(t, tc.want, decision.Effect)
				})
			}
		})
	}
}
