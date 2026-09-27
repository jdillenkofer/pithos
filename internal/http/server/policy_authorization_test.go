package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestLuaReceivesAuthenticatedAuthType(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name          string
		authType      authentication.AuthType
		authenticated bool
		want          string
	}{
		{"anonymous", authentication.AuthTypeAnonymous, false, "Anonymous"},
		{"header", authentication.AuthTypeSigV4Header, true, "REST-HEADER"},
		{"presigned", authentication.AuthTypeSigV4Presign, true, "REST-QUERY-STRING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return r.authorization.authType == "` + tc.want + `" end`)
			require.NoError(t, err)
			auth := authentication.RequestAuthentication{Type: tc.authType, Authenticated: tc.authenticated}
			if tc.authenticated {
				auth.Identity = &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "a", PrincipalID: "p"}
			}
			ctx := authentication.WithRequestAuthentication(context.Background(), auth)
			// Raw query/header values must not override the middleware's result.
			r := httptest.NewRequest("GET", "/bucket/key?X-Amz-Credential=untrusted", nil).WithContext(ctx)
			r.Header.Set("Authorization", "untrusted")
			request, _ := makeAuthorizationRequest(ctx, authorization.OperationGetObject, stringPtr("bucket"), stringPtr("key"), r)
			d, err := authorizer.AuthorizeRequest(ctx, request)
			require.NoError(t, err)
			require.Equal(t, authorization.Allow, d.Effect)
		})
	}
}

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

func TestPolicyUsesVerifiedSignatureVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, allowed := range []string{"AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256"} {
		snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":
			{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"s3:signatureversion":"` + allowed + `"}}}
		}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"},{"type":"anonymous"}]}]}`))
		require.NoError(t, err)
		for _, authType := range []authentication.AuthType{authentication.AuthTypeSigV4Header, authentication.AuthTypeSigV4Presign} {
			for _, version := range []string{"AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256", ""} {
				t.Run(allowed+"/"+string(authType)+"/"+version, func(t *testing.T) {
					auth := authentication.RequestAuthentication{
						Authenticated: true, Type: authType, SignatureVersion: version,
						Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "a", PrincipalID: "p"},
					}
					for _, authenticated := range []bool{true, false} {
						auth.Authenticated = authenticated
						ctx := authentication.WithRequestAuthentication(context.Background(), auth)
						// Forged raw metadata must never override the verified result.
						r := httptest.NewRequest("GET", "/bucket/key?X-Amz-Algorithm="+allowed, nil).WithContext(ctx)
						r.Header.Set("Authorization", allowed+" untrusted")
						request, _ := makeAuthorizationRequest(ctx, authorization.OperationGetObject, stringPtr("bucket"), stringPtr("key"), r)
						d, err := snapshot.AuthorizeRequest(ctx, request)
						require.NoError(t, err)
						want := authorization.ImplicitDeny
						if authenticated && version == allowed {
							want = authorization.Allow
						}
						require.Equal(t, want, d.Effect)
					}
				})
			}
		}
	}
}
