package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type accountStorage struct {
	delegator.DelegatingStorage
	owners map[string]string
}

func (s *accountStorage) HeadBucket(_ context.Context, name storage.BucketName) (*storage.Bucket, error) {
	owner, ok := s.owners[name.String()]
	if !ok {
		return nil, storage.ErrNoSuchBucket
	}
	return &storage.Bucket{Name: name, OwnerAccountID: owner}, nil
}

type captureAuthorizer struct {
	called  bool
	request *authorization.Request
	allowed bool
}

func (a *captureAuthorizer) AuthorizeRequest(_ context.Context, request *authorization.Request) (authorization.Decision, error) {
	a.called, a.request = true, request
	effect := authorization.ExplicitDeny
	if a.allowed {
		effect = authorization.Allow
	}
	return authorization.Decision{Effect: effect}, nil
}

func TestAccountBoundaryPrecedesLuaAuthorization(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name       string
		account    string
		bucket     string
		source     *string
		wantStatus int
		wantLua    bool
	}{
		{name: "same account", account: "account-a", wantStatus: 200, wantLua: true},
		{name: "foreign destination", account: "account-b", wantStatus: 403},
		{name: "missing destination", account: "account-a", bucket: "missing-destination", wantStatus: 404},
		{name: "foreign copy source", account: "account-a", source: stringPtr("source"), wantStatus: 403},
		{name: "missing copy source", account: "account-a", source: stringPtr("missing-source"), wantStatus: 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := &captureAuthorizer{allowed: true}
			s := &Server{storage: &accountStorage{owners: map[string]string{"destination": "account-a", "source": "account-b"}}, requestAuthorizer: authorizer}
			bucket := tc.bucket
			if bucket == "" {
				bucket = "destination"
			}
			request := &authorization.Request{Operation: authorization.OperationGetObject, Bucket: &bucket, SourceBucket: tc.source, Authorization: authorization.Authorization{AccountId: &tc.account}}
			response := httptest.NewRecorder()
			stopped := s.runAuthorization(context.Background(), request, true, response, httptest.NewRequest("GET", "/destination/key", nil))
			require.Equal(t, tc.wantStatus != 200, stopped)
			require.Equal(t, tc.wantStatus, response.Code)
			require.Equal(t, tc.wantLua, authorizer.called)
			if tc.wantLua {
				require.Equal(t, "account-a", *authorizer.request.ResourceAccountId)
			}
		})
	}
}

func TestAnonymousOperationsRequireAuthorizerApproval(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, operation := range []string{authorization.OperationGetObject, authorization.OperationPutObject, authorization.OperationDeleteBucket} {
		for _, allowed := range []bool{false, true} {
			authorizer := &captureAuthorizer{allowed: allowed}
			s := &Server{storage: &accountStorage{owners: map[string]string{"public": "account-a"}}, requestAuthorizer: authorizer}
			request := &authorization.Request{Operation: operation, Bucket: stringPtr("public")}
			response := httptest.NewRecorder()
			stopped := s.runAuthorization(context.Background(), request, false, response, httptest.NewRequest("PUT", "/public/key", nil))
			require.Equal(t, !allowed, stopped)
			require.True(t, authorizer.called)
			require.Equal(t, "account-a", *authorizer.request.ResourceAccountId)
			if !allowed {
				require.Equal(t, 401, response.Code)
			}
		}
	}
}

func TestDisabledAuthenticationLeavesAuthorizationToAuthorizer(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer := &captureAuthorizer{allowed: true}
	s := &Server{
		storage:                &accountStorage{owners: map[string]string{"destination": "account-a"}},
		requestAuthorizer:      authorizer,
		authenticationDisabled: true,
	}
	request := &authorization.Request{Operation: authorization.OperationPutObject, Bucket: stringPtr("destination")}
	response := httptest.NewRecorder()
	stopped := s.runAuthorization(context.Background(), request, false, response, httptest.NewRequest("PUT", "/destination/key", nil))
	require.False(t, stopped)
	require.True(t, authorizer.called)
	require.Nil(t, authorizer.request.ResourceAccountId)
	require.Equal(t, "authentication-disabled", s.storageAccountID(context.Background()))
}

func stringPtr(value string) *string { return &value }
