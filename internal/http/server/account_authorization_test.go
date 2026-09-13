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

func (a *captureAuthorizer) AuthorizeRequest(_ context.Context, request *authorization.Request) (bool, error) {
	a.called, a.request = true, request
	return a.allowed, nil
}

func TestAccountBoundaryPrecedesLuaAuthorization(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name       string
		account    string
		source     *string
		wantStatus int
		wantLua    bool
	}{
		{name: "same account", account: "account-a", wantStatus: 200, wantLua: true},
		{name: "foreign destination", account: "account-b", wantStatus: 403},
		{name: "foreign copy source", account: "account-a", source: stringPtr("source"), wantStatus: 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := &captureAuthorizer{allowed: true}
			s := &Server{storage: &accountStorage{owners: map[string]string{"destination": "account-a", "source": "account-b"}}, requestAuthorizer: authorizer}
			request := &authorization.Request{Operation: authorization.OperationGetObject, Bucket: stringPtr("destination"), SourceBucket: tc.source, Authorization: authorization.Authorization{AccountId: &tc.account}}
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

func TestAnonymousWebsiteReadStillRequiresLuaApproval(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, allowed := range []bool{false, true} {
		authorizer := &captureAuthorizer{allowed: allowed}
		s := &Server{storage: &accountStorage{owners: map[string]string{"public": "account-a"}}, requestAuthorizer: authorizer}
		request := &authorization.Request{Operation: authorization.OperationGetObject, Bucket: stringPtr("public")}
		response := httptest.NewRecorder()
		stopped := s.runAuthorization(context.Background(), request, false, response, httptest.NewRequest("GET", "/public/index.html", nil))
		require.Equal(t, !allowed, stopped)
		require.True(t, authorizer.called)
		require.Equal(t, "account-a", *authorizer.request.ResourceAccountId)
		if !allowed {
			require.Equal(t, 401, response.Code)
		}
	}
}

func stringPtr(value string) *string { return &value }
