package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type denialCaptureStorage struct {
	storage.Storage
	operation auditlog.Operation
	resource  auditlog.ResourceDetails
	details   *auditlog.ObjectLockDetails
}

func (s *denialCaptureStorage) HeadBucket(_ context.Context, name storage.BucketName) (*storage.Bucket, error) {
	return &storage.Bucket{Name: name, OwnerAccountID: "account"}, nil
}

func (s *denialCaptureStorage) RecordAuthorizationDenied(ctx context.Context, operation auditlog.Operation, resource auditlog.ResourceDetails, details *auditlog.ObjectLockDetails) {
	s.operation = operation
	s.resource = resource
	s.details = details
}
func TestObjectLockAuthorizationAndDeniedAudit(t *testing.T) {
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request)
   if request.operation == "BypassGovernanceRetention" then
     return request.versionID == "allowed-version" and request.bypassGovernanceRetentionRequested
   end
   return request.operation == "PutObjectRetention" and request.objectLockMode == "GOVERNANCE"
 end`)
	require.NoError(t, err)
	st := &denialCaptureStorage{}
	server := &Server{storage: st, requestAuthorizer: authorizer}
	request := httptest.NewRequest("DELETE", "/bucket/key?versionId=protected-version", nil)
	request = request.WithContext(authentication.WithRequestAuthentication(request.Context(), authentication.RequestAuthentication{Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "account", PrincipalID: "principal"}}))
	request.Header.Set("x-amz-bypass-governance-retention", "true")
	response := httptest.NewRecorder()
	allowed, stop := server.authorizeGovernanceBypass(request.Context(), "bucket", "key", response, request)
	require.False(t, allowed)
	require.True(t, stop)
	require.Equal(t, auditlog.Operation(authorization.OperationBypassGovernanceRetention), st.operation)
	require.Equal(t, "protected-version", st.resource.VersionID)
	require.True(t, st.details.BypassRequested)
	require.False(t, st.details.BypassAuthorized)
	// Confirm Lua receives the parsed requested values and the explicit version.
	request = httptest.NewRequest("PUT", "/bucket/key?versionId=allowed-version", nil)
	request = request.WithContext(authentication.WithRequestAuthentication(request.Context(), authentication.RequestAuthentication{Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "account", PrincipalID: "principal"}}))
	request.Header.Set("x-amz-bypass-governance-retention", "true")
	response = httptest.NewRecorder()
	allowed, stop = server.authorizeGovernanceBypass(request.Context(), "bucket", "key", response, request)
	require.True(t, allowed)
	require.False(t, stop)
	request.Header.Set("x-amz-object-lock-mode", "GOVERNANCE")
	auth, _ := makeAuthorizationRequest(request.Context(), authorization.OperationPutObjectRetention, nil, nil, request)
	allowed, err = authorizer.AuthorizeRequest(request.Context(), auth)
	require.NoError(t, err)
	require.True(t, allowed)
}
func TestObjectLockReadPermissionsAndValidation(t *testing.T) {
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request) return request.operation == "GetObjectLegalHold" and request.versionID == "version" end`)
	require.NoError(t, err)
	server := &Server{requestAuthorizer: authorizer}
	version := "version"
	hold := storage.LegalHoldOn
	request := httptest.NewRequest("HEAD", "/bucket/key", nil)
	request.SetPathValue(bucketPath, "bucket")
	request.SetPathValue(keyPath, "key")
	response := httptest.NewRecorder()
	server.setObjectLockHeaders(response, request, &storage.Object{VersionID: &version, ObjectLock: storage.ObjectLock{LegalHold: &hold, Retention: &storage.ObjectRetention{Mode: storage.RetentionModeCompliance, RetainUntilDate: time.Now().Add(time.Hour)}}})
	require.Equal(t, "ON", response.Header().Get("x-amz-object-lock-legal-hold"))
	require.Empty(t, response.Header().Get("x-amz-object-lock-mode"))
	for _, name := range []string{"x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-object-lock-legal-hold"} {
		request := httptest.NewRequest("PUT", "/bucket/key", nil)
		request.Header.Set(name, "")
		_, err := parseObjectLockHeaders(request)
		require.Error(t, err)
	}
	request = httptest.NewRequest("PUT", "/bucket?object-lock", strings.NewReader(`<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`))
	request.Header.Set("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")
	_, err = readObjectLockBody(request, httptest.NewRecorder())
	require.Error(t, err)
}
