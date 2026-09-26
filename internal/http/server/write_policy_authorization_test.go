package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type versionPolicyStorage struct{ accountStorage }

func (s *versionPolicyStorage) GetObjectTagging(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	if opts.VersionID != nil && *opts.VersionID == "old-allowed" {
		return map[string]string{"writable": "true"}, nil
	}
	return map[string]string{"writable": "false"}, nil
}

func TestWritePolicyIgnoresQueryVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*","Condition":{"StringEquals":{"s3:ExistingObjectTag/writable":"true"}}}}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"owner","principalId":"writer"}]}]}`))
	require.NoError(t, err)
	for _, operation := range []string{
		authorization.OperationPutObject, authorization.OperationAppendObject,
		authorization.OperationCreateMultipartUpload, authorization.OperationUploadPart,
		authorization.OperationCompleteMultipartUpload, authorization.OperationAbortMultipartUpload,
		authorization.OperationGetObjectVersion, authorization.OperationPutObjectVersionTagging,
		authorization.OperationPutObjectRetention,
	} {
		t.Run(operation, func(t *testing.T) {
			s := &Server{storage: &versionPolicyStorage{accountStorage{owners: map[string]string{"bucket": "owner"}}}, requestAuthorizer: snapshot}
			ctx := authentication.WithRequestAuthentication(context.Background(), authentication.RequestAuthentication{Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "owner", PrincipalID: "writer"}})
			r := httptest.NewRequest(http.MethodPut, "/bucket/key?versionId=old-allowed", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			stopped := s.authorizeRequest(ctx, operation, stringPtr("bucket"), stringPtr("key"), w, r)
			versioned := operation == authorization.OperationGetObjectVersion || operation == authorization.OperationPutObjectVersionTagging || operation == authorization.OperationPutObjectRetention
			require.Equal(t, !versioned, stopped)
			request, _ := makeAuthorizationRequest(ctx, operation, stringPtr("bucket"), stringPtr("key"), r)
			if versioned {
				require.Equal(t, "old-allowed", *request.VersionID)
			} else {
				require.Nil(t, request.VersionID)
				require.Equal(t, http.StatusForbidden, w.Code)
			}
		})
	}
}
