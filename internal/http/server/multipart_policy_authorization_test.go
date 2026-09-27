package server

import (
	"context"
	"errors"
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

type multipartPolicyStorage struct {
	accountStorage
	tags  map[string]string
	err   error
	calls int
}

func (s *multipartPolicyStorage) ListParts(_ context.Context, bucket storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, _ storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	s.calls++
	return &storage.ListPartsResult{Tags: s.tags}, s.err
}

func TestMultipartPolicyUsesStoredRequestTags(t *testing.T) {
	testutils.SkipIfIntegration(t)
	snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*","Condition":{"StringEquals":{"s3:RequestObjectTag/team":"storage"},"ForAllValues:StringEquals":{"s3:RequestObjectTagKeys":"team"}}}}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"owner","principalId":"writer"}]}]}`))
	require.NoError(t, err)
	for _, operation := range []string{authorization.OperationUploadPart, authorization.OperationUploadPartCopy, authorization.OperationCompleteMultipartUpload} {
		for _, tc := range []struct {
			name    string
			tags    map[string]string
			err     error
			allowed bool
		}{
			{"stored tags", map[string]string{"team": "storage"}, nil, true},
			{"forged header", map[string]string{"team": "other"}, nil, false},
			{"no initiation tags", map[string]string{}, nil, false},
			{"unsupported backend", nil, nil, false},
			{"lookup failure", nil, errors.New("lookup failed"), false},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				store := &multipartPolicyStorage{accountStorage: accountStorage{owners: map[string]string{"bucket": "owner"}}, tags: tc.tags, err: tc.err}
				s := &Server{storage: store, requestAuthorizer: snapshot}
				ctx := authentication.WithRequestAuthentication(context.Background(), authentication.RequestAuthentication{Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "owner", PrincipalID: "writer"}})
				r := httptest.NewRequest(http.MethodPut, "/bucket/key?uploadId=01ARZ3NDEKTSV4RRFFQ69G5FAV", nil).WithContext(ctx)
				r.Header.Set("x-amz-tagging", "team=storage")
				request, authenticated := makeAuthorizationRequest(ctx, operation, stringPtr("bucket"), stringPtr("key"), r)
				if operation == authorization.OperationUploadPartCopy {
					request.SourceBucket = stringPtr("bucket")
					request.SourceKey = stringPtr("source")
				}
				stopped := s.runAuthorization(ctx, request, authenticated, httptest.NewRecorder(), r)
				require.Equal(t, !tc.allowed, stopped)
				require.Equal(t, 1, store.calls)
			})
		}
	}
}
