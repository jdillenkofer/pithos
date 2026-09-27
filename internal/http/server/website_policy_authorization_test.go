package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type websitePolicyStorage struct {
	accountStorage
	tags           map[string]string
	tagErr         error
	resolvedKey    string
	getObjectCalls int
}

func (s *websitePolicyStorage) GetBucketWebsiteConfiguration(context.Context, storage.BucketName) (*storage.WebsiteConfiguration, error) {
	return &storage.WebsiteConfiguration{IndexDocumentSuffix: "index.html"}, nil
}

func (s *websitePolicyStorage) GetObjectTagging(_ context.Context, _ storage.BucketName, key storage.ObjectKey, _ *storage.ObjectTaggingOptions) (map[string]string, error) {
	s.resolvedKey = key.String()
	return s.tags, s.tagErr
}

func (s *websitePolicyStorage) GetObject(context.Context, storage.BucketName, storage.ObjectKey, []storage.ByteRange, *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	s.getObjectCalls++
	return &storage.Object{Size: 6}, []io.ReadCloser{io.NopCloser(strings.NewReader("secret"))}, nil
}

func TestWebsiteErrorDocumentRequiresAuthorization(t *testing.T) {
	testutils.SkipIfIntegration(t)
	snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/missing"}}},"bindings":[{"policy":"p","subjects":[{"type":"anonymous"}]}]}`))
	require.NoError(t, err)
	store := &websitePolicyStorage{accountStorage: accountStorage{owners: map[string]string{"bucket": "owner"}}}
	s := &Server{storage: store, requestAuthorizer: snapshot}
	r := httptest.NewRequest(http.MethodGet, "/bucket/missing", nil)
	w := httptest.NewRecorder()
	bucket := storage.MustNewBucketName("bucket")
	config := &storage.WebsiteConfiguration{ErrorDocumentKey: stringPtr("private/error.html")}

	s.serveErrorDocument(w, r, bucket, config, http.StatusNotFound, "NoSuchKey", "missing")

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.NotContains(t, w.Body.String(), "secret")
	require.Zero(t, store.getObjectCalls)
}

func TestWebsitePolicyUsesResolvedObjectTags(t *testing.T) {
	testutils.SkipIfIntegration(t)
	snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},
 {"Effect":"Deny","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"s3:ExistingObjectTag/private":"true"}}}
 ]}},"bindings":[{"policy":"p","subjects":[{"type":"anonymous"}]}]}`))
	require.NoError(t, err)
	for _, operation := range []string{authorization.OperationGetObject, authorization.OperationHeadObject} {
		for _, tc := range []struct {
			name    string
			tags    map[string]string
			tagErr  error
			allowed bool
		}{
			{"private", map[string]string{"private": "true"}, nil, false},
			{"public", map[string]string{"private": "false"}, nil, true},
			{"untagged", nil, nil, true},
			{"lookup error", nil, errors.New("tag lookup failed"), false},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				store := &websitePolicyStorage{accountStorage: accountStorage{owners: map[string]string{"bucket": "a"}}, tags: tc.tags, tagErr: tc.tagErr}
				s := &Server{storage: store, requestAuthorizer: snapshot}
				r := httptest.NewRequest("GET", "/bucket/docs/", nil)
				r.SetPathValue(keyPath, "docs/")
				bucket, err := storage.NewBucketName("bucket")
				require.NoError(t, err)
				_, _, _, ok := s.websitePrepare(r.Context(), httptest.NewRecorder(), r, operation, bucket)
				require.Equal(t, tc.allowed, ok)
				require.Equal(t, "docs/index.html", store.resolvedKey)
			})
		}
	}
}
