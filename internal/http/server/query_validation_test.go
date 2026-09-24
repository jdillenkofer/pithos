package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestQueryValidationPreservesUnambiguousSignedQuery(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, raw := range []string{"", "versions", "prefix=", "prefix=public%2f&max-keys=10", "prefix=a%3Bb%26c", "X-Amz-Credential=key%2Fscope&X-Amz-Signature=abc"} {
		t.Run(raw, func(t *testing.T) {
			called := false
			h := makeQueryValidationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				require.Equal(t, raw, r.URL.RawQuery)
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest("GET", "http://s3.test/bucket", nil)
			r.URL.RawQuery = raw
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.True(t, called)
			require.Equal(t, http.StatusNoContent, w.Code)
		})
	}
}

type queryTestStorage struct {
	accountStorage
	prefixes []*string
}

func (s *queryTestStorage) ListObjects(_ context.Context, _ storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	s.prefixes = append(s.prefixes, opts.Prefix)
	return &storage.ListBucketResult{}, nil
}

func TestServerRejectsAmbiguousQueriesForBothAuthorizers(t *testing.T) {
	testutils.SkipIfIntegration(t)
	luaAuthorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return r.httpRequest:queryParamEquals("prefix", "public/") end`)
	require.NoError(t, err)
	policyAuthorizer, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*","Condition":{"StringEquals":{"s3:prefix":"public/"}}}}},"bindings":[{"policy":"p","subjects":[{"type":"anonymous"}]}]}`))
	require.NoError(t, err)
	for name, authorizer := range map[string]authorization.RequestAuthorizer{"lua": luaAuthorizer, "policy": policyAuthorizer} {
		t.Run(name, func(t *testing.T) {
			store := &queryTestStorage{}
			h := SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, store)
			for _, raw := range []string{
				"prefix=private/&prefix=public/", "prefix=public/&prefix=private/",
				"prefix=public/&%70refix=public/", "prefix=public/&max-keys=1000&max-keys=1",
				"prefix=public/&X-Amz-Credential=a&X-Amz-Credential=b",
				"prefix=public/&versionId=a&versionId=b", "prefix=public/&versions&versions",
				"prefix=public/&broken=%zz", "prefix=public/&broken=a;b",
			} {
				r := httptest.NewRequest("GET", "http://s3.test/bucket", nil)
				r.URL.RawQuery = raw
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				require.Equal(t, http.StatusBadRequest, w.Code, raw)
				require.Contains(t, w.Body.String(), "InvalidArgument")
			}
			require.Empty(t, store.prefixes)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "http://s3.test/bucket?prefix=public/", nil))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, store.prefixes, 1)
			require.Equal(t, "public/", *store.prefixes[0])
		})
	}
}
