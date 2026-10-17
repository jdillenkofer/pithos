package server

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type versionListingStorage struct {
	accountStorage
	calls int
}

func (s *versionListingStorage) ListObjectVersions(context.Context, storage.BucketName, storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	s.calls++
	return &storage.ListObjectVersionsResult{}, nil
}

func TestListObjectVersionsRejectsVersionIDMarkerWithoutKeyMarker(t *testing.T) {
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	require.NoError(t, err)
	for _, tc := range []struct {
		query    string
		wantCode int
	}{
		{"versions&version-id-marker=v1", 400},
		{"versions&key-marker=&version-id-marker=v1", 400},
		{"versions&key-marker=a&version-id-marker=v1", 200},
		{"versions&key-marker=a", 200},
		{"versions&version-id-marker=", 200},
		{"versions", 200},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store := &versionListingStorage{}
			handler := SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, store)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest("GET", "http://s3.test/bucket?"+tc.query, nil))
			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			if tc.wantCode == 400 {
				var errorResponse struct{ Code string }
				require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &errorResponse))
				require.Equal(t, "InvalidArgument", errorResponse.Code)
				require.Zero(t, store.calls)
			} else {
				require.Equal(t, 1, store.calls)
			}
		})
	}
}
