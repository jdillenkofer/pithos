package server

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/stretchr/testify/require"
)

func TestGetBucketLocation(t *testing.T) {
	for _, tc := range []struct {
		region, want string
	}{
		{"eu-central-1", "eu-central-1"},
		{"us-east-1", ""},
	} {
		t.Run(tc.region, func(t *testing.T) {
			authorizer := &captureAuthorizer{allowed: true}
			store := &accountStorage{owners: map[string]string{"bucket": "account"}}
			handler := SetupServer(nil, tc.region, "s3.test", "website.test", authorizer, store)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://s3.test/bucket?location", nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, authorization.OperationGetBucketLocation, authorizer.request.Operation)
			require.Equal(t, "bucket", *authorizer.request.Bucket)
			require.Nil(t, authorizer.request.Key)
			var response struct {
				XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
				Value   string   `xml:",chardata"`
			}
			require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &response))
			require.Equal(t, tc.want, response.Value)
		})
	}
}

func TestGetBucketLocationMissingBucket(t *testing.T) {
	store := &accountStorage{owners: map[string]string{}}
	handler := SetupServer(nil, "eu-central-1", "s3.test", "website.test", &captureAuthorizer{allowed: true}, store)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://s3.test/bucket?location", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "NoSuchBucket")
}

func TestGetBucketLocationDenied(t *testing.T) {
	store := &accountStorage{owners: map[string]string{"bucket": "account"}}
	handler := SetupServer(nil, "eu-central-1", "s3.test", "website.test", &captureAuthorizer{}, store)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://s3.test/bucket?location", nil))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "AccessDenied")
}

func TestBucketLocationOnlySupportsGet(t *testing.T) {
	for _, tc := range []struct {
		method string
		route  func(*Server, http.ResponseWriter, *http.Request)
	}{
		{http.MethodPut, (*Server).routeBucketPutHandler},
		{http.MethodDelete, (*Server).routeBucketDeleteHandler},
	} {
		t.Run(tc.method, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.route(&Server{}, w, httptest.NewRequest(tc.method, "/bucket?location", nil))
			require.Equal(t, http.StatusNotImplemented, w.Code)
		})
	}
}
