package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type createBucketStorage struct {
	accountStorage
	created int
}

func (s *createBucketStorage) CreateBucket(context.Context, storage.BucketName, ...storage.CreateBucketOptions) error {
	s.created++
	return nil
}

func TestCreateBucketLocationConstraint(t *testing.T) {
	configuration := func(constraint string) string {
		return `<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + constraint + `</LocationConstraint></CreateBucketConfiguration>`
	}
	for _, tc := range []struct {
		name, region, body string
		wantCode           int
		wantError          string
	}{
		{"no body", "eu-central-1", "", http.StatusOK, ""},
		{"whitespace body", "eu-central-1", "\n  ", http.StatusOK, ""},
		{"matching constraint", "eu-central-1", configuration("eu-central-1"), http.StatusOK, ""},
		{"empty constraint", "eu-central-1", configuration(""), http.StatusOK, ""},
		{"configuration without constraint", "eu-central-1", `<CreateBucketConfiguration/>`, http.StatusOK, ""},
		{"configuration without namespace", "eu-central-1", `<CreateBucketConfiguration><LocationConstraint>eu-central-1</LocationConstraint></CreateBucketConfiguration>`, http.StatusOK, ""},
		{"EU alias for eu-west-1", "eu-west-1", configuration("EU"), http.StatusOK, ""},
		{"EU alias for another region", "eu-central-1", configuration("EU"), http.StatusBadRequest, "IllegalLocationConstraintException"},
		{"mismatching constraint", "eu-central-1", configuration("us-west-2"), http.StatusBadRequest, "IllegalLocationConstraintException"},
		{"malformed XML", "eu-central-1", `<CreateBucketConfiguration>`, http.StatusBadRequest, "MalformedXML"},
		{"wrong root element", "eu-central-1", `<LocationConstraint>eu-central-1</LocationConstraint>`, http.StatusBadRequest, "MalformedXML"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &createBucketStorage{}
			handler := SetupServer(nil, tc.region, "s3.test", "website.test", &captureAuthorizer{allowed: true}, store)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "http://s3.test/bucket", strings.NewReader(tc.body)))
			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			if tc.wantError == "" {
				require.Equal(t, 1, store.created)
				return
			}
			require.Contains(t, w.Body.String(), "<Code>"+tc.wantError+"</Code>")
			require.Zero(t, store.created)
		})
	}
}

func TestCreateBucketLocationConstraintCheckedAfterAuthorization(t *testing.T) {
	store := &createBucketStorage{}
	handler := SetupServer(nil, "eu-central-1", "s3.test", "website.test", &captureAuthorizer{}, store)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "http://s3.test/bucket", strings.NewReader(`<CreateBucketConfiguration>`)))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, store.created)
}
