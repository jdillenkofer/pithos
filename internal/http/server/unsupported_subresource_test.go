package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedDestructiveSubresourcesDoNotFallThrough(t *testing.T) {
	for _, tc := range []struct {
		name, method, target string
		bucket               bool
		body                 string
	}{
		{"delete bucket policy", http.MethodDelete, "/bucket?policy", true, ""},
		{"put canned object acl", http.MethodPut, "/bucket/key?acl", false, ""},
		{"put xml object acl", http.MethodPut, "/bucket/key?acl", false, "<AccessControlPolicy/>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			s := &Server{}
			if tc.bucket {
				s.routeBucketDeleteHandler(w, r)
			} else {
				s.uploadPartOrPutObjectHandler(w, r)
			}
			require.Equal(t, http.StatusNotImplemented, w.Code)
			require.Contains(t, w.Body.String(), "NotImplemented")
		})
	}
}
