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
		route                func(*Server, http.ResponseWriter, *http.Request)
		body                 string
	}{
		{"delete bucket policy", http.MethodDelete, "/bucket?policy", (*Server).routeBucketDeleteHandler, ""},
		{"delete bucket versioning", http.MethodDelete, "/bucket?versioning", (*Server).routeBucketDeleteHandler, ""},
		{"get bucket encryption", http.MethodGet, "/bucket?encryption", (*Server).routeBucketGetHandler, ""},
		{"put bucket acl", http.MethodPut, "/bucket?acl", (*Server).routeBucketPutHandler, ""},
		{"put object acl", http.MethodPut, "/bucket/key?acl", (*Server).uploadPartOrPutObjectHandler, ""},
		{"put object acl multipart collision", http.MethodPut, "/bucket/key?acl&uploadId=upload&partNumber=1", (*Server).uploadPartOrPutObjectHandler, ""},
		{"get object acl", http.MethodGet, "/bucket/key?acl", (*Server).getObjectOrListPartsHandler, ""},
		{"head object attributes", http.MethodHead, "/bucket/key?attributes", (*Server).headObjectHandler, ""},
		{"post object select multipart collision", http.MethodPost, "/bucket/key?select&uploads", (*Server).createMultipartUploadOrCompleteMultipartUploadHandler, ""},
		{"delete object restore", http.MethodDelete, "/bucket/key?restore", (*Server).abortMultipartUploadOrDeleteObjectHandler, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			s := &Server{}
			tc.route(s, w, r)
			require.Equal(t, http.StatusNotImplemented, w.Code)
			require.Contains(t, w.Body.String(), "NotImplemented")
		})
	}
}
