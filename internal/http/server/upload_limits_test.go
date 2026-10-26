package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	storage "github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestSizeLimitMiddleware(t *testing.T) {
	testutils.SkipIfIntegration(t)

	testCases := []struct {
		name           string
		contentLength  int64
		headers        map[string]string
		expectRejected bool
	}{
		{name: "content length at limit", contentLength: storage.MaxEntitySize},
		{name: "content length over limit", contentLength: storage.MaxEntitySize + 1, expectRejected: true},
		{name: "unknown content length", contentLength: -1},
		{
			name:          "aws-chunked decoded length at limit",
			contentLength: storage.MaxEntitySize + 1,
			headers: map[string]string{
				"Content-Encoding":             "aws-chunked",
				"X-Amz-Decoded-Content-Length": strconv.FormatInt(storage.MaxEntitySize, 10),
			},
		},
		{
			name:          "aws-chunked decoded length over limit",
			contentLength: 1024,
			headers: map[string]string{
				"Content-Encoding":             "aws-chunked,gzip",
				"X-Amz-Decoded-Content-Length": strconv.FormatInt(storage.MaxEntitySize+1, 10),
			},
			expectRejected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			called := false
			handler := makeRequestSizeLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest(http.MethodPut, "/bucket/key", http.NoBody)
			r.ContentLength = testCase.contentLength
			for key, value := range testCase.headers {
				r.Header.Set(key, value)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)

			if testCase.expectRejected {
				require.False(t, called)
				assert.Equal(t, http.StatusBadRequest, w.Code)
				assert.Contains(t, w.Body.String(), "<Code>EntityTooLarge</Code>")
			} else {
				assert.True(t, called)
				assert.Equal(t, http.StatusNoContent, w.Code)
			}
		})
	}
}
