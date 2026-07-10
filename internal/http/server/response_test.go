package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteXMLResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/bucket", nil)

	writeXMLResponse(recorder, request, http.StatusCreated, BucketResult{Name: "bucket"})

	result := recorder.Result()
	defer result.Body.Close()
	assert.Equal(t, http.StatusCreated, result.StatusCode)
	assert.Equal(t, applicationXmlContentType, result.Header.Get(contentTypeHeader))
	assert.True(t, strings.HasPrefix(recorder.Body.String(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"))
	assert.Contains(t, recorder.Body.String(), "<Name>bucket</Name>")
}

func TestWriteXMLResponseHandlesMarshalFailure(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/bucket", nil)

	writeXMLResponse(recorder, request, http.StatusOK, struct {
		Unsupported chan struct{} `xml:"unsupported"`
	}{Unsupported: make(chan struct{})})

	assert.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestWriteS3ErrorResponseUsesRequestID(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	request = request.WithContext(context.WithValue(request.Context(), authentication.RequestIDContextKey{}, "request-id"))
	handler := httpmiddleware.MakeRequestContextMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeS3ErrorResponse(w, r, http.StatusBadRequest, "InvalidRequest", "invalid request", r.URL.Path)
	}))
	handler.ServeHTTP(recorder, request)

	result := recorder.Result()
	defer result.Body.Close()
	require.Equal(t, http.StatusBadRequest, result.StatusCode)
	assert.Contains(t, recorder.Body.String(), "<RequestId>request-id</RequestId>")
	assert.Contains(t, recorder.Body.String(), "<Resource>/bucket/key</Resource>")
}
