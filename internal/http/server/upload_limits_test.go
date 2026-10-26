package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
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
		{
			name:          "streaming payload without aws-chunked encoding",
			contentLength: 1024,
			headers: map[string]string{
				"X-Amz-Content-Sha256":         "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
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

func TestUploadsRequireDeclaredContentLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	require.NoError(t, err)
	handler := SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, &queryTestStorage{})

	for _, target := range []string{
		"http://s3.test/bucket/key",
		"http://s3.test/bucket/key?partNumber=1&uploadId=upload",
	} {
		t.Run(target, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, target, strings.NewReader("payload"))
			r.ContentLength = -1
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, http.StatusLengthRequired, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>MissingContentLength</Code>")
		})
	}

	r := httptest.NewRequest(http.MethodPut, "http://s3.test/bucket/key", strings.NewReader("payload"))
	r.Header.Set("Content-Length", "invalid")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidArgument</Code>")
}

type emptyCredentialProvider struct{}

func (emptyCredentialProvider) Lookup(context.Context, string) (authentication.Credential, bool, error) {
	return authentication.Credential{}, false, nil
}

// Errors raised before routing are HTML pages on the website endpoint and
// S3 XML errors on the API endpoint.
func TestPreRoutingErrorsMatchEndpoint(t *testing.T) {
	testutils.SkipIfIntegration(t)

	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	require.NoError(t, err)
	handler := SetupServer(emptyCredentialProvider{}, "us-east-1", "s3.test", "website.test", authorizer, &queryTestStorage{})

	oversized := func(target string) *http.Request {
		r := httptest.NewRequest(http.MethodPut, target, http.NoBody)
		r.ContentLength = storage.MaxEntitySize + 1
		return r
	}
	unknownAccessKey := func(target string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		date := time.Now().UTC()
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=unknown/"+date.Format("20060102")+"/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
		r.Header.Set("X-Amz-Date", date.Format("20060102T150405Z"))
		r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		return r
	}

	testCases := []struct {
		name        string
		request     *http.Request
		statusCode  int
		contentType string
		code        string
	}{
		{"oversized upload on api endpoint", oversized("http://s3.test/bucket/key"), http.StatusBadRequest, "application/xml", "<Code>EntityTooLarge</Code>"},
		{"oversized upload on website endpoint", oversized("http://bucket.website.test/key"), http.StatusBadRequest, "text/html", "Code: EntityTooLarge"},
		{"oversized upload on custom domain", oversized("http://www.example.com/key"), http.StatusBadRequest, "text/html", "Code: EntityTooLarge"},
		{"authentication failure on api endpoint", unknownAccessKey("http://s3.test/bucket/key"), http.StatusForbidden, "application/xml", "<Code>InvalidAccessKeyId</Code>"},
		{"authentication failure on website endpoint", unknownAccessKey("http://bucket.website.test/key"), http.StatusForbidden, "text/html", "Code: InvalidAccessKeyId"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, testCase.request)
			assert.Equal(t, testCase.statusCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), testCase.contentType)
			assert.Contains(t, w.Body.String(), testCase.code)
		})
	}
}

type singleCredentialProvider struct{}

func (singleCredentialProvider) Lookup(_ context.Context, accessKeyID string) (authentication.Credential, bool, error) {
	if accessKeyID != "key" {
		return authentication.Credential{}, false, nil
	}
	return authentication.Credential{AccessKeyID: "key", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "client"}, true, nil
}

func TestRequestTimeTooSkewedIncludesS3Details(t *testing.T) {
	testutils.SkipIfIntegration(t)

	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	require.NoError(t, err)
	handler := SetupServer(singleCredentialProvider{}, "us-east-1", "s3.test", "website.test", authorizer, &queryTestStorage{})

	signingTime := time.Now().UTC().Add(-time.Hour)
	r := httptest.NewRequest(http.MethodGet, "http://s3.test/bucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/"+signingTime.Format("20060102")+"/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
	r.Header.Set("X-Amz-Date", signingTime.Format("20060102T150405Z"))
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	var response ErrorResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, "RequestTimeTooSkewed", response.Code)
	assert.Equal(t, signingTime.Format("20060102T150405Z"), response.RequestTime)
	assert.NotEmpty(t, response.ServerTime)
	assert.Equal(t, int64(900000), response.MaxAllowedSkewMilliseconds)
	assert.Equal(t, w.Header().Get("x-amz-request-id"), response.RequestId)
}
