package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeAndValidateCORSRulesRejectsWildcardCredentials(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := NormalizeAndValidateCORSRules([]CORSRule{{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET"},
		AllowCredentials: true,
	}})
	require.Error(t, err)
}

func TestCORSMiddlewareHandlesPreflightAndActualRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)

	maxAge := 3600
	rules, err := NormalizeAndValidateCORSRules([]CORSRule{
		{
			AllowedOrigins: []string{"https://*.example.com"},
			AllowedMethods: []string{"GET", "PUT"},
			AllowedHeaders: []string{"content-type", "x-amz-*"},
			ExposeHeaders:  []string{"etag", "x-amz-version-id"},
			MaxAgeSeconds:  &maxAge,
		},
	})
	require.NoError(t, err)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	handler := MakeCORSMiddleware(rules, next)

	preflightReq := httptest.NewRequest(http.MethodOptions, "http://pithos.localhost/test-bucket/test-key", nil)
	preflightReq.Header.Set("Origin", "https://app.example.com")
	preflightReq.Header.Set("Access-Control-Request-Method", "PUT")
	preflightReq.Header.Set("Access-Control-Request-Headers", "Content-Type, X-Amz-Meta-Foo")
	preflightRes := httptest.NewRecorder()

	handler.ServeHTTP(preflightRes, preflightReq)

	assert.Equal(t, http.StatusOK, preflightRes.Code)
	assert.Equal(t, "https://app.example.com", preflightRes.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, PUT", preflightRes.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "content-type, x-amz-*", preflightRes.Header().Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "3600", preflightRes.Header().Get("Access-Control-Max-Age"))
	assert.Contains(t, preflightRes.Header().Values("Vary"), "Origin")

	actualReq := httptest.NewRequest(http.MethodGet, "http://pithos.localhost/test-bucket/test-key", nil)
	actualReq.Header.Set("Origin", "https://app.example.com")
	actualRes := httptest.NewRecorder()

	handler.ServeHTTP(actualRes, actualReq)

	assert.Equal(t, http.StatusCreated, actualRes.Code)
	assert.Equal(t, "https://app.example.com", actualRes.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "etag, x-amz-version-id", actualRes.Header().Get("Access-Control-Expose-Headers"))
}

func TestCORSMiddlewareRejectsUnmatchedPreflight(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rules, err := NormalizeAndValidateCORSRules([]CORSRule{
		{
			AllowedOrigins: []string{"https://allowed.example.com"},
			AllowedMethods: []string{"GET"},
			AllowedHeaders: []string{"content-type"},
		},
	})
	require.NoError(t, err)

	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})
	handler := MakeCORSMiddleware(rules, next)

	req := httptest.NewRequest(http.MethodOptions, "http://pithos.localhost/test-bucket/test-key", nil)
	req.Header.Set("Origin", "https://disallowed.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	assert.Equal(t, http.StatusForbidden, res.Code)
	assert.False(t, nextCalled)
	assert.Equal(t, "", res.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddlewareAppliesFirstMatchingRule(t *testing.T) {
	testutils.SkipIfIntegration(t)

	rules, err := NormalizeAndValidateCORSRules([]CORSRule{
		{
			AllowedOrigins: []string{"https://*.example.com"},
			AllowedMethods: []string{"GET"},
			ExposeHeaders:  []string{"x-first-rule"},
		},
		{
			AllowedOrigins: []string{"https://app.example.com"},
			AllowedMethods: []string{"GET"},
			ExposeHeaders:  []string{"x-second-rule"},
		},
	})
	require.NoError(t, err)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := MakeCORSMiddleware(rules, next)

	req := httptest.NewRequest(http.MethodGet, "http://pithos.localhost/test-bucket/test-key", nil)
	req.Header.Set("Origin", "https://app.example.com")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "x-first-rule", res.Header().Get("Access-Control-Expose-Headers"))
}
