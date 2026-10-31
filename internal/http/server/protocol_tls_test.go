package server

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestRequestSchemeUsesTLSConnectionState(t *testing.T) {
	testutils.SkipIfIntegration(t)
	request := httptest.NewRequest("GET", "http://example.com/object", nil)
	assert.Equal(t, "http", getRequestScheme(request))

	request.TLS = new(tls.ConnectionState)
	assert.Equal(t, "https", getRequestScheme(request))
	assert.Equal(t, "https", makeAuthorizationHTTPRequest(request).Scheme)
}
