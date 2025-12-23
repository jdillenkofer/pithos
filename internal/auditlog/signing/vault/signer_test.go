package vault

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestNewSigner_AppRole(t *testing.T) {
	testutils.SkipIfIntegration(t)
	// Mock Vault Server
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/approle/login" {
			// Verify AppRole login request
			var req map[string]string
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req["role_id"] != "my-role-id" || req["secret_id"] != "my-secret-id" {
				http.Error(w, "invalid credentials", http.StatusUnauthorized)
				return
			}

			// Return fake auth response
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "new-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	signer, err := NewSigner(ts.URL, "", "my-role-id", "my-secret-id", "my-key", "transit", tlsConfig)
	require.NoError(t, err)
	defer signer.Close()

	assert.Equal(t, "my-key", signer.keyName)
	assert.Equal(t, "transit", signer.mountPath)
	// Check token logic indirectly via internal state or subsequent calls?
	// The token is set in the client, but api.Client doesn't expose it easily.
	// But AuthenticateWithAppRole returning correct token implies success.
}

func TestNewSigner_Token(t *testing.T) {
	testutils.SkipIfIntegration(t)
	signer, err := NewSigner("http://localhost:8200", "my-token", "", "", "my-key", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "transit", signer.mountPath) // Default
	assert.Equal(t, "my-key", signer.keyName)
}

func TestSigner_Sign(t *testing.T) {
	testutils.SkipIfIntegration(t)
	expectedSignature := []byte("test-signature")
	expectedSignatureBase64 := base64.StdEncoding.EncodeToString(expectedSignature)
	vaultSignature := "vault:v1:" + expectedSignatureBase64

	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transit/sign/my-key" {
			// Check Input
			var req map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			input, ok := req["input"].(string)
			if !ok {
				http.Error(w, "missing input", http.StatusBadRequest)
				return
			}
			
			data, err := base64.StdEncoding.DecodeString(input)
			if err != nil {
				http.Error(w, "invalid base64", http.StatusBadRequest)
				return
			}
			
			if string(data) != "data-to-sign" {
				http.Error(w, "wrong data", http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"signature": vaultSignature,
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	// Initialize with token to skip auth call
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	signer, err := NewSigner(ts.URL, "valid-token", "", "", "my-key", "transit", tlsConfig)
	require.NoError(t, err)

	sig, err := signer.Sign([]byte("data-to-sign"))
	require.NoError(t, err)
	assert.Equal(t, expectedSignature, sig)
}

func TestNewSigner_InvalidArgs(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := NewSigner("http://localhost:8200", "", "", "", "key", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be provided")

	_, err = NewSigner("http://localhost:8200", "token", "role", "secret", "key", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use both")
}

func TestSigner_RefreshToken(t *testing.T) {
	testutils.SkipIfIntegration(t)
	// This test is tricky because it involves time and background goroutines.
	// We can try to trigger it manually or verify the logic in unit test fashion if we expose things, 
	// but strictly black-box testing the auto-refresh is hard without waiting.
	// For now, let's verify that refreshToken method works (it's private, but we can call it via reflection or if we are in the same package).
	// Since we are package vault, we can access private members.

	authCalls := 0
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/approle/login" {
			authCalls++
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   fmt.Sprintf("token-%d", authCalls),
					"lease_duration": 2, // Short duration
					"renewable":      true,
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	signer, err := NewSigner(ts.URL, "", "role", "secret", "key", "transit", tlsConfig)
	require.NoError(t, err)
	defer signer.Close()

	assert.Equal(t, 1, authCalls)

	// Manually trigger refresh
	err = signer.refreshToken()
	require.NoError(t, err)
	assert.Equal(t, 2, authCalls)
}

