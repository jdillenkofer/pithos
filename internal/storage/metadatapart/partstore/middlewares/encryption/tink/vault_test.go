package tink

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock inner PartStore
type mockPartStore struct{}

func (m *mockPartStore) Start(ctx context.Context) error { return nil }
func (m *mockPartStore) Stop(ctx context.Context) error  { return nil }
func (m *mockPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	return nil
}
func (m *mockPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	return nil, nil
}
func (m *mockPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	return nil
}

func TestNewWithHCVault_AppRole(t *testing.T) {
	testutils.SkipIfIntegration(t)
	// Mock Vault Server
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/auth/approle/login":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   "new-token",
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/encrypt/"):
			w.WriteHeader(http.StatusOK)
			// Mock encryption response
			// Tink Vault AEAD expects specific response format
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"ciphertext": "vault:v1:" + base64.StdEncoding.EncodeToString([]byte("mock-ciphertext")),
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/decrypt/"):
			w.WriteHeader(http.StatusOK)
			// Mock decryption response
			// We need to return the original plaintext "test" for testKeyAvailability
			plaintext := base64.StdEncoding.EncodeToString([]byte("test"))
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"plaintext": plaintext,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Use a mock or temporary filesystem part store for the inner store
	innerStore := &mockPartStore{}
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	// Test successful creation
	mw, err := NewWithHCVault(ts.URL, "", "role-id", "secret-id", "transit/keys/my-key", innerStore, tlsConfig)
	require.NoError(t, err)
	assert.NotNil(t, mw)

	tinkMw, ok := mw.(*TinkEncryptionPartStoreMiddleware)
	require.True(t, ok)
	assert.Equal(t, "new-token", tinkMw.vaultToken)
	assert.Equal(t, KeyTypeVault, tinkMw.keyType)
	
	// Cleanup
	mw.Stop(context.Background())
}

func TestNewWithHCVault_Token(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/transit/encrypt/"):
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"ciphertext": "vault:v1:" + base64.StdEncoding.EncodeToString([]byte("mock-ciphertext")),
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/decrypt/"):
			w.WriteHeader(http.StatusOK)
			plaintext := base64.StdEncoding.EncodeToString([]byte("test"))
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"plaintext": plaintext,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	innerStore := &mockPartStore{}
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	mw, err := NewWithHCVault(ts.URL, "my-token", "", "", "transit/keys/my-key", innerStore, tlsConfig)
	require.NoError(t, err)
	assert.NotNil(t, mw)

	tinkMw, ok := mw.(*TinkEncryptionPartStoreMiddleware)
	require.True(t, ok)
	assert.Equal(t, "my-token", tinkMw.vaultToken)
	
	mw.Stop(context.Background())
}

func TestNewWithHCVault_InvalidArgs(t *testing.T) {
	testutils.SkipIfIntegration(t)
	innerStore := &mockPartStore{}
	
	_, err := NewWithHCVault("http://localhost:8200", "", "", "", "key", innerStore, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be provided")

	_, err = NewWithHCVault("http://localhost:8200", "token", "role", "secret", "key", innerStore, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use both")
}

func TestTinkMiddleware_RefreshVaultToken(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authCalls := 0
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/auth/approle/login":
			authCalls++
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   fmt.Sprintf("token-%d", authCalls),
					"lease_duration": 3600,
					"renewable":      true,
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/encrypt/"):
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"ciphertext": "vault:v1:" + base64.StdEncoding.EncodeToString([]byte("mock-ciphertext")),
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/decrypt/"):
			w.WriteHeader(http.StatusOK)
			plaintext := base64.StdEncoding.EncodeToString([]byte("test"))
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"plaintext": plaintext,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	innerStore := &mockPartStore{}
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	mw, err := NewWithHCVault(ts.URL, "", "role", "secret", "transit/keys/my-key", innerStore, tlsConfig)
	require.NoError(t, err)
	
	tinkMw, ok := mw.(*TinkEncryptionPartStoreMiddleware)
	require.True(t, ok)
	assert.Equal(t, "token-1", tinkMw.vaultToken)
	assert.Equal(t, 1, authCalls)

	// Trigger refresh manually
	err = tinkMw.refreshVaultToken()
	require.NoError(t, err)
	
	assert.Equal(t, "token-2", tinkMw.vaultToken)
	assert.Equal(t, 2, authCalls)
	
	mw.Stop(context.Background())
}
