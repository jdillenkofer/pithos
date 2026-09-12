package authentication

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setCredential(t *testing.T, index, accessKeyID, secretAccessKey string) {
	t.Helper()
	t.Setenv(credentialEnvPrefix+index+"_ACCESS_KEY_ID", accessKeyID)
	t.Setenv(credentialEnvPrefix+index+"_SECRET_ACCESS_KEY", secretAccessKey)
}

func setPrincipal(t *testing.T, index, principalID string) {
	t.Helper()
	t.Setenv(credentialEnvPrefix+index+"_PRINCIPAL_ID", principalID)
}

func clearCredentials(t *testing.T) {
	t.Helper()
	for i := 0; i <= 3; i++ {
		index := string(rune('0' + i))
		t.Setenv(credentialEnvPrefix+index+"_ACCESS_KEY_ID", "")
		t.Setenv(credentialEnvPrefix+index+"_SECRET_ACCESS_KEY", "")
		t.Setenv(credentialEnvPrefix+index+"_PRINCIPAL_ID", "")
	}
}

func TestEnvCredentialProviderLookup(t *testing.T) {
	t.Run("index zero", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		credential, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "key-0")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, Credential{AccessKeyID: "key-0", SecretAccessKey: "secret-0"}, credential)
	})

	t.Run("index one", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "1", "key-1", "secret-1")
		_, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "key-1")
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("multiple credentials", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		setCredential(t, "1", "key-1", "secret-1")
		credential, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "key-1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "secret-1", credential.SecretAccessKey)
	})

	t.Run("unknown key", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		_, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "unknown")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("incomplete pair stops lookup", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		t.Setenv(credentialEnvPrefix+"1_ACCESS_KEY_ID", "incomplete")
		setCredential(t, "2", "key-2", "secret-2")
		_, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "key-2")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("gap stops lookup", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		setCredential(t, "2", "key-2", "secret-2")
		_, found, err := NewEnvCredentialProvider().Lookup(context.Background(), "key-2")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("environment changes after construction", func(t *testing.T) {
		clearCredentials(t)
		provider := NewEnvCredentialProvider()
		setCredential(t, "0", "key", "old-secret")
		credential, found, err := provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "old-secret", credential.SecretAccessKey)
		t.Setenv(credentialEnvPrefix+"0_SECRET_ACCESS_KEY", "new-secret")
		credential, found, err = provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "new-secret", credential.SecretAccessKey)
	})

	t.Run("explicit and shared principal", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "old-key", "old-secret")
		setPrincipal(t, "0", "client")
		setCredential(t, "1", "new-key", "new-secret")
		setPrincipal(t, "1", "client")
		provider := NewEnvCredentialProvider()
		for _, key := range []string{"old-key", "new-key"} {
			credential, found, err := provider.Lookup(context.Background(), key)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, "client", credential.PrincipalID)
		}
	})

	t.Run("missing principal stays empty and changes dynamically", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key", "secret")
		provider := NewEnvCredentialProvider()
		credential, found, err := provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Empty(t, credential.PrincipalID)
		setPrincipal(t, "0", "new-principal")
		credential, found, err = provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "new-principal", credential.PrincipalID)
	})
}

type recordingCredentialProvider struct {
	calls int
	err   error
}

func (p *recordingCredentialProvider) Lookup(context.Context, string) (Credential, bool, error) {
	p.calls++
	return Credential{}, false, p.err
}

func TestSignatureMiddlewareCredentialProviderBehavior(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	t.Run("anonymous request bypasses provider", func(t *testing.T) {
		provider := &recordingCredentialProvider{err: errors.New("must not be called")}
		recorder := httptest.NewRecorder()
		MakeSignatureMiddleware(provider, "eu-central-1", next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.Equal(t, http.StatusNoContent, recorder.Code)
		assert.Zero(t, provider.calls)
	})

	t.Run("unknown access key is unauthorized", func(t *testing.T) {
		provider := &recordingCredentialProvider{}
		recorder := httptest.NewRecorder()
		request := signedLookingRequest()
		MakeSignatureMiddleware(provider, "eu-central-1", next).ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Equal(t, 1, provider.calls)
	})

	t.Run("provider error is internal server error", func(t *testing.T) {
		provider := &recordingCredentialProvider{err: errors.New("backend unavailable")}
		recorder := httptest.NewRecorder()
		MakeSignatureMiddleware(provider, "eu-central-1", next).ServeHTTP(recorder, signedLookingRequest())
		assert.Equal(t, http.StatusInternalServerError, recorder.Code)
		assert.Equal(t, 1, provider.calls)
	})
}

func signedLookingRequest() *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/20260912/eu-central-1/s3/aws4_request,SignedHeaders=host,Signature=invalid")
	request.Header.Set("X-Amz-Date", "20260912T120000Z")
	return request
}
