package authentication

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeCredentialsFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}

func TestFileCredentialProvider(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCredentialsFile(t, path, `{
		"credentials": [
			{"accessKeyId":"old-key","secretAccessKey":"old-secret","principalId":"client"},
			{"accessKeyId":"legacy-key","secretAccessKey":"legacy-secret"}
		]
	}`)

	provider, err := NewFileCredentialProvider(path, 10*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	credential, found, err := provider.Lookup(context.Background(), "old-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, Credential{AccessKeyID: "old-key", SecretAccessKey: "old-secret", PrincipalID: "client"}, credential)

	credential, found, err = provider.Lookup(context.Background(), "legacy-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Empty(t, credential.PrincipalID)

	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"new-key","secretAccessKey":"new-secret","principalId":"client"}]}`)
	require.Eventually(t, func() bool {
		credential, found, err = provider.Lookup(context.Background(), "new-key")
		return err == nil && found && credential.PrincipalID == "client"
	}, time.Second, 5*time.Millisecond)
	_, found, err = provider.Lookup(context.Background(), "old-key")
	require.NoError(t, err)
	assert.False(t, found)

	// A partial or malformed update must not replace the active snapshot.
	writeCredentialsFile(t, path, `{"credentials":[`)
	time.Sleep(25 * time.Millisecond)
	credential, found, err = provider.Lookup(context.Background(), "new-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "new-secret", credential.SecretAccessKey)

	// An explicit empty set is valid and revokes all credentials.
	writeCredentialsFile(t, path, `{"credentials":[]}`)
	require.Eventually(t, func() bool {
		_, found, err = provider.Lookup(context.Background(), "new-key")
		return err == nil && !found
	}, time.Second, 5*time.Millisecond)
}

func TestFileCredentialProviderReloadInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"key","secretAccessKey":"old"}]}`)
	provider, err := NewFileCredentialProvider(path, time.Hour)
	require.NoError(t, err)

	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"key","secretAccessKey":"new"}]}`)
	credential, found, err := provider.Lookup(context.Background(), "key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "old", credential.SecretAccessKey)
}

func TestFileCredentialProviderZeroReloadIntervalKeepsStartupSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"key","secretAccessKey":"old"}]}`)
	provider, err := NewFileCredentialProvider(path, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"key","secretAccessKey":"new"}]}`)
	time.Sleep(25 * time.Millisecond)
	credential, found, err := provider.Lookup(context.Background(), "key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "old", credential.SecretAccessKey)
}

func TestFileCredentialProviderRejectsInvalidInitialFile(t *testing.T) {
	tests := map[string]string{
		"missing credentials":   `{}`,
		"null credentials":      `{"credentials":null}`,
		"unknown field":         `{"credentials":[],"extra":true}`,
		"incomplete credential": `{"credentials":[{"accessKeyId":"key"}]}`,
		"duplicate access key":  `{"credentials":[{"accessKeyId":"key","secretAccessKey":"one"},{"accessKeyId":"key","secretAccessKey":"two"}]}`,
		"multiple documents":    `{"credentials":[]} {"credentials":[]}`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.json")
			writeCredentialsFile(t, path, contents)
			_, err := NewFileCredentialProvider(path, time.Second)
			assert.Error(t, err)
		})
	}
}

func TestFileCredentialProviderHonorsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCredentialsFile(t, path, `{"credentials":[]}`)
	provider, err := NewFileCredentialProvider(path, 0)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = provider.Lookup(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
}

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

func newEnvCredentialProvider(t *testing.T) *EnvCredentialProvider {
	t.Helper()
	provider, err := NewEnvCredentialProvider()
	require.NoError(t, err)
	return provider
}

func TestEnvCredentialProviderLookup(t *testing.T) {
	t.Run("index zero", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		credential, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "key-0")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, Credential{AccessKeyID: "key-0", SecretAccessKey: "secret-0"}, credential)
	})

	t.Run("index one", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "1", "key-1", "secret-1")
		_, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "key-1")
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("multiple credentials", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		setCredential(t, "1", "key-1", "secret-1")
		credential, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "key-1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "secret-1", credential.SecretAccessKey)
	})

	t.Run("unknown key", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		_, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "unknown")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("incomplete pair stops lookup", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		t.Setenv(credentialEnvPrefix+"1_ACCESS_KEY_ID", "incomplete")
		setCredential(t, "2", "key-2", "secret-2")
		_, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "key-2")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("gap stops lookup", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key-0", "secret-0")
		setCredential(t, "2", "key-2", "secret-2")
		_, found, err := newEnvCredentialProvider(t).Lookup(context.Background(), "key-2")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("environment changes after construction require a new provider", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key", "old-secret")
		provider := newEnvCredentialProvider(t)
		credential, found, err := provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "old-secret", credential.SecretAccessKey)
		t.Setenv(credentialEnvPrefix+"0_SECRET_ACCESS_KEY", "new-secret")
		credential, found, err = provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "old-secret", credential.SecretAccessKey)

		credential, found, err = newEnvCredentialProvider(t).Lookup(context.Background(), "key")
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
		provider := newEnvCredentialProvider(t)
		for _, key := range []string{"old-key", "new-key"} {
			credential, found, err := provider.Lookup(context.Background(), key)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, "client", credential.PrincipalID)
		}
	})

	t.Run("missing principal stays empty until a new provider is created", func(t *testing.T) {
		clearCredentials(t)
		setCredential(t, "0", "key", "secret")
		provider := newEnvCredentialProvider(t)
		credential, found, err := provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Empty(t, credential.PrincipalID)
		setPrincipal(t, "0", "new-principal")
		credential, found, err = provider.Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Empty(t, credential.PrincipalID)

		credential, found, err = newEnvCredentialProvider(t).Lookup(context.Background(), "key")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "new-principal", credential.PrincipalID)
	})
}

func TestEnvCredentialProviderRejectsInvalidCredentials(t *testing.T) {
	tests := map[string]func(*testing.T){
		"long access key ID": func(t *testing.T) {
			setCredential(t, "0", strings.Repeat("a", MaxAccessKeyIDLength+1), "secret")
		},
		"long secret access key": func(t *testing.T) {
			setCredential(t, "0", "key", strings.Repeat("s", MaxSecretAccessKeyLength+1))
		},
		"long principal ID": func(t *testing.T) {
			setCredential(t, "0", "key", "secret")
			setPrincipal(t, "0", strings.Repeat("p", MaxPrincipalIDLength+1))
		},
	}
	for name, configure := range tests {
		t.Run(name, func(t *testing.T) {
			clearCredentials(t)
			configure(t)
			_, err := NewEnvCredentialProvider()
			require.ErrorContains(t, err, "environment credential 0")
		})
	}
}

func TestValidateCredentialLengths(t *testing.T) {
	valid := Credential{
		AccessKeyID:     strings.Repeat("a", MaxAccessKeyIDLength),
		SecretAccessKey: strings.Repeat("s", MaxSecretAccessKeyLength),
		PrincipalID:     strings.Repeat("p", MaxPrincipalIDLength),
	}
	require.NoError(t, validateCredential(valid))

	tests := map[string]Credential{
		"empty access key ID":     {SecretAccessKey: "secret"},
		"long access key ID":      {AccessKeyID: strings.Repeat("a", MaxAccessKeyIDLength+1), SecretAccessKey: "secret"},
		"empty secret access key": {AccessKeyID: "key"},
		"long secret access key":  {AccessKeyID: "key", SecretAccessKey: strings.Repeat("s", MaxSecretAccessKeyLength+1)},
		"long principal ID":       {AccessKeyID: "key", SecretAccessKey: "secret", PrincipalID: strings.Repeat("p", MaxPrincipalIDLength+1)},
	}
	for name, credential := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, validateCredential(credential))
		})
	}

	valid.PrincipalID = ""
	require.NoError(t, validateCredential(valid))
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
	t.Run("anonymous request bypasses provider", func(t *testing.T) {
		provider := &recordingCredentialProvider{err: errors.New("must not be called")}
		recorder := httptest.NewRecorder()
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := RequestAuthenticationFromContext(r.Context())
			assert.False(t, auth.Authenticated)
			assert.Nil(t, auth.Identity)
			assert.Equal(t, AuthTypeAnonymous, auth.Type)
			w.WriteHeader(http.StatusNoContent)
		})
		MakeSignatureMiddleware(provider, "eu-central-1", next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.Equal(t, http.StatusNoContent, recorder.Code)
		assert.Zero(t, provider.calls)
	})

	t.Run("unknown access key is unauthorized", func(t *testing.T) {
		provider := &recordingCredentialProvider{}
		recorder := httptest.NewRecorder()
		request := signedLookingRequest()
		MakeSignatureMiddleware(provider, "eu-central-1", http.NotFoundHandler()).ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Equal(t, 1, provider.calls)
	})

	t.Run("overlong access key is unauthorized without provider lookup", func(t *testing.T) {
		provider := &recordingCredentialProvider{}
		recorder := httptest.NewRecorder()
		request := signedLookingRequest()
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+strings.Repeat("a", MaxAccessKeyIDLength+1)+"/20260912/eu-central-1/s3/aws4_request,SignedHeaders=host,Signature=invalid")
		MakeSignatureMiddleware(provider, "eu-central-1", http.NotFoundHandler()).ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Zero(t, provider.calls)
	})

	t.Run("provider error is internal server error", func(t *testing.T) {
		provider := &recordingCredentialProvider{err: errors.New("backend unavailable")}
		recorder := httptest.NewRecorder()
		MakeSignatureMiddleware(provider, "eu-central-1", http.NotFoundHandler()).ServeHTTP(recorder, signedLookingRequest())
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
