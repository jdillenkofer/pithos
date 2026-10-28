package tink

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go-awskms/v3/integration/awskms"
)

// fakeAWSKMS binds each ciphertext to its encryption context like AWS KMS does.
type fakeAWSKMS struct {
	mu       sync.Mutex
	contexts []map[string]string
}

type fakeAWSKMSCiphertext struct {
	Context   map[string]string
	Plaintext []byte
}

func (f *fakeAWSKMS) Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	f.mu.Lock()
	f.contexts = append(f.contexts, maps.Clone(params.EncryptionContext))
	f.mu.Unlock()
	blob, err := json.Marshal(fakeAWSKMSCiphertext{Context: params.EncryptionContext, Plaintext: params.Plaintext})
	if err != nil {
		return nil, err
	}
	return &kms.EncryptOutput{CiphertextBlob: blob, KeyId: params.KeyId}, nil
}

func (f *fakeAWSKMS) Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	var ciphertext fakeAWSKMSCiphertext
	if err := json.Unmarshal(params.CiphertextBlob, &ciphertext); err != nil {
		return nil, err
	}
	if !maps.Equal(ciphertext.Context, params.EncryptionContext) {
		return nil, errors.New("InvalidCiphertextException: encryption context mismatch")
	}
	return &kms.DecryptOutput{Plaintext: ciphertext.Plaintext, KeyId: params.KeyId}, nil
}

func roundTripPart(t *testing.T, mw partstore.PartStore, partId partstore.PartId) {
	content := []byte("kms wire format round trip")
	require.NoError(t, mw.PutPart(t.Context(), nil, partId, bytes.NewReader(content)))
	rc, err := mw.GetPart(t.Context(), nil, partId)
	require.NoError(t, err)
	defer rc.Close()
	decrypted, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, decrypted)
}

// Parts written before the tink-go migration used the "additionalData"
// encryption context name. AWS KMS refuses to decrypt their DEKs with any
// other name.
func TestAWSKMSKeepsLegacyEncryptionContextName(t *testing.T) {
	testutils.SkipIfIntegration(t)
	fakeKMS := &fakeAWSKMS{}
	keyURI := "aws-kms://arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
	mw, err := newWithAWSKMS(keyURI, newMemoryPartStore(), nil, awskms.WithKMS(fakeKMS))
	require.NoError(t, err)

	roundTripPart(t, mw, *legacyFixturePartId)

	require.NotEmpty(t, fakeKMS.contexts)
	partContext := fakeKMS.contexts[len(fakeKMS.contexts)-1]
	assert.Equal(t, map[string]string{"additionalData": hex.EncodeToString(legacyFixturePartId.Bytes())}, partContext)
}

// Parts written before the tink-go migration sent the associated data as
// Vault's "context" parameter. Vault requires the same context on decrypt
// for derived keys.
func TestHCVaultKeepsLegacyContextParameter(t *testing.T) {
	testutils.SkipIfIntegration(t)
	var mu sync.Mutex
	var encryptRequests []map[string]any
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/transit/encrypt/"):
			mu.Lock()
			encryptRequests = append(encryptRequests, body)
			mu.Unlock()
			sealed, _ := json.Marshal(body)
			json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"ciphertext": "vault:v1:" + base64.StdEncoding.EncodeToString(sealed)},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/decrypt/"):
			sealed, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(body["ciphertext"].(string), "vault:v1:"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var encryptBody map[string]any
			if err := json.Unmarshal(sealed, &encryptBody); err != nil || encryptBody["context"] != body["context"] {
				http.Error(w, "context mismatch", http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"plaintext": encryptBody["plaintext"]},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	mw, err := NewWithHCVault(ts.URL, "my-token", "", "", "transit/keys/my-key", newMemoryPartStore(), &tls.Config{InsecureSkipVerify: true}, nil)
	require.NoError(t, err)
	defer mw.Stop(context.Background())

	roundTripPart(t, mw, *legacyFixturePartId)

	require.NotEmpty(t, encryptRequests)
	partRequest := encryptRequests[len(encryptRequests)-1]
	assert.Equal(t, base64.StdEncoding.EncodeToString(legacyFixturePartId.Bytes()), partRequest["context"])
	assert.NotContains(t, partRequest, "associated_data")
}
