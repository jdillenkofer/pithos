package authentication

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestSignatureMiddlewareVerifiedAlgorithm(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, algorithm := range []signatureAlgorithm{signatureAlgorithmV4, signatureAlgorithmV4a} {
		for _, presigned := range []bool{false, true} {
			kind := "header"
			if presigned {
				kind = "presigned"
			}
			t.Run(string(algorithm)+"/"+kind, func(t *testing.T) {
				r := signedMetadataRequest(t, algorithm, presigned)
				called := false
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					auth := RequestAuthenticationFromContext(r.Context())
					require.True(t, auth.Authenticated)
					require.Equal(t, string(algorithm), auth.SignatureVersion)
					require.Equal(t, sigV4aTestAccessKey, auth.Identity.AccessKeyID)
					w.WriteHeader(http.StatusNoContent)
				})
				handler := MakeSignatureMiddleware(sigV4aTestCredentials(), "eu-central-1", next)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				require.True(t, called)
				require.Equal(t, http.StatusNoContent, w.Code)

				// An invalid signature must not publish any verified metadata.
				called = false
				r = signedMetadataRequest(t, algorithm, presigned)
				r.URL.Path = "/tampered"
				w = httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				require.False(t, called)
				require.Equal(t, http.StatusUnauthorized, w.Code)
			})
		}
	}
	t.Run("anonymous algorithm hint is not trusted", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256", nil)
		handler := MakeSignatureMiddleware(sigV4aTestCredentials(), "eu-central-1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := RequestAuthenticationFromContext(r.Context())
			require.False(t, auth.Authenticated)
			require.Empty(t, auth.SignatureVersion)
			w.WriteHeader(http.StatusNoContent)
		}))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		require.Equal(t, http.StatusNoContent, w.Code)
	})
}

func signedMetadataRequest(t *testing.T, algorithm signatureAlgorithm, presigned bool) *http.Request {
	t.Helper()
	now := time.Now().UTC()
	date, timestamp := now.Format("20060102"), now.Format("20060102T150405Z")
	scope := createScope(date, "eu-central-1", expectedService, expectedRequest)
	if algorithm == signatureAlgorithmV4a {
		scope = createSigV4aScope(date, expectedService, expectedRequest)
	}
	r := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	headers := []string{"host"}
	if presigned {
		query := r.URL.Query()
		query.Set("X-Amz-Algorithm", string(algorithm))
		query.Set("X-Amz-Credential", sigV4aTestAccessKey+"/"+scope)
		query.Set("X-Amz-Date", timestamp)
		query.Set("X-Amz-Expires", "3600")
		query.Set("X-Amz-SignedHeaders", "host")
		if algorithm == signatureAlgorithmV4a {
			query.Set("X-Amz-Region-Set", "eu-central-1")
		}
		r.URL.RawQuery = query.Encode()
	} else {
		r.Header.Set("x-amz-date", timestamp)
		headers = append(headers, "x-amz-date")
		if algorithm == signatureAlgorithmV4a {
			r.Header.Set("x-amz-region-set", "eu-central-1")
			headers = append(headers, "x-amz-region-set")
		}
	}
	toSign, err := generateStringToSign(r, timestamp, scope, headers, presigned, algorithm)
	require.NoError(t, err)
	var signature string
	if algorithm == signatureAlgorithmV4a {
		signature = signSigV4aString(t, *toSign)
	} else {
		key := createSigningKey(sigV4aTestSecretKey, date, "eu-central-1", expectedService, expectedRequest)
		signature = createSignature(key, *toSign)
	}
	if presigned {
		query := r.URL.Query()
		query.Set("X-Amz-Signature", signature)
		r.URL.RawQuery = query.Encode()
	} else {
		r.Header.Set("Authorization", string(algorithm)+" Credential="+sigV4aTestAccessKey+"/"+scope+",SignedHeaders="+strings.Join(headers, ";")+",Signature="+signature)
	}
	return r
}
