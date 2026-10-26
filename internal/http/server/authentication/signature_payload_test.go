package authentication

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	payloadTestAccessKeyID     = "test-access-key"
	payloadTestSecretAccessKey = "test-secret-key"
	payloadTestRegion          = "eu-central-1"
)

func payloadTestCredentials() staticCredentialProvider {
	return staticCredentialProvider{
		payloadTestAccessKeyID: {AccessKeyID: payloadTestAccessKeyID, SecretAccessKey: payloadTestSecretAccessKey, AccountID: "account", PrincipalID: "client"},
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// failingReader fails the test if the body is read before it is expected.
type failingReader struct {
	t *testing.T
}

func (r failingReader) Read(p []byte) (int, error) {
	r.t.Fatal("body must not be read")
	return 0, io.ErrUnexpectedEOF
}

// newSignedPutRequest signs a SigV4 header request whose canonical request
// uses signedPayloadHash and then attaches body.
func newSignedPutRequest(t *testing.T, declaredHash string, signedPayloadHash string, body io.Reader, contentLength int64) *http.Request {
	t.Helper()
	signingTime := time.Now().UTC().Truncate(time.Second)
	date := signingTime.Format("20060102")
	timestamp := signingTime.Format("20060102T150405Z")
	scope := createScope(date, payloadTestRegion, expectedService, expectedRequest)

	r, err := http.NewRequest(http.MethodPut, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	require.NoError(t, err)
	r.Header.Set("x-amz-date", timestamp)
	signedHeaders := []string{"host", "x-amz-date"}
	if declaredHash != "" {
		r.Header.Set(contentSHA256Header, declaredHash)
		signedHeaders = []string{"host", "x-amz-content-sha256", "x-amz-date"}
	}

	canonicalRequest := generateCanonicalHttpMethod(r) + "\n" +
		generateCanonicalURI(r) + "\n" +
		generateCanonicalQueryString(r) + "\n" +
		generateCanonicalHeaders(r, signedHeaders) + "\n" +
		generateSignedHeaders(r, signedHeaders) + "\n" +
		signedPayloadHash
	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := signatureAlgorithmV4 + "\n" + timestamp + "\n" + scope + "\n" + hex.EncodeToString(canonicalRequestHash[:])
	signingKey := createSigningKey(payloadTestSecretAccessKey, date, payloadTestRegion, expectedService, expectedRequest)
	signature := createSignature(signingKey, stringToSign)
	r.Header.Set("Authorization", signatureAlgorithmV4+" Credential="+payloadTestAccessKeyID+"/"+scope+",SignedHeaders="+strings.Join(signedHeaders, ";")+",Signature="+signature)

	r.Body = io.NopCloser(body)
	r.ContentLength = contentLength
	return r
}

func TestCheckAuthenticationVerifiesSignatureBeforeReadingDeclaredPayload(t *testing.T) {
	testutils.SkipIfIntegration(t)

	payload := []byte("hello payload")
	r := newSignedPutRequest(t, sha256Hex(payload), sha256Hex(payload), failingReader{t: t}, int64(len(payload)))
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	assert.Nil(t, authErr)
}

func TestCheckAuthenticationRejectsInvalidSignatureWithoutReadingPayload(t *testing.T) {
	testutils.SkipIfIntegration(t)

	payload := []byte("hello payload")
	r := newSignedPutRequest(t, sha256Hex(payload), sha256Hex([]byte("other")), failingReader{t: t}, int64(len(payload)))
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	assert.Equal(t, errSignatureDoesNotMatch, authErr)
}

func TestCheckAuthenticationStreamsMatchingDeclaredPayload(t *testing.T) {
	testutils.SkipIfIntegration(t)

	payload := []byte("hello payload")
	r := newSignedPutRequest(t, sha256Hex(payload), sha256Hex(payload), bytes.NewReader(payload), int64(len(payload)))
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	require.Nil(t, authErr)

	data, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
}

func TestCheckAuthenticationFailsReadOfMismatchingDeclaredPayload(t *testing.T) {
	testutils.SkipIfIntegration(t)

	declared := []byte("hello payload")
	tampered := []byte("HELLO payload")
	for _, contentLength := range []int64{int64(len(tampered)), -1} {
		r := newSignedPutRequest(t, sha256Hex(declared), sha256Hex(declared), bytes.NewReader(tampered), contentLength)
		_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
		require.NoError(t, providerErr)
		require.Nil(t, authErr)

		_, err := io.ReadAll(r.Body)
		assert.ErrorIs(t, err, ErrContentSHA256Mismatch)
	}
}

func TestPayloadHashVerifierFailsReadDeliveringLastByteOfSizedBody(t *testing.T) {
	testutils.SkipIfIntegration(t)

	declared := sha256.Sum256([]byte("abcd"))
	reader := newPayloadHashVerifyingReadCloser(t.Context(), io.NopCloser(strings.NewReader("abcX")), declared[:], 4)
	buf := make([]byte, 8)
	n, err := reader.Read(buf)
	assert.Equal(t, 4, n)
	assert.ErrorIs(t, err, ErrContentSHA256Mismatch)

	_, err = reader.Read(buf)
	assert.ErrorIs(t, err, ErrContentSHA256Mismatch)
}

func TestCheckAuthenticationRejectsMalformedDeclaredPayloadHash(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := newSignedPutRequest(t, "not-a-sha256", "not-a-sha256", failingReader{t: t}, 0)
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	assert.Equal(t, errInvalidContentSHA256, authErr)
}

func TestCheckAuthenticationRequiresContentSHA256ForHeaderSignedRequests(t *testing.T) {
	testutils.SkipIfIntegration(t)

	payload := []byte("hello payload")
	r := newSignedPutRequest(t, "", sha256Hex(payload), failingReader{t: t}, int64(len(payload)))
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	assert.Equal(t, errMissingContentSHA256, authErr)
}
