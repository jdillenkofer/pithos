package authentication

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"slices"
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
	return newSignedPutRequestWithHeaders(t, declaredHash, signedPayloadHash, body, contentLength, nil)
}

// newSignedPutRequestWithHeaders is newSignedPutRequest with additional
// signed x-amz-* headers.
func newSignedPutRequestWithHeaders(t *testing.T, declaredHash string, signedPayloadHash string, body io.Reader, contentLength int64, headers map[string]string) *http.Request {
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
	for key, value := range headers {
		r.Header.Set(key, value)
		signedHeaders = append(signedHeaders, strings.ToLower(key))
	}
	slices.Sort(signedHeaders)

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
	assert.Equal(t, 0, n)
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

func TestCheckAuthenticationRequiresDecodedContentLengthForAwsChunked(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := newSignedPutRequest(t, contentSHA256StreamingUnsignedPayloadTrailing, contentSHA256StreamingUnsignedPayloadTrailing, failingReader{t: t}, 64)
	r.Header.Set("Content-Encoding", contentEncodingAwsChunked)
	_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
	require.NoError(t, providerErr)
	assert.Equal(t, errMissingDecodedContentLength, authErr)
}

func TestCheckAuthenticationDecodesStreamingPayloadWithoutAwsChunkedEncoding(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, contentSHA256 := range []string{contentSHA256StreamingPayload, contentSHA256StreamingPayloadTrailing, contentSHA256StreamingUnsignedPayloadTrailing} {
		t.Run(contentSHA256, func(t *testing.T) {
			r := newSignedPutRequest(t, contentSHA256, contentSHA256, failingReader{t: t}, 64)
			_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
			require.NoError(t, providerErr)
			assert.Equal(t, errMissingDecodedContentLength, authErr)
		})
	}
}

func TestCheckAuthenticationRejectsInvalidDecodedContentLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, decodedContentLength := range []string{"abc", "-1", "1.5"} {
		t.Run(decodedContentLength, func(t *testing.T) {
			r := newSignedPutRequestWithHeaders(t, contentSHA256StreamingUnsignedPayloadTrailing, contentSHA256StreamingUnsignedPayloadTrailing, failingReader{t: t}, 64,
				map[string]string{DecodedContentLengthHeader: decodedContentLength})
			_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
			require.NoError(t, providerErr)
			assert.Equal(t, errInvalidDecodedContentLength, authErr)
		})
	}
}

func TestCheckAuthenticationEnforcesDecodedContentLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
	tests := []struct {
		decodedContentLength string
		wantErr              error
	}{
		{decodedContentLength: "11"},
		{decodedContentLength: "12", wantErr: ErrIncompleteBody},
		{decodedContentLength: "8", wantErr: ErrMalformedChunk},
	}
	for _, tt := range tests {
		t.Run(tt.decodedContentLength, func(t *testing.T) {
			r := newSignedPutRequestWithHeaders(t, contentSHA256StreamingUnsignedPayload, contentSHA256StreamingUnsignedPayload, strings.NewReader(body), int64(len(body)),
				map[string]string{DecodedContentLengthHeader: tt.decodedContentLength})
			_, authErr, providerErr := checkAuthentication(payloadTestCredentials(), payloadTestRegion, r)
			require.NoError(t, providerErr)
			require.Nil(t, authErr)
			assert.Equal(t, tt.decodedContentLength, r.Header.Get("Content-Length"))

			data, err := io.ReadAll(r.Body)
			if tt.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, "hello world", string(data))
				return
			}
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Consumers that stop after the declared length never read to EOF. io.CopyN
// and an exactly sized io.ReadFull discard an error returned together with the
// last bytes, so the readers must withhold them.
func TestPayloadVerifiersFailConsumersStoppingAtDeclaredLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	declared := sha256.Sum256([]byte("hello world"))
	tests := []struct {
		name      string
		newReader func() io.Reader
		wantErr   error
	}{
		{
			name: "payload hash mismatch",
			newReader: func() io.Reader {
				return newPayloadHashVerifyingReadCloser(t.Context(), io.NopCloser(strings.NewReader("hello WORLD")), declared[:], 11)
			},
			wantErr: ErrContentSHA256Mismatch,
		},
		{
			name: "aws-chunked trailer checksum mismatch",
			newReader: func() io.Reader {
				return newUnsignedTrailerChunkReader("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n", "x-amz-checksum-crc32")
			},
			wantErr: ErrTrailerChecksumMismatch,
		},
		{
			name: "aws-chunked missing zero-length chunk",
			newReader: func() io.Reader {
				return newUnsignedTrailerChunkReader("b\r\nhello world\r\n", "x-amz-checksum-crc32")
			},
			wantErr: ErrIncompleteBody,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := io.CopyN(io.Discard, tt.newReader(), 11)
			assert.ErrorIs(t, err, tt.wantErr)

			_, err = io.ReadFull(tt.newReader(), make([]byte, 11))
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAwsChunkReaderReturnsEOFWithLastDeclaredBytes(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := newUnsignedTrailerChunkReader("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n", "x-amz-checksum-crc32")
	buf := make([]byte, 64)
	n, err := body.Read(buf)
	assert.Equal(t, 11, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, "hello world", string(buf[:n]))
}
