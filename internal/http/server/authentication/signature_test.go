package authentication

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sigV4aTestAccessKey = "AKISORANDOMAASORANDOM"
	sigV4aTestSecretKey = "q+jcrXGc+0zWN6uzclKVhvMmUsIfRPa4rlRandom"
)

func TestCreateSignature(t *testing.T) {
	testutils.SkipIfIntegration(t)
	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	stringToSign := "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9"
	signingKey := createSigningKey(secretAccessKey, "20151229", "us-east-1", "s3", "aws4_request")
	signature := createSignature(signingKey, stringToSign)
	assert.Equal(t, "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e", signature)
}

func TestCreateSignatureFromRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)
	var r *http.Request = &http.Request{}
	r.Method = "GET"
	r.URL = &url.URL{}
	r.URL.Path = "/test.txt"
	r.Host = "examplebucket.s3.amazonaws.com"
	r.Header = http.Header{}
	r.Header.Add("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	r.Header.Add("Range", "bytes=0-9")
	r.Header.Add("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	r.Header.Add("x-amz-date", "20130524T000000Z")
	r.Body = io.NopCloser(bytes.NewReader([]byte{}))

	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	date := "20130524"
	region := "us-east-1"
	service := "s3"
	request := "aws4_request"

	scope := createScope(date, region, service, request)
	assert.Equal(t, "20130524/us-east-1/s3/aws4_request", scope)

	isPresigned := false

	stringToSign, err := generateStringToSign(r, date+"T000000Z", scope, []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}, isPresigned, signatureAlgorithmV4)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	signature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", signature)
}

func TestCreateSeedSignatureFromAwsChunkRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)

	// Example from https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
	var r *http.Request = &http.Request{}
	r.Method = "PUT"
	r.URL = &url.URL{}
	r.URL.Path = "/examplebucket/chunkObject.txt"
	r.Host = "s3.amazonaws.com"
	r.Header = http.Header{}
	r.Header.Add("x-amz-date", "20130524T000000Z")
	r.Header.Add("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Add("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
	r.Header.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	r.Header.Add("Content-Encoding", "aws-chunked")
	r.Header.Add("x-amz-decoded-content-length", "66560")
	r.Header.Add("Content-Length", "66824")
	content := []byte(
		"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n" + strings.Repeat("a", 65536) + "\r\n" +
			"400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n" + strings.Repeat("a", 1024) + "\r\n" +
			"0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n")

	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	date := "20130524"
	region := "us-east-1"
	service := "s3"
	request := "aws4_request"

	scope := createScope(date, region, service, request)
	assert.Equal(t, "20130524/us-east-1/s3/aws4_request", scope)

	isPresigned := false

	timestamp := date + "T000000Z"
	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"content-encoding", "content-length", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class"}, isPresigned, signatureAlgorithmV4)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\ncee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	expectedSignature := "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
	hasTrailingHeader := false
	hasTrailingHeaderWithSignature := false
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, newSigV4Verifier(signingKey), hasTrailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation, "")

	seedSignature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, expectedSignature, seedSignature)

	data, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte(strings.Repeat("a", 65536+1024)), data)

	err = r.Body.Close()
	assert.NoError(t, err)
}

func TestCreateSeedSignatureFromAwsChunkRequestWithTrailingHeader(t *testing.T) {
	testutils.SkipIfIntegration(t)

	var r *http.Request = &http.Request{}
	r.Method = "PUT"
	r.URL = &url.URL{}
	r.URL.Path = "/examplebucket/chunkObject.txt"
	r.Host = "s3.amazonaws.com"
	r.Header = http.Header{}
	r.Header.Add("x-amz-date", "20130524T000000Z")
	r.Header.Add("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Add("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e")
	r.Header.Add("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	r.Header.Add("Content-Encoding", "aws-chunked")
	r.Header.Add("x-amz-decoded-content-length", "66560")
	r.Header.Add("x-amz-trailer", "x-amz-checksum-crc32c")
	r.Header.Add("Content-Length", "66824")
	content := []byte(
		"10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n" + strings.Repeat("a", 65536) + "\r\n" +
			"400;chunk-signature=1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\r\n" + strings.Repeat("a", 1024) + "\r\n" +
			"0;chunk-signature=2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\r\n\r\n" +
			"x-amz-checksum-crc32c:sOO8/Q==\r\n" +
			"x-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n")

	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	date := "20130524"
	region := "us-east-1"
	service := "s3"
	request := "aws4_request"

	scope := createScope(date, region, service, request)
	assert.Equal(t, "20130524/us-east-1/s3/aws4_request", scope)

	isPresigned := false

	timestamp := date + "T000000Z"
	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class", "x-amz-trailer"}, isPresigned, signatureAlgorithmV4)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	expectedSignature := "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
	hasTrailingHeader := true
	hasTrailingHeaderWithSignature := true
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, newSigV4Verifier(signingKey), hasTrailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation, "x-amz-checksum-crc32c")

	seedSignature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, expectedSignature, seedSignature)

	data, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte(strings.Repeat("a", 65536+1024)), data)

	err = r.Body.Close()
	assert.NoError(t, err)
}

// Same as TestCreateSeedSignatureFromAwsChunkRequestWithTrailingHeader, but
// with the trailer section directly following the zero-length chunk without a
// blank line in between, which is the framing the AWS SDKs send (RFC 7230
// chunked trailer part).
func TestCreateSeedSignatureFromAwsChunkRequestWithTrailingHeaderWithoutBlankLine(t *testing.T) {
	testutils.SkipIfIntegration(t)

	content := []byte(
		"10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n" + strings.Repeat("a", 65536) + "\r\n" +
			"400;chunk-signature=1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\r\n" + strings.Repeat("a", 1024) + "\r\n" +
			"0;chunk-signature=2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\r\n" +
			"x-amz-checksum-crc32c:sOO8/Q==\r\n" +
			"x-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n" +
			"\r\n")

	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	scope := createScope("20130524", "us-east-1", "s3", "aws4_request")
	signingKey := createSigningKey(secretAccessKey, "20130524", "us-east-1", "s3", "aws4_request")
	seedSignature := "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"

	body := newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), "20130524T000000Z", scope, seedSignature, newSigV4Verifier(signingKey), true, true, false, "x-amz-checksum-crc32c")

	data, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.Equal(t, []byte(strings.Repeat("a", 65536+1024)), data)
}

func TestAwsChunkedContentEncodingHelpers(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name             string
		contentEncoding  string
		wantIsAwsChunked bool
		wantStripped     string
	}{
		{name: "empty", contentEncoding: "", wantIsAwsChunked: false, wantStripped: ""},
		{name: "other encoding", contentEncoding: "gzip", wantIsAwsChunked: false, wantStripped: "gzip"},
		{name: "aws chunked only", contentEncoding: "aws-chunked", wantIsAwsChunked: true, wantStripped: ""},
		{name: "aws chunked then gzip", contentEncoding: "aws-chunked,gzip", wantIsAwsChunked: true, wantStripped: "gzip"},
		{name: "aws chunked then spaced gzip", contentEncoding: "aws-chunked, gzip", wantIsAwsChunked: true, wantStripped: "gzip"},
		{name: "aws chunked with surrounding whitespace", contentEncoding: " aws-chunked , gzip, br ", wantIsAwsChunked: true, wantStripped: "gzip, br"},
		{name: "aws chunked uppercase", contentEncoding: "AWS-CHUNKED, gzip", wantIsAwsChunked: true, wantStripped: "gzip"},
		{name: "aws chunked not first", contentEncoding: "gzip, aws-chunked", wantIsAwsChunked: false, wantStripped: "gzip, aws-chunked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantIsAwsChunked, hasAwsChunkedContentEncoding(tt.contentEncoding))
			assert.Equal(t, tt.wantStripped, stripAwsChunkedContentEncoding(tt.contentEncoding))
		})
	}
}

// newUnsignedTrailerChunkReader builds a reader for the
// STREAMING-UNSIGNED-PAYLOAD-TRAILER format, where chunks carry no signatures
// and only the trailer checksum is validated.
func newUnsignedTrailerChunkReader(content string, trailerName string) io.ReadCloser {
	return newAwsChunkReadCloser(context.Background(), io.NopCloser(strings.NewReader(content)), "", "", "", newSigV4Verifier(nil), true, false, true, trailerName)
}

func TestAwsChunkReaderValidatesUnsignedTrailerChecksum(t *testing.T) {
	testutils.SkipIfIntegration(t)

	// crc32 of "hello world" is DUoRhQ==
	body := newUnsignedTrailerChunkReader(
		"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n",
		"x-amz-checksum-crc32")
	data, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello world"), data)
}

func TestAwsChunkReaderRejectsWrongUnsignedTrailerChecksum(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := newUnsignedTrailerChunkReader(
		"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n",
		"x-amz-checksum-crc32")
	_, err := io.ReadAll(body)
	assert.ErrorIs(t, err, ErrTrailerChecksumMismatch)
}

func TestAwsChunkReaderRejectsTrailerNotMatchingDeclaration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := newUnsignedTrailerChunkReader(
		"b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=\r\n\r\n",
		"x-amz-checksum-crc32")
	_, err := io.ReadAll(body)
	assert.ErrorIs(t, err, ErrMalformedTrailer)
}

func TestAwsChunkReaderRejectsUnsupportedChecksumTrailer(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := newUnsignedTrailerChunkReader(
		"b\r\nhello world\r\n0\r\nx-amz-checksum-foo:AAAAAA==\r\n\r\n",
		"x-amz-checksum-foo")
	_, err := io.ReadAll(body)
	assert.ErrorIs(t, err, ErrMalformedTrailer)
}

func TestAwsChunkReaderRejectsMissingDeclaredTrailer(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := newUnsignedTrailerChunkReader(
		"b\r\nhello world\r\n0\r\n\r\n",
		"x-amz-checksum-crc32")
	_, err := io.ReadAll(body)
	assert.ErrorIs(t, err, ErrMalformedTrailer)
}

func TestCreateSignatureFromPresignedRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)

	var r *http.Request = &http.Request{}
	r.Method = "GET"
	r.URL = &url.URL{}
	r.URL.Path = "/test.txt"
	r.URL.RawQuery = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	r.Host = "examplebucket.s3.amazonaws.com"
	r.Header = http.Header{}
	r.Body = io.NopCloser(bytes.NewReader([]byte{}))

	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	date := "20130524"
	region := "us-east-1"
	service := "s3"
	request := "aws4_request"

	scope := createScope(date, region, service, request)
	assert.Equal(t, "20130524/us-east-1/s3/aws4_request", scope)

	isPresigned := true

	stringToSign, err := generateStringToSign(r, date+"T000000Z", scope, []string{"host"}, isPresigned, signatureAlgorithmV4)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n3bfa292879f6447bbcda7001decf97f4a54dc650c8942174ae0a9121cf58ad04", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	signature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", signature)
}

func TestCheckAuthenticationAcceptsPresignedRequestFromPreviousUTCDate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	const accessKeyID = "test-access-key"
	const secretAccessKey = "test-secret-key"
	const region = "eu-central-1"
	signingTime := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	date := signingTime.Format("20060102")
	timestamp := signingTime.Format("20060102T150405Z")
	scope := createScope(date, region, expectedService, expectedRequest)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	assert.NoError(t, err)
	query := r.URL.Query()
	query.Set("X-Amz-Algorithm", signatureAlgorithmV4)
	query.Set("X-Amz-Credential", accessKeyID+"/"+scope)
	query.Set("X-Amz-Date", timestamp)
	query.Set("X-Amz-Expires", "604800")
	query.Set("X-Amz-SignedHeaders", "host")
	r.URL.RawQuery = query.Encode()

	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"host"}, true, signatureAlgorithmV4)
	assert.NoError(t, err)
	signingKey := createSigningKey(secretAccessKey, date, region, expectedService, expectedRequest)
	query.Set("X-Amz-Signature", createSignature(signingKey, *stringToSign))
	r.URL.RawQuery = query.Encode()

	usedAccessKeyID, authenticated := checkAuthentication([]Credentials{{
		AccessKeyId:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}}, region, r)
	assert.True(t, authenticated)
	if assert.NotNil(t, usedAccessKeyID) {
		assert.Equal(t, accessKeyID, *usedAccessKeyID)
	}
}

func TestGenerateCanonicalHeadersIncludesOnlySignedHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodPut, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	assert.NoError(t, err)
	r.Host = "examplebucket.s3.amazonaws.com"
	r.Header.Add("Content-Type", "application/octet-stream")
	r.Header.Add("X-Amz-Meta-Test", "meta-value")
	r.Header.Add("X-Amz-Date", "20130524T000000Z")

	headersToInclude := []string{"host", "x-amz-date"}
	canonicalHeaders := generateCanonicalHeaders(r, headersToInclude)
	signedHeaders := generateSignedHeaders(r, headersToInclude)

	assert.Equal(t, "host:examplebucket.s3.amazonaws.com\nx-amz-date:20130524T000000Z\n", canonicalHeaders)
	assert.Equal(t, "host;x-amz-date", signedHeaders)
}

func TestGenerateCanonicalQueryStringSortsByKeyThenValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt?b=2&a=2&a=1", nil)
	assert.NoError(t, err)

	queryString := generateCanonicalQueryString(r)
	assert.Equal(t, "a=1&a=2&b=2", queryString)
}

func TestGenerateCanonicalQueryStringUsesAwsUriEncoding(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt?prefix=a b*~", nil)
	assert.NoError(t, err)

	queryString := generateCanonicalQueryString(r)
	assert.Equal(t, "prefix=a%20b%2A~", queryString)
}

func TestGenerateCanonicalQueryStringSortsAfterEncoding(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt?z=1&%C3%A4=1", nil)
	assert.NoError(t, err)

	queryString := generateCanonicalQueryString(r)
	assert.Equal(t, "%C3%A4=1&z=1", queryString)
}

func TestGenerateCanonicalURIUsesAwsStyleEscaping(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodPut, "http://examplebucket.s3.amazonaws.com/test$file.text", nil)
	assert.NoError(t, err)

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/test%24file.text", canonicalURI)
}

func TestGenerateCanonicalURIDoesNotNormalizePath(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/my-object//example//photo.user", nil)
	assert.NoError(t, err)

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/my-object//example//photo.user", canonicalURI)
}

func TestGenerateCanonicalURIUsesRawPathWhenPresent(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com", nil)
	assert.NoError(t, err)
	r.URL.Path = "/photos/month/sample.jpg"
	r.URL.RawPath = "/photos%2Fmonth%2Fsample.jpg"

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/photos%2Fmonth%2Fsample.jpg", canonicalURI)
}

func TestGenerateCanonicalURIEncodesReservedPathChars(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/photos/month/a b+c*.txt", nil)
	assert.NoError(t, err)

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/photos/month/a%20b%2Bc%2A.txt", canonicalURI)
}

func TestGenerateCanonicalURINormalizesPercentEscapesToUppercase(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com", nil)
	assert.NoError(t, err)
	r.URL.Path = "/photos/month/sample.jpg"
	r.URL.RawPath = "/photos%2fmonth%2fsample.jpg"

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/photos%2Fmonth%2Fsample.jpg", canonicalURI)
}

func TestGenerateCanonicalURIReturnsSlashForEmptyPath(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com", nil)
	assert.NoError(t, err)
	r.URL.Path = ""
	r.URL.RawPath = ""

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/", canonicalURI)
}

func TestGenerateCanonicalURIEncodesInvalidPercentSequences(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com", nil)
	assert.NoError(t, err)
	r.URL.Path = "/photos/%zz/sample"
	r.URL.RawPath = "/photos/%zz/sample"

	canonicalURI := generateCanonicalURI(r)
	assert.Equal(t, "/photos/%25zz/sample", canonicalURI)
}

func TestDeriveSigV4aPrivateKeyMatchesAWSSDK(t *testing.T) {
	testutils.SkipIfIntegration(t)

	privateKey, err := deriveSigV4aPrivateKey(sigV4aTestAccessKey, sigV4aTestSecretKey)
	require.NoError(t, err)

	assert.Equal(t, "15d242ceebf8d8169fd6a8b5a746c41140414c3b07579038da06af89190fffcb", privateKey.X.Text(16))
	assert.Equal(t, "515242cedd82e94799482e4c0514b505afccf2c0c98d6a553bf539f424c5ec0", privateKey.Y.Text(16))
}

func TestVerifySigV4aRequestSignedByAWSSDK(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt?foo=bar", nil)
	require.NoError(t, err)
	r.Body = io.NopCloser(strings.NewReader(""))
	r.Header.Set(contentSHA256Header, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	r.Header.Set("x-amz-date", "20250102T030405Z")
	r.Header.Set("x-amz-region-set", "eu-central-1,us-west-*")

	scope := createSigV4aScope("20250102", expectedService, expectedRequest)
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-region-set"}
	stringToSign, err := generateStringToSign(r, "20250102T030405Z", scope, signedHeaders, false, signatureAlgorithmV4a)
	require.NoError(t, err)

	publicKey, err := deriveSigV4aPublicKey(sigV4aTestAccessKey, sigV4aTestSecretKey)
	require.NoError(t, err)
	// Generated by github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.32.
	signature := "3045022077ba5b0d7900c35abf41256f3c7030dee504231989f0bb1ffa0ff074d543c9d9022100f67c433ff715e91d3ee04c3244cd1e339b3479eb8522a9fdf9c45f82918a5dbe"
	assert.True(t, verifySigV4aSignature(publicKey, *stringToSign, signature))
	assert.False(t, verifySigV4aSignature(publicKey, *stringToSign+"tampered", signature))
	assert.False(t, verifySigV4aSignature(publicKey, *stringToSign, signature+"**"))
}

func TestVerifyPresignedSigV4aRequestSignedByAWSSDK(t *testing.T) {
	testutils.SkipIfIntegration(t)

	signedURL := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=AKISORANDOMAASORANDOM%2F20250102%2Fs3%2Faws4_request&X-Amz-Date=20250102T030405Z&X-Amz-Expires=3600&X-Amz-Region-Set=eu-central-1%2Cus-west-%2A&X-Amz-SignedHeaders=host&foo=bar&X-Amz-Signature=3046022100f391173626ef340486cb9b03eb8390f32227eda713fb2a37e6d24ce8512e25120221009da4aa5214cbc05f4ec8eb3b8195af6cf747db16e7d590dd2b0428c214c2986e"
	r, err := http.NewRequest(http.MethodGet, signedURL, nil)
	require.NoError(t, err)

	scope := createSigV4aScope("20250102", expectedService, expectedRequest)
	stringToSign, err := generateStringToSign(r, "20250102T030405Z", scope, []string{"host"}, true, signatureAlgorithmV4a)
	require.NoError(t, err)
	publicKey, err := deriveSigV4aPublicKey(sigV4aTestAccessKey, sigV4aTestSecretKey)
	require.NoError(t, err)

	assert.True(t, verifySigV4aSignature(publicKey, *stringToSign, r.URL.Query().Get("X-Amz-Signature")))
}

func TestRegionSetIncludes(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name           string
		regionSet      string
		expectedRegion string
		want           bool
	}{
		{name: "exact", regionSet: "eu-central-1", expectedRegion: "eu-central-1", want: true},
		{name: "list", regionSet: "us-east-1, eu-central-1", expectedRegion: "eu-central-1", want: true},
		{name: "wildcard suffix", regionSet: "eu-*", expectedRegion: "eu-central-1", want: true},
		{name: "all regions", regionSet: "*", expectedRegion: "eu-central-1", want: true},
		{name: "wrong region", regionSet: "us-east-1,us-west-*", expectedRegion: "eu-central-1", want: false},
		{name: "empty", regionSet: "", expectedRegion: "eu-central-1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, regionSetIncludes(tt.regionSet, tt.expectedRegion))
		})
	}
}

func TestCheckAuthenticationAcceptsSigV4aHeader(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := newSignedSigV4aHeaderRequest(t, "eu-central-1,us-west-*")
	usedAccessKeyID, authenticated := checkAuthentication(sigV4aTestCredentials(), "eu-central-1", r)

	assert.True(t, authenticated)
	if assert.NotNil(t, usedAccessKeyID) {
		assert.Equal(t, sigV4aTestAccessKey, *usedAccessKeyID)
	}
}

func TestCheckAuthenticationRejectsSigV4aHeaderOutsideRegionSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := newSignedSigV4aHeaderRequest(t, "us-east-1,us-west-*")
	_, authenticated := checkAuthentication(sigV4aTestCredentials(), "eu-central-1", r)
	assert.False(t, authenticated)
}

func TestCheckAuthenticationAcceptsPresignedSigV4aRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)

	signingTime := time.Now().UTC().Truncate(time.Second)
	timestamp := signingTime.Format("20060102T150405Z")
	scope := createSigV4aScope(signingTime.Format("20060102"), expectedService, expectedRequest)
	r, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt?foo=bar", nil)
	require.NoError(t, err)

	query := r.URL.Query()
	query.Set("X-Amz-Algorithm", signatureAlgorithmV4a)
	query.Set("X-Amz-Credential", sigV4aTestAccessKey+"/"+scope)
	query.Set("X-Amz-Date", timestamp)
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-Region-Set", "eu-central-1,us-west-*")
	query.Set("X-Amz-SignedHeaders", "host")
	r.URL.RawQuery = query.Encode()

	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"host"}, true, signatureAlgorithmV4a)
	require.NoError(t, err)
	query.Set("X-Amz-Signature", signSigV4aString(t, *stringToSign))
	r.URL.RawQuery = query.Encode()

	usedAccessKeyID, authenticated := checkAuthentication(sigV4aTestCredentials(), "eu-central-1", r)
	assert.True(t, authenticated)
	if assert.NotNil(t, usedAccessKeyID) {
		assert.Equal(t, sigV4aTestAccessKey, *usedAccessKeyID)
	}
}

func TestCheckAuthenticationValidatesSigV4aStreamingPayloadAndTrailer(t *testing.T) {
	testutils.SkipIfIntegration(t)

	payload := []byte("hello")
	signingTime := time.Now().UTC().Truncate(time.Second)
	timestamp := signingTime.Format("20060102T150405Z")
	scope := createSigV4aScope(signingTime.Format("20060102"), expectedService, expectedRequest)
	r, err := http.NewRequest(http.MethodPut, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	require.NoError(t, err)
	r.Header.Set("Content-Encoding", contentEncodingAwsChunked)
	r.Header.Set(contentSHA256Header, contentSHA256StreamingECDSAPayloadTrailing)
	r.Header.Set("x-amz-date", timestamp)
	r.Header.Set("x-amz-decoded-content-length", "5")
	r.Header.Set("x-amz-region-set", "eu-*")
	r.Header.Set(trailerHeader, "x-amz-checksum-crc32")

	signedHeaders := []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-region-set", "x-amz-trailer"}
	stringToSign, err := generateStringToSign(r, timestamp, scope, signedHeaders, false, signatureAlgorithmV4a)
	require.NoError(t, err)
	seedSignature := signSigV4aString(t, *stringToSign)
	r.Header.Set("Authorization", signatureAlgorithmV4a+" Credential="+sigV4aTestAccessKey+"/"+scope+",SignedHeaders="+strings.Join(signedHeaders, ";")+",Signature="+seedSignature)

	chunkHasher := sha256.New()
	_, err = chunkHasher.Write(payload)
	require.NoError(t, err)
	chunkStringToSign := generateStringToSignForChunk(signatureAlgorithmV4a, timestamp, scope, seedSignature, chunkHasher)
	chunkSignature := signPaddedSigV4aString(t, chunkStringToSign)

	emptyHasher := sha256.New()
	verifier := signatureVerifier{algorithm: signatureAlgorithmV4a}
	zeroChunkStringToSign := generateStringToSignForChunk(signatureAlgorithmV4a, timestamp, scope, verifier.normalizeStreamingSignature(chunkSignature), emptyHasher)
	zeroChunkSignature := signPaddedSigV4aString(t, zeroChunkStringToSign)

	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, crc32.ChecksumIEEE(payload))
	checksumHeader := "x-amz-checksum-crc32:" + base64.StdEncoding.EncodeToString(checksumBytes)
	trailerStringToSign := generateStringToSignForTrailerChunk(signatureAlgorithmV4a, timestamp, scope, verifier.normalizeStreamingSignature(zeroChunkSignature), checksumHeader)
	trailerSignature := signPaddedSigV4aString(t, trailerStringToSign)

	encodedBody := "5;chunk-signature=" + chunkSignature + "\r\n" + string(payload) + "\r\n" +
		"0;chunk-signature=" + zeroChunkSignature + "\r\n" + checksumHeader + "\r\n" +
		"x-amz-trailer-signature:" + trailerSignature + "\r\n\r\n"
	r.Body = io.NopCloser(strings.NewReader(encodedBody))

	_, authenticated := checkAuthentication(sigV4aTestCredentials(), "eu-central-1", r)
	require.True(t, authenticated)
	decodedPayload, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, decodedPayload)
}

func newSignedSigV4aHeaderRequest(t *testing.T, regionSet string) *http.Request {
	t.Helper()

	signingTime := time.Now().UTC().Truncate(time.Second)
	timestamp := signingTime.Format("20060102T150405Z")
	scope := createSigV4aScope(signingTime.Format("20060102"), expectedService, expectedRequest)
	r, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	require.NoError(t, err)
	r.Body = io.NopCloser(strings.NewReader(""))
	r.Header.Set(contentSHA256Header, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	r.Header.Set("x-amz-date", timestamp)
	r.Header.Set("x-amz-region-set", regionSet)
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-region-set"}
	stringToSign, err := generateStringToSign(r, timestamp, scope, signedHeaders, false, signatureAlgorithmV4a)
	require.NoError(t, err)
	r.Header.Set("Authorization", signatureAlgorithmV4a+" Credential="+sigV4aTestAccessKey+"/"+scope+",SignedHeaders="+strings.Join(signedHeaders, ";")+",Signature="+signSigV4aString(t, *stringToSign))
	return r
}

func signSigV4aString(t *testing.T, stringToSign string) string {
	t.Helper()
	privateKey, err := deriveSigV4aPrivateKey(sigV4aTestAccessKey, sigV4aTestSecretKey)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(stringToSign))
	signature, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	require.NoError(t, err)
	return hex.EncodeToString(signature)
}

func signPaddedSigV4aString(t *testing.T, stringToSign string) string {
	t.Helper()
	signature := signSigV4aString(t, stringToSign)
	require.LessOrEqual(t, len(signature), 144)
	return signature + strings.Repeat("*", 144-len(signature))
}

func sigV4aTestCredentials() []Credentials {
	return []Credentials{{AccessKeyId: sigV4aTestAccessKey, SecretAccessKey: sigV4aTestSecretKey}}
}
