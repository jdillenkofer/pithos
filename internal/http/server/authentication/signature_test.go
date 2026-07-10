package authentication

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
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

	stringToSign, err := generateStringToSign(r, date+"T000000Z", scope, []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}, isPresigned)
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
	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"content-encoding", "content-length", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class"}, isPresigned)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\ncee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	expectedSignature := "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
	hasTrailingHeader := false
	hasTrailingHeaderWithSignature := false
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, signingKey, hasTrailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation, "")

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
	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class", "x-amz-trailer"}, isPresigned)
	assert.NoError(t, err)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559", *stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	expectedSignature := "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
	hasTrailingHeader := true
	hasTrailingHeaderWithSignature := true
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, signingKey, hasTrailingHeader, hasTrailingHeaderWithSignature, skipChunkValidation, "x-amz-checksum-crc32c")

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

	body := newAwsChunkReadCloser(context.Background(), io.NopCloser(bytes.NewReader(content)), "20130524T000000Z", scope, seedSignature, signingKey, true, true, false, "x-amz-checksum-crc32c")

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
	return newAwsChunkReadCloser(context.Background(), io.NopCloser(strings.NewReader(content)), "", "", "", nil, true, false, true, trailerName)
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

	stringToSign, err := generateStringToSign(r, date+"T000000Z", scope, []string{"host"}, isPresigned)
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
	query.Set("X-Amz-Algorithm", signatureAlgorithm)
	query.Set("X-Amz-Credential", accessKeyID+"/"+scope)
	query.Set("X-Amz-Date", timestamp)
	query.Set("X-Amz-Expires", "604800")
	query.Set("X-Amz-SignedHeaders", "host")
	r.URL.RawQuery = query.Encode()

	stringToSign, err := generateStringToSign(r, timestamp, scope, []string{"host"}, true)
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
