package authentication

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateSignature(t *testing.T) {
	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	stringToSign := "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9"
	signingKey := createSigningKey(secretAccessKey, "20151229", "us-east-1", "s3", "aws4_request")
	signature := createSignature(signingKey, stringToSign)
	assert.Equal(t, "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e", signature)
}

func TestCreateSignatureFromRequest(t *testing.T) {
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
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, signingKey, hasTrailingHeader, skipChunkValidation)

	seedSignature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, expectedSignature, seedSignature)

	data, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte(strings.Repeat("a", 65536+1024)), data)

	err = r.Body.Close()
	assert.NoError(t, err)
}

func TestCreateSeedSignatureFromAwsChunkRequestWithTrailingHeader(t *testing.T) {
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
	skipChunkValidation := false
	r.Body = newAwsChunkReadCloser(io.NopCloser(bytes.NewReader(content)), timestamp, scope, expectedSignature, signingKey, hasTrailingHeader, skipChunkValidation)

	seedSignature := createSignature(signingKey, *stringToSign)
	assert.Equal(t, expectedSignature, seedSignature)

	data, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte(strings.Repeat("a", 65536+1024)), data)

	err = r.Body.Close()
	assert.NoError(t, err)
}

func TestCreateSignatureFromPresignedRequest(t *testing.T) {
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
