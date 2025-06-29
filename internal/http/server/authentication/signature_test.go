package authentication

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
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

	stringToSign := generateStringToSign(r, date+"T000000Z", scope, []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}, isPresigned)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972", stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	signature := createSignature(signingKey, stringToSign)
	assert.Equal(t, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", signature)
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

	stringToSign := generateStringToSign(r, date+"T000000Z", scope, []string{"host"}, isPresigned)
	assert.Equal(t, "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n3bfa292879f6447bbcda7001decf97f4a54dc650c8942174ae0a9121cf58ad04", stringToSign)

	signingKey := createSigningKey(secretAccessKey, date, region, service, request)

	signature := createSignature(signingKey, stringToSign)
	assert.Equal(t, "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", signature)
}
