package authentication

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckAuthenticationReportsS3ErrorCodes(t *testing.T) {
	testutils.SkipIfIntegration(t)

	provider := staticCredentialProvider{
		"key": {AccessKeyID: "key", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "client"},
	}
	now := time.Now().UTC()
	currentDate := now.Format("20060102")
	currentTimestamp := now.Format("20060102T150405Z")
	expiredTime := now.Add(-2 * time.Hour)

	testCases := []struct {
		name       string
		request    func() *http.Request
		statusCode int
		code       string
		message    string
	}{
		{
			name: "skewed header timestamp",
			request: func() *http.Request {
				return signedLookingRequest()
			},
			statusCode: http.StatusForbidden,
			code:       "RequestTimeTooSkewed",
		},
		{
			name: "wrong region",
			request: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/"+currentDate+"/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
				r.Header.Set("X-Amz-Date", currentTimestamp)
				r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				return r
			},
			statusCode: http.StatusBadRequest,
			code:       "AuthorizationHeaderMalformed",
		},
		{
			name: "unsigned x-amz header",
			request: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/"+currentDate+"/eu-central-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
				r.Header.Set("X-Amz-Date", currentTimestamp)
				r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				r.Header.Set("X-Amz-Meta-Unsigned", "value")
				return r
			},
			statusCode: http.StatusForbidden,
			code:       "AccessDenied",
			message:    "There were headers present in the request which were not signed",
		},
		{
			name: "invalid signature",
			request: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/"+currentDate+"/eu-central-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
				r.Header.Set("X-Amz-Date", currentTimestamp)
				r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
				return r
			},
			statusCode: http.StatusForbidden,
			code:       "SignatureDoesNotMatch",
		},
		{
			name: "expired presigned url",
			request: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F"+expiredTime.Format("20060102")+"%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Date="+expiredTime.Format("20060102T150405Z")+"&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=invalid", nil)
			},
			statusCode: http.StatusForbidden,
			code:       "AccessDenied",
			message:    "Request has expired",
		},
		{
			name: "presigned url with invalid expiry",
			request: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F"+currentDate+"%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Date="+currentTimestamp+"&X-Amz-Expires=0&X-Amz-SignedHeaders=host&X-Amz-Signature=invalid", nil)
			},
			statusCode: http.StatusBadRequest,
			code:       "AuthorizationQueryParametersError",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			MakeSignatureMiddleware(provider, "eu-central-1", http.NotFoundHandler()).ServeHTTP(recorder, testCase.request())
			require.Equal(t, testCase.statusCode, recorder.Code)
			assert.Equal(t, "application/xml", recorder.Header().Get("Content-Type"))
			assert.Contains(t, recorder.Body.String(), "<Code>"+testCase.code+"</Code>")
			if testCase.message != "" {
				assert.Contains(t, recorder.Body.String(), "<Message>"+testCase.message+"</Message>")
			}
		})
	}
}
