package authentication

import (
	"encoding/xml"
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
			MakeSignatureMiddleware(provider, "eu-central-1", WriteRequestError, http.NotFoundHandler()).ServeHTTP(recorder, testCase.request())
			require.Equal(t, testCase.statusCode, recorder.Code)
			assert.Equal(t, "application/xml", recorder.Header().Get("Content-Type"))
			assert.Contains(t, recorder.Body.String(), "<Code>"+testCase.code+"</Code>")
			if testCase.message != "" {
				assert.Contains(t, recorder.Body.String(), "<Message>"+testCase.message+"</Message>")
			}
		})
	}
}

// S3 accepts a header-signed request within 15 minutes of x-amz-date in
// either direction and answers RequestTimeTooSkewed outside of that window.
func TestCheckAuthenticationRejectsHeaderTimestampOutsideSkewWindow(t *testing.T) {
	testutils.SkipIfIntegration(t)

	provider := staticCredentialProvider{
		"key": {AccessKeyID: "key", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "client"},
	}
	testCases := []struct {
		offset time.Duration
		code   string
	}{
		{offset: -16 * time.Minute, code: "RequestTimeTooSkewed"},
		{offset: 16 * time.Minute, code: "RequestTimeTooSkewed"},
		{offset: -14 * time.Minute, code: "SignatureDoesNotMatch"},
		{offset: 14 * time.Minute, code: "SignatureDoesNotMatch"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.offset.String(), func(t *testing.T) {
			signingTime := time.Now().UTC().Add(testCase.offset)
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/"+signingTime.Format("20060102")+"/eu-central-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid")
			r.Header.Set("X-Amz-Date", signingTime.Format("20060102T150405Z"))
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

			recorder := httptest.NewRecorder()
			MakeSignatureMiddleware(provider, "eu-central-1", WriteRequestError, http.NotFoundHandler()).ServeHTTP(recorder, r)
			assert.Equal(t, http.StatusForbidden, recorder.Code)
			assert.Contains(t, recorder.Body.String(), "<Code>"+testCase.code+"</Code>")
			if testCase.code == "RequestTimeTooSkewed" {
				assert.Contains(t, recorder.Body.String(), "<RequestTime>"+r.Header.Get("X-Amz-Date")+"</RequestTime>")
				assert.Contains(t, recorder.Body.String(), "<ServerTime>")
				assert.Contains(t, recorder.Body.String(), "<MaxAllowedSkewMilliseconds>900000</MaxAllowedSkewMilliseconds>")
			}
		})
	}
}

// Malformed authentication is reported with the fixed messages S3 uses, never
// with the underlying parse error.
func TestCheckAuthenticationReportsS3MessagesForMalformedAuthentication(t *testing.T) {
	testutils.SkipIfIntegration(t)

	provider := staticCredentialProvider{
		"key": {AccessKeyID: "key", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "client"},
	}
	now := time.Now().UTC()
	currentDate := now.Format("20060102")
	currentTimestamp := now.Format("20060102T150405Z")
	presignedQuery := func(expires string) string {
		return "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F" + currentDate + "%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Date=" + currentTimestamp + "&X-Amz-Expires=" + expires + "&X-Amz-SignedHeaders=host&X-Amz-Signature=invalid"
	}
	headerRequest := func(authorization string) func() *http.Request {
		return func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Authorization", authorization)
			r.Header.Set("X-Amz-Date", currentTimestamp)
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			return r
		}
	}

	testCases := []struct {
		name    string
		request func() *http.Request
		code    string
		message string
	}{
		{
			name:    "non-numeric X-Amz-Expires",
			request: func() *http.Request { return httptest.NewRequest(http.MethodGet, presignedQuery("abc"), nil) },
			code:    "AuthorizationQueryParametersError",
			message: "X-Amz-Expires should be a number",
		},
		{
			name:    "X-Amz-Expires above a week",
			request: func() *http.Request { return httptest.NewRequest(http.MethodGet, presignedQuery("604801"), nil) },
			code:    "AuthorizationQueryParametersError",
			message: "X-Amz-Expires must be less than a week (in seconds) that is; 604800",
		},
		{
			name: "missing presigned query parameter",
			request: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F"+currentDate+"%2Feu-central-1%2Fs3%2Faws4_request", nil)
			},
			code:    "AuthorizationQueryParametersError",
			message: "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		},
		{
			name: "presigned credential with wrong region",
			request: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2F"+currentDate+"%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date="+currentTimestamp+"&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=invalid", nil)
			},
			code:    "AuthorizationQueryParametersError",
			message: "Error parsing the X-Amz-Credential parameter; the region 'us-east-1' is wrong; expecting 'eu-central-1'",
		},
		{
			name:    "signature version 2",
			request: headerRequest("AWS key:signature"),
			code:    "InvalidRequest",
			message: "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		},
		{
			name:    "unknown authorization scheme",
			request: headerRequest("Basic a2V5OnNlY3JldA=="),
			code:    "InvalidArgument",
			message: "Unsupported Authorization Type",
		},
		{
			name:    "missing authorization component",
			request: headerRequest("AWS4-HMAC-SHA256 Credential=key/" + currentDate + "/eu-central-1/s3/aws4_request,Signature=invalid"),
			code:    "AuthorizationHeaderMalformed",
			message: "The authorization header is malformed; the authorization header requires three components: Credential, SignedHeaders, and Signature.",
		},
		{
			name:    "malformed credential",
			request: headerRequest("AWS4-HMAC-SHA256 Credential=key/" + currentDate + "/eu-central-1/s3,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid"),
			code:    "AuthorizationHeaderMalformed",
			message: `The authorization header is malformed; the Credential is mal-formed; expecting "<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request".`,
		},
		{
			name:    "wrong service",
			request: headerRequest("AWS4-HMAC-SHA256 Credential=key/" + currentDate + "/eu-central-1/iam/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid"),
			code:    "AuthorizationHeaderMalformed",
			message: `The authorization header is malformed; incorrect service "iam". This endpoint belongs to "s3".`,
		},
		{
			name:    "credential date not matching x-amz-date",
			request: headerRequest("AWS4-HMAC-SHA256 Credential=key/20200101/eu-central-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=invalid"),
			code:    "AuthorizationHeaderMalformed",
			message: `The authorization header is malformed; Invalid credential date "20200101". This date is not the same as X-Amz-Date: "` + currentDate + `".`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			MakeSignatureMiddleware(provider, "eu-central-1", WriteRequestError, http.NotFoundHandler()).ServeHTTP(recorder, testCase.request())
			var response errorResponse
			require.NoError(t, xml.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, testCase.code, response.Code)
			assert.Equal(t, testCase.message, response.Message)
		})
	}
}
