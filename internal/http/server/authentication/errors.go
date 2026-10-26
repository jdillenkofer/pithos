package authentication

import (
	"net/http"
	"time"
)

// RequestError is an authentication failure reported to the client as an S3
// error response.
type RequestError struct {
	StatusCode int
	Code       string
	Message    string
	// ClockSkew carries the details S3 adds to RequestTimeTooSkewed.
	ClockSkew *ClockSkewDetails
}

// ClockSkewDetails describes a request rejected as RequestTimeTooSkewed.
type ClockSkewDetails struct {
	RequestTime    string
	ServerTime     time.Time
	MaxAllowedSkew time.Duration
}

func (e *RequestError) Error() string {
	return e.Code
}

var errSignatureDoesNotMatch = &RequestError{
	StatusCode: http.StatusForbidden,
	Code:       "SignatureDoesNotMatch",
	Message:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
}

var errInvalidAccessKeyID = &RequestError{
	StatusCode: http.StatusForbidden,
	Code:       "InvalidAccessKeyId",
	Message:    "The AWS Access Key Id you provided does not exist in our records.",
}

// maxClockSkew is how far x-amz-date of a header-signed request may be from
// the server time in either direction.
const maxClockSkew = 15 * time.Minute

// newRequestTimeTooSkewedError reports a header-signed request outside the
// allowed clock skew, with the request and server time S3 includes.
func newRequestTimeTooSkewedError(requestTime string, serverTime time.Time) *RequestError {
	return &RequestError{
		StatusCode: http.StatusForbidden,
		Code:       "RequestTimeTooSkewed",
		Message:    "The difference between the request time and the current time is too large.",
		ClockSkew:  &ClockSkewDetails{RequestTime: requestTime, ServerTime: serverTime, MaxAllowedSkew: maxClockSkew},
	}
}

var errMissingContentSHA256 = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidRequest",
	Message:    "Missing required header for this request: x-amz-content-sha256",
}

var errInvalidContentSHA256 = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidArgument",
	Message:    "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, STREAMING-UNSIGNED-PAYLOAD-TRAILER, STREAMING-AWS4-HMAC-SHA256-PAYLOAD, STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER, STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD, STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER or a valid sha256 value.",
}

var errContentSHA256Mismatch = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "XAmzContentSHA256Mismatch",
	Message:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
}

var errMissingDecodedContentLength = &RequestError{
	StatusCode: http.StatusLengthRequired,
	Code:       "MissingContentLength",
	Message:    "You must provide the Content-Length HTTP header.",
}

var errInvalidDecodedContentLength = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidArgument",
	Message:    "x-amz-decoded-content-length must be a non-negative integer.",
}

var errUnsignedStreamingPayloadRequired = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidRequest",
	Message:    "An aws-chunked payload of an unsigned request must use STREAMING-UNSIGNED-PAYLOAD or STREAMING-UNSIGNED-PAYLOAD-TRAILER as x-amz-content-sha256.",
}

var errInternalError = &RequestError{
	StatusCode: http.StatusInternalServerError,
	Code:       "InternalError",
	Message:    "We encountered an internal error. Please try again.",
}

func newAccessDeniedError(message string) *RequestError {
	return &RequestError{StatusCode: http.StatusForbidden, Code: "AccessDenied", Message: message}
}

var errUnsupportedAuthorizationType = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidArgument",
	Message:    "Unsupported Authorization Type",
}

var errUnsupportedSignatureV2 = &RequestError{
	StatusCode: http.StatusBadRequest,
	Code:       "InvalidRequest",
	Message:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
}

var errMissingPresignedQueryParameters = newMalformedAuthorizationError(true, "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.")

var errMalformedAuthorizationComponents = newMalformedAuthorizationError(false, "the authorization header requires three components: Credential, SignedHeaders, and Signature.")

// newMalformedCredentialError reports an unusable credential scope, with the
// prefix S3 uses for the Authorization header or the X-Amz-Credential query
// parameter.
func newMalformedCredentialError(isPresigned bool, detail string) *RequestError {
	if isPresigned {
		return newMalformedAuthorizationError(true, "Error parsing the X-Amz-Credential parameter; "+detail)
	}
	return newMalformedAuthorizationError(false, detail)
}

// newMalformedAuthorizationError reports an unusable Authorization header or
// presigned query, using the code S3 returns for the respective form.
func newMalformedAuthorizationError(isPresigned bool, message string) *RequestError {
	if isPresigned {
		return &RequestError{StatusCode: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: message}
	}
	return &RequestError{StatusCode: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "The authorization header is malformed; " + message}
}

// ErrorWriter renders an authentication failure for the client.
type ErrorWriter func(w http.ResponseWriter, r *http.Request, requestErr *RequestError)
