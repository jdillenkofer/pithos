package authentication

import (
	"encoding/xml"
	"log/slog"
	"net/http"
)

// RequestError is an authentication failure reported to the client as an S3
// error response.
type RequestError struct {
	StatusCode int
	Code       string
	Message    string
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

var errRequestTimeTooSkewed = &RequestError{
	StatusCode: http.StatusForbidden,
	Code:       "RequestTimeTooSkewed",
	Message:    "The difference between the request time and the current time is too large.",
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

var errInternalError = &RequestError{
	StatusCode: http.StatusInternalServerError,
	Code:       "InternalError",
	Message:    "We encountered an internal error. Please try again.",
}

func newAccessDeniedError(message string) *RequestError {
	return &RequestError{StatusCode: http.StatusForbidden, Code: "AccessDenied", Message: message}
}

// newMalformedAuthorizationError reports an unusable Authorization header or
// presigned query, using the code S3 returns for the respective form.
func newMalformedAuthorizationError(isPresigned bool, message string) *RequestError {
	if isPresigned {
		return &RequestError{StatusCode: http.StatusBadRequest, Code: "AuthorizationQueryParametersError", Message: message}
	}
	return &RequestError{StatusCode: http.StatusBadRequest, Code: "AuthorizationHeaderMalformed", Message: "The authorization header is malformed; " + message}
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestId string   `xml:"RequestId"`
}

func writeRequestError(w http.ResponseWriter, r *http.Request, requestErr *RequestError) {
	requestID, _ := r.Context().Value(RequestIDContextKey{}).(string)
	body, err := xml.MarshalIndent(errorResponse{
		Code:      requestErr.Code,
		Message:   requestErr.Message,
		Resource:  r.URL.Path,
		RequestId: requestID,
	}, "", "  ")
	if err != nil {
		slog.ErrorContext(r.Context(), "Failed to marshal XML response", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(requestErr.StatusCode)
	if _, err := w.Write(append([]byte(xml.Header), body...)); err != nil {
		slog.DebugContext(r.Context(), "Failed to write XML response", "error", err)
	}
}
