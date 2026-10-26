package server

import (
	"net/http"
	"strconv"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

const decodedContentLengthHeader = "x-amz-decoded-content-length"

// declaredPayloadLength returns the payload size a request announces: the
// decoded length of an aws-chunked body, otherwise its Content-Length.
func declaredPayloadLength(r *http.Request) (int64, bool) {
	if authentication.HasAwsChunkedContentEncoding(r.Header.Get("Content-Encoding")) {
		decodedContentLength, err := strconv.ParseInt(r.Header.Get(decodedContentLengthHeader), 10, 64)
		if err != nil {
			return 0, false
		}
		return decodedContentLength, true
	}
	if r.ContentLength < 0 {
		return 0, false
	}
	return r.ContentLength, true
}

// Reject oversized payloads before authentication, so a request is refused
// on its declared size without the credential lookup or any body read.
// Handlers still bound the bytes they actually read.
func makeRequestSizeLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if payloadLength, ok := declaredPayloadLength(r); ok && payloadLength > storage.MaxEntitySize {
			handleError(storage.ErrEntityTooLarge, w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
