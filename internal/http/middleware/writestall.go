package middleware

import (
	"net/http"
	"time"
)

// writeStallSliceSize is the most data one write deadline covers. A client is
// dropped only if it accepts less than this within the timeout.
const writeStallSliceSize = 32 << 10

// MakeWriteStallTimeoutMiddleware aborts a response once the client accepted
// less than writeStallSliceSize bytes within timeout. Unlike
// http.Server.WriteTimeout it does not cut off a slow download that keeps
// making progress; it drops clients that stopped reading, which would
// otherwise hold their reader (and its reader lease) forever. It must wrap
// the handler the server calls directly: response writer wrappers inside it
// hide the connection.
func MakeWriteStallTimeoutMiddleware(timeout time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controller := http.NewResponseController(w)
		// Clear the deadline the previous response on this connection left.
		_ = controller.SetWriteDeadline(time.Time{})
		// The server flushes buffered output after the handler returns and
		// clears the deadline once it did.
		defer func() { _ = controller.SetWriteDeadline(time.Now().Add(timeout)) }()
		next.ServeHTTP(&writeStallResponseWriter{ResponseWriter: w, controller: controller, timeout: timeout}, r)
	})
}

type writeStallResponseWriter struct {
	http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

// Write re-arms the deadline for every slice of p, so a large write by a slow
// client that keeps reading does not exceed it. The writer deliberately has
// no ReadFrom: the underlying one reads the source under the write deadline,
// so a slow backend would count as a stalled client. io.Copy therefore reads
// each chunk before Write arms the deadline.
func (w *writeStallResponseWriter) Write(p []byte) (int, error) {
	written := 0
	for {
		slice := p[written:min(written+writeStallSliceSize, len(p))]
		_ = w.controller.SetWriteDeadline(time.Now().Add(w.timeout))
		n, err := w.ResponseWriter.Write(slice)
		written += n
		if err != nil || written == len(p) {
			return written, err
		}
	}
}

// FlushError re-arms the deadline like Write, since the handler may flush
// after waiting on its source.
func (w *writeStallResponseWriter) FlushError() error {
	_ = w.controller.SetWriteDeadline(time.Now().Add(w.timeout))
	return w.controller.Flush()
}

func (w *writeStallResponseWriter) Flush() {
	_ = w.FlushError()
}

func (w *writeStallResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
