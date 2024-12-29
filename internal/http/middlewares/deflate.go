package middlewares

import (
	"compress/flate"
	"io"
	"net/http"
	"strings"
)

type deflateResponseWriter struct {
	io.Writer
	http.ResponseWriter
	wroteHeader bool
}

func (w deflateResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		w.ResponseWriter.WriteHeader(code) // Allow multiple calls to propagate.
		return
	}
	w.wroteHeader = true
	defer w.ResponseWriter.WriteHeader(code)

	// Already compressed data?
	if w.Header().Get("Content-Encoding") != "" {
		return
	}

	w.Header().Set("Content-Encoding", "deflate")
	w.Header().Add("Vary", "Accept-Encoding")

	// The content-length after compression is unknown
	w.Header().Del("Content-Length")
}

func (w deflateResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func MakeDeflateMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "deflate") || w.Header().Get("Content-Encoding") != "" {
			h.ServeHTTP(w, r)
			return
		}
		flate, err := flate.NewWriter(w, -1)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		defer flate.Close()
		h.ServeHTTP(deflateResponseWriter{Writer: flate, ResponseWriter: w}, r)
	})
}
