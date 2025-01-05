package middlewares

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
	wroteHeader bool
}

func (w gzipResponseWriter) WriteHeader(code int) {
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

	w.Header().Add("Vary", "Accept-Encoding")

	// The content-length after compression is unknown
	w.Header().Del("Content-Length")
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func MakeGzipMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") || w.Header().Get("Content-Encoding") != "" {
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		h.ServeHTTP(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	})
}
