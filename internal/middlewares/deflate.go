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
		w.Header().Set("Content-Encoding", "deflate")
		flate, err := flate.NewWriter(w, -1)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		defer flate.Close()
		h.ServeHTTP(deflateResponseWriter{Writer: flate, ResponseWriter: w}, r)
	})
}
