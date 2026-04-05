package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/oklog/ulid/v2"
)

const requestIDHeader = "x-amz-request-id"

type requestIDContextKey struct{}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func MakeRequestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()
		requestID, ok := ctx.Value(authentication.RequestIDContextKey{}).(string)
		if !ok || requestID == "" {
			requestID = ulid.Make().String()
			ctx = context.WithValue(ctx, authentication.RequestIDContextKey{}, requestID)
		}
		ctx = context.WithValue(ctx, requestIDContextKey{}, requestID)

		w.Header().Set(requestIDHeader, requestID)
		wrappedWriter := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrappedWriter, r.WithContext(ctx))

		slog.InfoContext(
			ctx,
			"Request completed",
			"method", r.Method,
			"host", r.Host,
			"path", r.URL.Path,
			"status", wrappedWriter.statusCode,
			"durationMs", time.Since(start).Milliseconds(),
		)
	})
}

func RequestIDFromContext(ctx context.Context) (string, bool) {
	requestID, ok := ctx.Value(requestIDContextKey{}).(string)
	if !ok || requestID == "" {
		return "", false
	}
	return requestID, true
}
