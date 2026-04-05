package logging

import (
	"context"
	"log/slog"

	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"go.opentelemetry.io/otel/trace"
)

type ContextEnrichingHandler struct {
	next slog.Handler
}

func NewContextEnrichingHandler(next slog.Handler) *ContextEnrichingHandler {
	return &ContextEnrichingHandler{next: next}
}

func (h *ContextEnrichingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *ContextEnrichingHandler) Handle(ctx context.Context, record slog.Record) error {
	record = record.Clone()
	if requestID, ok := httpmiddleware.RequestIDFromContext(ctx); ok {
		record.AddAttrs(slog.String("requestId", requestID))
	}

	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		record.AddAttrs(
			slog.String("traceId", spanContext.TraceID().String()),
			slog.String("spanId", spanContext.SpanID().String()),
		)
	}

	return h.next.Handle(ctx, record)
}

func (h *ContextEnrichingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextEnrichingHandler{next: h.next.WithAttrs(attrs)}
}

func (h *ContextEnrichingHandler) WithGroup(name string) slog.Handler {
	return &ContextEnrichingHandler{next: h.next.WithGroup(name)}
}
