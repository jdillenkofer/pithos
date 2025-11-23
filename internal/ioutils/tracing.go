package ioutils

import (
	"context"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type tracingReadCloser struct {
	ctx      context.Context
	tracer   trace.Tracer
	spanName string
	inner    io.ReadCloser
}

func NewTracingReadCloser(ctx context.Context, tracer trace.Tracer, spanName string, inner io.ReadCloser) io.ReadCloser {
	return &tracingReadCloser{
		ctx:      ctx,
		tracer:   tracer,
		spanName: spanName,
		inner:    inner,
	}
}

func (t *tracingReadCloser) Read(p []byte) (int, error) {
	if err := t.ctx.Err(); err != nil {
		return 0, err
	}
	_, span := t.tracer.Start(t.ctx, t.spanName+".Read")
	defer span.End()
	n, err := t.inner.Read(p)
	span.SetAttributes(attribute.Int("bytes.read", n))

	if err != nil {
		span.SetStatus(codes.Error, "Read failed")
		span.RecordError(err)
	}
	return n, err
}

func (t *tracingReadCloser) Close() error {
	return t.inner.Close()
}

type tracingReadSeekCloser struct {
	ctx      context.Context
	tracer   trace.Tracer
	spanName string
	inner    io.ReadSeekCloser
}

func NewTracingReadSeekCloser(ctx context.Context, tracer trace.Tracer, spanName string, inner io.ReadSeekCloser) io.ReadSeekCloser {
	return &tracingReadSeekCloser{
		ctx:      ctx,
		tracer:   tracer,
		spanName: spanName,
		inner:    inner,
	}
}

func (t *tracingReadSeekCloser) Read(p []byte) (int, error) {
	if err := t.ctx.Err(); err != nil {
		return 0, err
	}
	_, span := t.tracer.Start(t.ctx, t.spanName+".Read")
	defer span.End()
	n, err := t.inner.Read(p)
	span.SetAttributes(attribute.Int("bytes.read", n))

	if err != nil {
		span.SetStatus(codes.Error, "Read failed")
		span.RecordError(err)
	}
	return n, err
}

func (t *tracingReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if err := t.ctx.Err(); err != nil {
		return 0, err
	}
	_, span := t.tracer.Start(t.ctx, t.spanName+".Seek")
	defer span.End()
	n, err := t.inner.Seek(offset, whence)

	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int64("offset", offset),
			attribute.String("whence", convertWhenceToStr(whence)),
			attribute.Int64("result", n))
	}

	if err != nil {
		span.SetStatus(codes.Error, "Seek failed")
		span.RecordError(err)
	}
	return n, err
}

func convertWhenceToStr(whence int) string {
	var whenceStr string
	switch whence {
	case io.SeekStart:
		whenceStr = "io.SeekStart"
	case io.SeekCurrent:
		whenceStr = "io.SeekCurrent"
	case io.SeekEnd:
		whenceStr = "io.SeekEnd"
	default:
		whenceStr = "unknown"
	}
	return whenceStr
}

func (t *tracingReadSeekCloser) Close() error {
	return t.inner.Close()
}
