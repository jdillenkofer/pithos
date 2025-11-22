package ioutils

import (
	"context"
	"io"

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
	return t.inner.Read(p)
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
	return t.inner.Read(p)
}

func (t *tracingReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if err := t.ctx.Err(); err != nil {
		return 0, err
	}
	_, span := t.tracer.Start(t.ctx, t.spanName+".Seek")
	defer span.End()
	return t.inner.Seek(offset, whence)
}

func (t *tracingReadSeekCloser) Close() error {
	return t.inner.Close()
}
