package tracing

import (
	"context"
	"database/sql"
	"io"
	"runtime/trace"

	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type tracingBlobStoreMiddleware struct {
	*lifecycle.ValidatedLifecycle
	regionName     string
	innerBlobStore blobstore.BlobStore
	tracer         oteltrace.Tracer
}

// Compile-time check to ensure tracingBlobStoreMiddleware implements blobstore.BlobStore
var _ blobstore.BlobStore = (*tracingBlobStoreMiddleware)(nil)

func New(regionName string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	lifecycle, err := lifecycle.NewValidatedLifecycle("TracingBlobStoreMiddleware")
	if err != nil {
		return nil, err
	}
	tbsm := &tracingBlobStoreMiddleware{
		ValidatedLifecycle: lifecycle,
		regionName:         regionName,
		innerBlobStore:     innerBlobStore,
		tracer:             otel.Tracer("github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/tracing"),
	}
	return tbsm, nil
}

func (tbsm *tracingBlobStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Start()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".Start")
	defer span.End()
	if err := tbsm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	return tbsm.innerBlobStore.Start(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Stop()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".Stop")
	defer span.End()
	if err := tbsm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	return tbsm.innerBlobStore.Stop(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".PutBlob()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".PutBlob")
	defer span.End()

	return tbsm.innerBlobStore.PutBlob(ctx, tx, blobId, reader)
}

func (tbsm *tracingBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlob()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".GetBlob")
	defer span.End()

	return tbsm.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (tbsm *tracingBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlobIds()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".GetBlobIds")
	defer span.End()

	return tbsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (tbsm *tracingBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".DeleteBlob()").End()
	ctx, span := tbsm.tracer.Start(ctx, tbsm.regionName+".DeleteBlob")
	defer span.End()

	return tbsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
