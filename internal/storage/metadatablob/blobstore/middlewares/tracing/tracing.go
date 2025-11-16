package tracing

import (
	"context"
	"database/sql"
	"io"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type tracingBlobStoreMiddleware struct {
	*lifecycle.ValidatedLifecycle
	regionName     string
	innerBlobStore blobstore.BlobStore
}

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
	}
	return tbsm, nil
}

func (tbsm *tracingBlobStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Start()").End()
	if err := tbsm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	return tbsm.innerBlobStore.Start(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Stop()").End()
	if err := tbsm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	return tbsm.innerBlobStore.Stop(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".PutBlob()").End()

	return tbsm.innerBlobStore.PutBlob(ctx, tx, blobId, reader)
}

func (tbsm *tracingBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlob()").End()

	return tbsm.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (tbsm *tracingBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlobIds()").End()

	return tbsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (tbsm *tracingBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".DeleteBlob()").End()

	return tbsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
