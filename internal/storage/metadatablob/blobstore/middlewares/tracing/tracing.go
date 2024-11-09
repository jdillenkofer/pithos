package tracing

import (
	"context"
	"database/sql"
	"io"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type tracingBlobStoreMiddleware struct {
	regionName     string
	innerBlobStore blobstore.BlobStore
}

func New(regionName string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	tbsm := &tracingBlobStoreMiddleware{
		regionName:     regionName,
		innerBlobStore: innerBlobStore,
	}
	return tbsm, nil
}

func (tbsm *tracingBlobStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Start()").End()

	return tbsm.innerBlobStore.Start(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Stop()").End()

	return tbsm.innerBlobStore.Stop(ctx)
}

func (tbsm *tracingBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*blobstore.PutBlobResult, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".PutBlob()").End()

	return tbsm.innerBlobStore.PutBlob(ctx, tx, blobId, reader)
}

func (tbsm *tracingBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadSeekCloser, error) {
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
