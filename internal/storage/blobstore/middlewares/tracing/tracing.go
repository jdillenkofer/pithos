package tracing

import (
	"context"
	"database/sql"
	"io"
	"runtime/trace"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
)

type TracingBlobStoreMiddleware struct {
	regionName     string
	innerBlobStore blobstore.BlobStore
}

func New(regionName string, innerBlobStore blobstore.BlobStore) (*TracingBlobStoreMiddleware, error) {
	tbsm := &TracingBlobStoreMiddleware{
		regionName:     regionName,
		innerBlobStore: innerBlobStore,
	}
	return tbsm, nil
}

func (tbsm *TracingBlobStoreMiddleware) Start(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Start()").End()

	return tbsm.innerBlobStore.Start(ctx)
}

func (tbsm *TracingBlobStoreMiddleware) Stop(ctx context.Context) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".Stop()").End()

	return tbsm.innerBlobStore.Stop(ctx)
}

func (tbsm *TracingBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*blobstore.PutBlobResult, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".PutBlob()").End()

	return tbsm.innerBlobStore.PutBlob(ctx, tx, blobId, reader)
}

func (tbsm *TracingBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadSeekCloser, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlob()").End()

	return tbsm.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (tbsm *TracingBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlobIds()").End()

	return tbsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (tbsm *TracingBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".DeleteBlob()").End()

	return tbsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
