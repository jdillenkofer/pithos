package blob

import (
	"context"
	"database/sql"
	"io"
	"runtime/trace"
)

type TracingBlobStoreMiddleware struct {
	regionName     string
	innerBlobStore BlobStore
}

func NewTracingBlobStoreMiddleware(regionName string, innerBlobStore BlobStore) (*TracingBlobStoreMiddleware, error) {
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

func (tbsm *TracingBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".PutBlob()").End()

	return tbsm.innerBlobStore.PutBlob(ctx, tx, blobId, blob)
}

func (tbsm *TracingBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	defer trace.StartRegion(ctx, tbsm.regionName+".GetBlob()").End()

	return tbsm.innerBlobStore.GetBlob(ctx, tx, blobId)
}

func (tbsm *TracingBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	defer trace.StartRegion(ctx, tbsm.regionName+".DeleteBlob()").End()

	return tbsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
