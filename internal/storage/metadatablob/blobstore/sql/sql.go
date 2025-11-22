package sql

import (
	"context"
	"database/sql"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	blobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type sqlBlobStore struct {
	*lifecycle.ValidatedLifecycle
	blobContentRepository blobContent.Repository
	tracer                trace.Tracer
}

// Compile-time check to ensure sqlBlobStore implements blobstore.BlobStore
var _ blobstore.BlobStore = (*sqlBlobStore)(nil)

func New(db database.Database, blobContentRepository blobContent.Repository) (blobstore.BlobStore, error) {
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("sqlBlobStore")
	if err != nil {
		return nil, err
	}
	return &sqlBlobStore{
		ValidatedLifecycle:    validatedLifecycle,
		blobContentRepository: blobContentRepository,
		tracer:                otel.Tracer("internal/storage/metadatablob/blobstore/sql"),
	}, nil
}

func (bs *sqlBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	ctx, span := bs.tracer.Start(ctx, "sqlBlobStore.PutBlob")
	defer span.End()

	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	blobContentEntity := blobContent.Entity{
		Id:      ptrutils.ToPtr(blobId),
		Content: content,
	}
	err = bs.blobContentRepository.PutBlobContent(ctx, tx, &blobContentEntity)
	if err != nil {
		return err
	}

	return nil
}

func (bs *sqlBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlBlobStore.GetBlob")
	defer span.End()

	blobContentEntity, err := bs.blobContentRepository.FindBlobContentById(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}
	if blobContentEntity == nil {
		return nil, blobstore.ErrBlobNotFound
	}
	reader := ioutils.NewByteReadSeekCloser(blobContentEntity.Content)

	return reader, nil
}

func (bs *sqlBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlBlobStore.GetBlobIds")
	defer span.End()

	return bs.blobContentRepository.FindBlobContentIds(ctx, tx)
}

func (bs *sqlBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	ctx, span := bs.tracer.Start(ctx, "sqlBlobStore.DeleteBlob")
	defer span.End()

	err := bs.blobContentRepository.DeleteBlobContentById(ctx, tx, blobId)
	return err
}
