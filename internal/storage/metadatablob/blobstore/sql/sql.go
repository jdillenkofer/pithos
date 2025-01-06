package sql

import (
	"context"
	"database/sql"
	"io"

	"github.com/oklog/ulid/v2"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	blobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type sqlBlobStore struct {
	blobContentRepository blobContent.Repository
}

func New(db *sql.DB, blobContentRepository blobContent.Repository) (blobstore.BlobStore, error) {
	return &sqlBlobStore{
		blobContentRepository: blobContentRepository,
	}, nil
}

func (bs *sqlBlobStore) Start(ctx context.Context) error {
	return nil
}

func (bs *sqlBlobStore) Stop(ctx context.Context) error {
	return nil
}

func (bs *sqlBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	blobContentEntity := blobContent.Entity{
		Id:      (*ulid.ULID)(&blobId),
		Content: content,
	}
	err = bs.blobContentRepository.PutBlobContent(ctx, tx, &blobContentEntity)
	if err != nil {
		return err
	}

	return nil
}

func (bs *sqlBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
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
	return bs.blobContentRepository.FindBlobContentIds(ctx, tx)
}

func (bs *sqlBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	err := bs.blobContentRepository.DeleteBlobContentById(ctx, tx, blobId)
	return err
}
