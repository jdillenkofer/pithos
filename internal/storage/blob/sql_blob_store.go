package blob

import (
	"bytes"
	"context"
	"database/sql"
	"io"

	"github.com/oklog/ulid/v2"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type SqlBlobStore struct {
	blobContentRepository repository.BlobContentRepository
}

func NewSqlBlobStore() (*SqlBlobStore, error) {
	return &SqlBlobStore{
		blobContentRepository: repository.NewBlobContentRepository(),
	}, nil
}

func (bs *SqlBlobStore) Start(ctx context.Context) error {
	return nil
}

func (bs *SqlBlobStore) Stop(ctx context.Context) error {
	return nil
}

func (bs *SqlBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
	content, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}
	blobContentEntity := repository.BlobContentEntity{
		Id:      (*ulid.ULID)(&blobId),
		Content: content,
	}
	err = bs.blobContentRepository.PutBlobContent(ctx, tx, &blobContentEntity)
	if err != nil {
		return nil, err
	}

	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	return &PutBlobResult{
		BlobId: blobId,
		ETag:   *etag,
		Size:   int64(len(content)),
	}, nil
}

func (bs *SqlBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	blobContentEntity, err := bs.blobContentRepository.FindBlobContentById(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}
	if blobContentEntity == nil {
		return nil, ErrBlobNotFound
	}
	reader := ioutils.NewByteReadSeekCloser(blobContentEntity.Content)

	return reader, nil
}

func (bs *SqlBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	err := bs.blobContentRepository.DeleteBlobContentById(ctx, tx, blobId)
	return err
}
