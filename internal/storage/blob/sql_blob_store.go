package blob

import (
	"bytes"
	"context"
	"database/sql"
	"io"

	"github.com/oklog/ulid/v2"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	blobContentRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent"
	sqliteBlobContentRepository "github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent/sqlite"
)

type SqlBlobStore struct {
	blobContentRepository blobContentRepository.BlobContentRepository
}

func NewSqlBlobStore(db *sql.DB) (*SqlBlobStore, error) {
	blobContentRepository, err := sqliteBlobContentRepository.New(db)
	if err != nil {
		return nil, err
	}
	return &SqlBlobStore{
		blobContentRepository: blobContentRepository,
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
	blobContentEntity := blobContentRepository.BlobContentEntity{
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

func (bs *SqlBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error) {
	return bs.blobContentRepository.FindBlobContentIds(ctx, tx)
}

func (bs *SqlBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	err := bs.blobContentRepository.DeleteBlobContentById(ctx, tx, blobId)
	return err
}
