package blob

import (
	"bytes"
	"database/sql"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type SqlBlobStore struct {
	db *sql.DB
}

func NewSqlBlobStore(db *sql.DB) (*SqlBlobStore, error) {
	return &SqlBlobStore{
		db: db,
	}, nil
}

func (bs *SqlBlobStore) PutBlob(blob io.Reader) (*PutBlobResult, error) {
	content, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}
	tx, err := bs.db.Begin()
	if err != nil {
		return nil, err
	}
	blobContentEntity := repository.BlobContentEntity{
		Content: content,
	}
	err = repository.SaveBlobContent(tx, &blobContentEntity)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	blobId := blobContentEntity.Id

	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	tx.Commit()
	return &PutBlobResult{
		BlobId: BlobId(*blobId),
		ETag:   *etag,
		Size:   int64(len(content)),
	}, nil
}

func (bs *SqlBlobStore) GetBlob(blobId BlobId) (io.ReadSeekCloser, error) {
	tx, err := bs.db.Begin()
	if err != nil {
		return nil, err
	}
	blobContentEntity, err := repository.FindBlobContentById(tx, blobId)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if blobContentEntity == nil {
		tx.Rollback()
		return nil, nil
	}
	reader := ioutils.NewByteReadSeekCloser(blobContentEntity.Content)

	tx.Commit()
	return reader, nil
}

func (bs *SqlBlobStore) DeleteBlob(blobId BlobId) error {
	tx, err := bs.db.Begin()
	if err != nil {
		return err
	}

	err = repository.DeleteBlobContentById(tx, blobId)
	if err != nil {
		tx.Rollback()
	}
	tx.Commit()
	return err
}
