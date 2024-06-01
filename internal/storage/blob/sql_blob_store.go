package blob

import (
	"bytes"
	"database/sql"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/repository"
)

type SqlBlobStore struct {
	db                    *sql.DB
	blobContentRepository repository.BlobContentRepository
}

func NewSqlBlobStore(db *sql.DB) (*SqlBlobStore, error) {
	return &SqlBlobStore{
		db:                    db,
		blobContentRepository: repository.NewBlobContentRepository(db),
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
	err = bs.blobContentRepository.SaveBlobContent(tx, &blobContentEntity)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	blobId := blobContentEntity.Id

	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

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
	blobContentEntity, err := bs.blobContentRepository.FindBlobContentById(tx, blobId)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if blobContentEntity == nil {
		tx.Rollback()
		return nil, nil
	}
	reader := ioutils.NewByteReadSeekCloser(blobContentEntity.Content)

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (bs *SqlBlobStore) DeleteBlob(blobId BlobId) error {
	tx, err := bs.db.Begin()
	if err != nil {
		return err
	}

	err = bs.blobContentRepository.DeleteBlobContentById(tx, blobId)
	if err != nil {
		tx.Rollback()
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
