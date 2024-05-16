package blob

import (
	"bytes"
	"database/sql"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/oklog/ulid/v2"
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
	blobId := ulid.Make()
	_, err = bs.db.Exec("INSERT INTO blob_contents (id, content, created_at, updated_at) VALUES(?, ?, datetime('now'), datetime('now'))", blobId.String(), content)
	if err != nil {
		return nil, err
	}

	etag, err := calculateETag(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	return &PutBlobResult{
		BlobId: BlobId(blobId),
		ETag:   *etag,
		Size:   int64(len(content)),
	}, nil
}

func (bs *SqlBlobStore) GetBlob(blobId BlobId) (io.ReadSeekCloser, error) {
	row := bs.db.QueryRow("SELECT content FROM blob_contents WHERE id = ?", blobId.String())
	var content []byte
	err := row.Scan(&content)
	if err != nil {
		return nil, err
	}
	reader := ioutils.NewByteReadSeekCloser(content)

	return reader, nil
}

func (bs *SqlBlobStore) DeleteBlob(blobId BlobId) error {
	_, err := bs.db.Exec("DELETE FROM blob_contents WHERE id = ?", blobId.String())
	return err
}
