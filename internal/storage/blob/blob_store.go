package blob

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"io"
	"os"

	"github.com/oklog/ulid/v2"
)

type BlobId = ulid.ULID

type PutBlobResult struct {
	BlobId BlobId
	Size   int64
	ETag   string
}

type BlobStore interface {
	Start() error
	Stop() error
	PutBlob(tx *sql.Tx, blob io.Reader) (*PutBlobResult, error)
	GetBlob(tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error)
	DeleteBlob(tx *sql.Tx, blobId BlobId) error
}

func calculateMd5Sum(reader io.Reader) (*string, error) {
	hash := md5.New()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	_, err = hash.Write(data)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	hexSum := hex.EncodeToString(sum)
	return &hexSum, nil
}

func calculateETag(reader io.Reader) (*string, error) {
	md5Sum, err := calculateMd5Sum(reader)
	if err != nil {
		return nil, err
	}
	etag := "\"" + *md5Sum + "\""
	return &etag, nil
}

func calculateETagFromPath(path string) (*string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	etag, err := calculateETag(f)
	if err != nil {
		return nil, err
	}
	return etag, nil
}
