package blob

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
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

var ErrBlobNotFound error = errors.New("blob not found")

type BlobStore interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error)
	GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error)
	GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error)
	DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error
}

func GenerateBlobId() (*BlobId, error) {
	blobIdBytes := make([]byte, 8)
	_, err := rand.Read(blobIdBytes)
	if err != nil {
		return nil, err
	}
	blobId := BlobId(ulid.Make())
	return &blobId, nil
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
