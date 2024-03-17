package blob

import (
	"io"

	"github.com/oklog/ulid/v2"
)

type BlobId = ulid.ULID

type PutBlobResult struct {
	BlobId BlobId
	Size   int64
	ETag   string
}

type BlobStore interface {
	PutBlob(blob io.Reader) (*PutBlobResult, error)
	GetBlob(blobId BlobId) (io.ReadSeekCloser, error)
	DeleteBlob(blobId BlobId) error
	Clear() error
}
