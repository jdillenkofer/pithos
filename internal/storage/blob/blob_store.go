package blob

import "io"

type BlobId = uint64

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
