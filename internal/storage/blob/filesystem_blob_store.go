package blob

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/oklog/ulid/v2"
)

type FilesystemBlobStore struct {
	root string
}

func (bs *FilesystemBlobStore) ensureRootDir() error {
	err := os.MkdirAll(bs.root, os.ModePerm)
	return err
}

func (bs *FilesystemBlobStore) getFilename(blobId BlobId) string {
	blobFilename := hex.EncodeToString(blobId[:])
	return filepath.Join(bs.root, blobFilename)
}

func NewFilesystemBlobStore(root string) (*FilesystemBlobStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	bs := &FilesystemBlobStore{
		root: root,
	}
	err = bs.ensureRootDir()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (bs *FilesystemBlobStore) Start() error {
	return nil
}

func (bs *FilesystemBlobStore) Stop() error {
	return nil
}

func (bs *FilesystemBlobStore) PutBlob(tx *sql.Tx, blob io.Reader) (*PutBlobResult, error) {
	blobIdBytes := make([]byte, 8)
	_, err := rand.Read(blobIdBytes)
	if err != nil {
		return nil, err
	}
	blobId := BlobId(ulid.Make())
	filename := bs.getFilename(blobId)
	{
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		_, err = io.Copy(f, blob)
		if err != nil {
			return nil, err
		}
	}
	etag, err := calculateETagFromPath(filename)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &PutBlobResult{
		BlobId: blobId,
		ETag:   *etag,
		Size:   stat.Size(),
	}, nil
}

func (bs *FilesystemBlobStore) GetBlob(tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	filename := bs.getFilename(blobId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	return f, err
}

func (bs *FilesystemBlobStore) DeleteBlob(tx *sql.Tx, blobId BlobId) error {
	filename := bs.getFilename(blobId)
	err := os.Remove(filename)
	return err
}
