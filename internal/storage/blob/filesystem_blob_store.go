package blob

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

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

func (bs *FilesystemBlobStore) getBlobId(filename string) (*BlobId, error) {
	if len(filename) != 32 {
		return nil, errors.New("invalid blobId")
	}
	blobIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, err
	}
	return &ulid.ULID{
		blobIdBytes[0], blobIdBytes[1], blobIdBytes[2], blobIdBytes[3], blobIdBytes[4], blobIdBytes[5], blobIdBytes[6], blobIdBytes[7],
		blobIdBytes[8], blobIdBytes[9], blobIdBytes[10], blobIdBytes[11], blobIdBytes[12], blobIdBytes[13], blobIdBytes[14], blobIdBytes[15],
	}, nil
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

func (bs *FilesystemBlobStore) Start(ctx context.Context) error {
	return nil
}

func (bs *FilesystemBlobStore) Stop(ctx context.Context) error {
	return nil
}

func (bs *FilesystemBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, blob io.Reader) (*PutBlobResult, error) {
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

func (bs *FilesystemBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error) {
	filename := bs.getFilename(blobId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrBlobNotFound
		}
		return nil, err
	}
	return f, err
}

func (bs *FilesystemBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error) {
	dirEntries, err := os.ReadDir(bs.root)
	if err != nil {
		return nil, err
	}
	blobIds := []BlobId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if ulidPtr, err := bs.getBlobId(dirEntry.Name()); err == nil {
			blobIds = append(blobIds, *ulidPtr)
		}
	}
	return blobIds, nil
}

func (bs *FilesystemBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error {
	filename := bs.getFilename(blobId)
	err := os.Remove(filename)
	if err != nil {
		e, ok := err.(*os.PathError)
		if ok && e.Err == syscall.ENOENT {
			// The file didn't exist
		} else {
			return err
		}
	}
	return nil
}
