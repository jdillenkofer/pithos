package filesystem

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

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
)

type filesystemBlobStore struct {
	*lifecycle.ValidatedLifecycle
	root string
}

var _ blobstore.BlobStore = (*filesystemBlobStore)(nil)

func (bs *filesystemBlobStore) ensureRootDir() error {
	err := os.MkdirAll(bs.root, os.ModePerm)
	return err
}

func (bs *filesystemBlobStore) getFilename(blobId blobstore.BlobId) string {
	blobFilename := hex.EncodeToString(blobId[:])
	return filepath.Join(bs.root, blobFilename)
}

func (bs *filesystemBlobStore) tryGetBlobIdFromFilename(filename string) (blobId *blobstore.BlobId, ok bool) {
	if len(filename) != 32 {
		return nil, false
	}
	blobIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, false
	}
	return &ulid.ULID{
		blobIdBytes[0], blobIdBytes[1], blobIdBytes[2], blobIdBytes[3],
		blobIdBytes[4], blobIdBytes[5], blobIdBytes[6], blobIdBytes[7],
		blobIdBytes[8], blobIdBytes[9], blobIdBytes[10], blobIdBytes[11],
		blobIdBytes[12], blobIdBytes[13], blobIdBytes[14], blobIdBytes[15],
	}, true
}

func New(root string) (blobstore.BlobStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("filesystemBlobStore")
	if err != nil {
		return nil, err
	}
	bs := &filesystemBlobStore{
		ValidatedLifecycle: validatedLifecycle,
		root:               root,
	}
	return bs, nil
}

func (bs *filesystemBlobStore) Start(ctx context.Context) error {
	return bs.ensureRootDir()
}

func (bs *filesystemBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	filename := bs.getFilename(blobId)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, reader)
	if err != nil {
		return err
	}

	return nil
}

func (bs *filesystemBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	filename := bs.getFilename(blobId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, blobstore.ErrBlobNotFound
		}
		return nil, err
	}
	return f, err
}

func (bs *filesystemBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	dirEntries, err := os.ReadDir(bs.root)
	if err != nil {
		return nil, err
	}
	blobIds := []blobstore.BlobId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if blobId, ok := bs.tryGetBlobIdFromFilename(dirEntry.Name()); ok {
			blobIds = append(blobIds, *blobId)
		}
	}
	return blobIds, nil
}

func (bs *filesystemBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
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
