package sftp

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

	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpBlobStore struct {
	addr         string
	clientConfig *ssh.ClientConfig
	root         string
	client       *sftp.Client
}

func (sftp *sftpBlobStore) ensureRootDir() error {
	err := sftp.client.MkdirAll(sftp.root)
	return err
}

func (sftp *sftpBlobStore) getFilename(blobId blobstore.BlobId) string {
	blobFilename := hex.EncodeToString(blobId[:])
	return filepath.Join(sftp.root, blobFilename)
}

func (sftp *sftpBlobStore) tryGetBlobIdFromFilename(filename string) (blobId *blobstore.BlobId, ok bool) {
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

func newSftpClient(addr string, config *ssh.ClientConfig) (*sftp.Client, error) {
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return sftpClient, nil
}

func New(addr string, clientConfig *ssh.ClientConfig, root string) (blobstore.BlobStore, error) {
	sftpClient, err := newSftpClient(addr, clientConfig)
	if err != nil {
		return nil, err
	}
	bs := &sftpBlobStore{
		addr:         addr,
		clientConfig: clientConfig,
		root:         root,
		client:       sftpClient,
	}
	err = bs.ensureRootDir()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (sftp *sftpBlobStore) Start(ctx context.Context) error {
	return nil
}

func (sftp *sftpBlobStore) Stop(ctx context.Context) error {
	err := sftp.client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sftp *sftpBlobStore) calculateETagFromPath(path string) (*string, error) {
	f, err := sftp.client.OpenFile(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	etag, err := blobstore.CalculateETag(f)
	if err != nil {
		return nil, err
	}
	return etag, nil
}

func (sftp *sftpBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*blobstore.PutBlobResult, error) {
	filename := sftp.getFilename(blobId)
	{
		f, err := sftp.client.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		_, err = io.Copy(f, reader)
		if err != nil {
			return nil, err
		}
	}
	etag, err := sftp.calculateETagFromPath(filename)
	if err != nil {
		return nil, err
	}
	stat, err := sftp.client.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &blobstore.PutBlobResult{
		BlobId: blobId,
		ETag:   *etag,
		Size:   stat.Size(),
	}, nil
}

func (sftp *sftpBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadSeekCloser, error) {
	filename := sftp.getFilename(blobId)
	f, err := sftp.client.OpenFile(filename, os.O_RDONLY)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, blobstore.ErrBlobNotFound
		}
		return nil, err
	}
	return f, err
}

func (sftp *sftpBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	dirEntries, err := sftp.client.ReadDir(sftp.root)
	if err != nil {
		return nil, err
	}
	blobIds := []blobstore.BlobId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if blobId, ok := sftp.tryGetBlobIdFromFilename(dirEntry.Name()); ok {
			blobIds = append(blobIds, *blobId)
		}
	}
	return blobIds, nil
}

func (sftp *sftpBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	filename := sftp.getFilename(blobId)
	err := sftp.client.Remove(filename)
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
