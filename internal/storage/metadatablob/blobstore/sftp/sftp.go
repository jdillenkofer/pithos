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
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const maxStpRetries = 3
const waitDurationBeforeRetry = 3 * time.Second

type sftpBlobStore struct {
	addr         string
	clientConfig *ssh.ClientConfig
	root         string
	client       *sftp.Client
}

func (s *sftpBlobStore) ensureRootDir() error {
	_, err := doRetriableOperation(func() (*struct{}, error) {
		return nil, s.client.MkdirAll(s.root)
	}, maxStpRetries, s.reconnectSftpClient)
	return err
}

func (s *sftpBlobStore) getFilename(blobId blobstore.BlobId) string {
	blobFilename := hex.EncodeToString(blobId[:])
	return filepath.Join(s.root, blobFilename)
}

func (s *sftpBlobStore) tryGetBlobIdFromFilename(filename string) (blobId *blobstore.BlobId, ok bool) {
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

func (s *sftpBlobStore) reconnectSftpClient() error {
	if s.client != nil {
		// If we have a retry wait a couple of seconds before continuing
		time.Sleep(waitDurationBeforeRetry)
		s.client.Close()
	}

	client, err := ssh.Dial("tcp", s.addr, s.clientConfig)
	if err != nil {
		return err
	}

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return err
	}
	s.client = sftpClient
	return nil
}

func doRetriableOperation[T any](op func() (T, error), maxRetries int, preRetry func() error) (T, error) {
	retries := 0
	var empty T
	for {
		t, err := op()
		if err != nil {
			retries += 1
			if retries < maxRetries {
				err = preRetry()
				if err != nil {
					return empty, err
				}
				continue
			}
			return empty, err
		}
		return t, nil
	}
}

func New(addr string, clientConfig *ssh.ClientConfig, root string) (blobstore.BlobStore, error) {
	bs := &sftpBlobStore{
		addr:         addr,
		clientConfig: clientConfig,
		root:         root,
		client:       nil,
	}
	return bs, nil
}

func (s *sftpBlobStore) Start(ctx context.Context) error {
	err := s.reconnectSftpClient()
	if err != nil {
		return err
	}

	err = s.ensureRootDir()
	if err != nil {
		return err
	}
	return nil
}

func (s *sftpBlobStore) Stop(ctx context.Context) error {
	err := s.client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *sftpBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	filename := s.getFilename(blobId)
	f, err := doRetriableOperation(func() (*sftp.File, error) {
		return s.client.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	}, maxStpRetries, s.reconnectSftpClient)
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

func (s *sftpBlobStore) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	filename := s.getFilename(blobId)
	// @Perf: We skip the stat call here to reduce the number of roundtrips.
	// This means that if the file doesn't exist, we will only find out when we try
	// to read from it.
	/*
		_, err := doRetriableOperation(func() (*struct{}, error) {
			_, err := s.client.Stat(filename)
			return nil, err
		}, maxStpRetries, s.reconnectSftpClient)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, blobstore.ErrBlobNotFound
			}
			return nil, err
		}
	*/

	f := ioutils.NewLazyReadSeekCloser(func() (io.ReadSeekCloser, error) {
		return doRetriableOperation(func() (*sftp.File, error) {
			f, err := s.client.OpenFile(filename, os.O_RDONLY)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil, blobstore.ErrBlobNotFound
				}
				return nil, err
			}
			return f, nil
		}, maxStpRetries, s.reconnectSftpClient)
	})
	return f, nil
}

func (s *sftpBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	dirEntries, err := doRetriableOperation(func() ([]os.FileInfo, error) {
		return s.client.ReadDir(s.root)
	}, maxStpRetries, s.reconnectSftpClient)
	if err != nil {
		return nil, err
	}
	blobIds := []blobstore.BlobId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if blobId, ok := s.tryGetBlobIdFromFilename(dirEntry.Name()); ok {
			blobIds = append(blobIds, *blobId)
		}
	}
	return blobIds, nil
}

func (s *sftpBlobStore) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	filename := s.getFilename(blobId)
	_, err := doRetriableOperation(func() (*struct{}, error) {
		return nil, s.client.Remove(filename)
	}, maxStpRetries, s.reconnectSftpClient)
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
