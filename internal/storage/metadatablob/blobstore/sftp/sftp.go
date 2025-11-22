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
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
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
	sshClient    *ssh.Client
	mu           sync.Mutex
	tracer       trace.Tracer
}

// Compile-time check to ensure sftpBlobStore implements blobstore.BlobStore
var _ blobstore.BlobStore = (*sftpBlobStore)(nil)

func (s *sftpBlobStore) ensureRootDir() error {
	_, err := doRetriableOperation(func() (*struct{}, error) {
		return nil, s.client.MkdirAll(s.root)
	}, maxStpRetries, s.reconnectSftpClient, nil)
	return err
}

func (s *sftpBlobStore) getFilename(blobId blobstore.BlobId) string {
	blobFilename := hex.EncodeToString(blobId.Bytes())
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
	blobId, err = blobstore.NewBlobIdFromBytes(blobIdBytes)
	if err != nil {
		return nil, false
	}
	return blobId, true
}

func (s *sftpBlobStore) reconnectSftpClient() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		// If we have a retry wait a couple of seconds before continuing
		time.Sleep(waitDurationBeforeRetry)
		s.client.Close()
	}

	// Close the SSH connection properly
	if s.sshClient != nil {
		s.sshClient.Close()
	}

	client, err := ssh.Dial("tcp", s.addr, s.clientConfig)
	if err != nil {
		return err
	}
	s.sshClient = client // Store SSH client reference

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		s.sshClient = nil
		return err
	}
	s.client = sftpClient
	return nil
}

func doRetriableOperation[T any](op func() (T, error), maxRetries int, preRetry func() error, shouldIgnoreError func(error) bool) (T, error) {
	retries := 0
	var empty T
	for {
		t, err := op()
		if err != nil {
			if shouldIgnoreError != nil && shouldIgnoreError(err) {
				return empty, err
			}

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
		tracer:       otel.Tracer("internal/storage/metadatablob/blobstore/sftp"),
	}
	return bs, nil
}

func (s *sftpBlobStore) Start(ctx context.Context) error {
	if err := s.reconnectSftpClient(); err != nil {
		return err
	}
	return s.ensureRootDir()
}

func (s *sftpBlobStore) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		s.client.Close()
	}
	if s.sshClient != nil {
		return s.sshClient.Close()
	}
	return nil
}

func (s *sftpBlobStore) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	_, span := s.tracer.Start(ctx, "sftpBlobStore.PutBlob")
	defer span.End()

	filename := s.getFilename(blobId)
	f, err := doRetriableOperation(func() (*sftp.File, error) {
		return s.client.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	}, maxStpRetries, s.reconnectSftpClient, nil)
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
	_, span := s.tracer.Start(ctx, "sftpBlobStore.GetBlob")
	defer span.End()

	filename := s.getFilename(blobId)
	// @Perf: We skip the stat call here to reduce the number of roundtrips.
	// This means that if the file doesn't exist, we will only find out when we try
	// to read from it.
	/*
	   _, err := doRetriableOperation(func() (*struct{}, error) {
	       _, err := s.client.Stat(filename)
	       return nil, err
	   }, maxStpRetries, s.reconnectSftpClient, func(err error) bool { return errors.Is(err, fs.ErrNotExist) })
	   if err != nil {
	       if errors.Is(err, fs.ErrNotExist) {
	           return nil, blobstore.ErrBlobNotFound
	       }
	       return nil, err
	   }
	*/

	f := ioutils.NewLazyReadSeekCloser(func() (io.ReadSeekCloser, error) {
		_, span := s.tracer.Start(ctx, "sftpBlobStore.GetBlob.LazyInit")
		defer span.End()

		return doRetriableOperation(func() (*sftp.File, error) {
			f, err := s.client.OpenFile(filename, os.O_RDONLY)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil, blobstore.ErrBlobNotFound
				}
				return nil, err
			}
			return f, nil
		}, maxStpRetries, s.reconnectSftpClient, func(err error) bool {
			return errors.Is(err, blobstore.ErrBlobNotFound)
		})
	})
	return f, nil
}

func (s *sftpBlobStore) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	_, span := s.tracer.Start(ctx, "sftpBlobStore.GetBlobIds")
	defer span.End()

	dirEntries, err := doRetriableOperation(func() ([]os.FileInfo, error) {
		return s.client.ReadDir(s.root)
	}, maxStpRetries, s.reconnectSftpClient, nil)
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
	_, span := s.tracer.Start(ctx, "sftpBlobStore.DeleteBlob")
	defer span.End()

	filename := s.getFilename(blobId)
	_, err := doRetriableOperation(func() (*struct{}, error) {
		return nil, s.client.Remove(filename)
	}, maxStpRetries, s.reconnectSftpClient, func(err error) bool {
		// Check for both ENOENT and fs.ErrNotExist to be safe
		return errors.Is(err, syscall.ENOENT) || errors.Is(err, fs.ErrNotExist)
	})
	if err != nil {
		// If the error is "file not found", we consider it a success (idempotent delete)
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}
