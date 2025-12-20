package sftp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const maxStpRetries = 5

type sftpPartStore struct {
	addr         string
	clientConfig *ssh.ClientConfig
	root         string
	client       *sftp.Client
	sshClient    *ssh.Client
	mu           sync.Mutex
	tracer       trace.Tracer
}

// Compile-time check to ensure sftpPartStore implements partstore.PartStore
var _ partstore.PartStore = (*sftpPartStore)(nil)

func (s *sftpPartStore) ensureRootDir() error {
	_, err := doRetriableOperation(func() (*struct{}, error) {
		return nil, s.client.MkdirAll(s.root)
	}, maxStpRetries, s.reconnectSftpClient, nil)
	return err
}

func (s *sftpPartStore) getFilename(partId partstore.PartId) string {
	partFilename := hex.EncodeToString(partId.Bytes())
	return filepath.Join(s.root, partFilename)
}

func (s *sftpPartStore) tryGetPartIdFromFilename(filename string) (partId *partstore.PartId, ok bool) {
	if len(filename) != 32 {
		return nil, false
	}
	partIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, false
	}
	partId, err = partstore.NewPartIdFromBytes(partIdBytes)
	if err != nil {
		return nil, false
	}
	return partId, true
}

func (s *sftpPartStore) reconnectSftpClient() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		slog.Info(fmt.Sprintf("SFTP reconnecting to %s", s.addr))
		s.client.Close()
	}

	// Close the SSH connection properly
	if s.sshClient != nil {
		s.sshClient.Close()
	}

	client, err := ssh.Dial("tcp", s.addr, s.clientConfig)
	if err != nil {
		slog.Error(fmt.Sprintf("SFTP failed to establish SSH connection to %s: %v", s.addr, err))
		return err
	}
	s.sshClient = client // Store SSH client reference

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		slog.Error(fmt.Sprintf("SFTP failed to create SFTP client for %s: %v", s.addr, err))
		client.Close()
		s.sshClient = nil
		return err
	}
	s.client = sftpClient
	slog.Info(fmt.Sprintf("SFTP successfully reconnected to %s", s.addr))
	return nil
}

// getBackoffDuration returns the backoff duration for a given retry attempt.
// First retry is immediate, subsequent retries use exponential backoff:
// attempt 1: 0ms, attempt 2: 100ms, attempt 3: 1s, attempt 4: 5s, attempt 5: 10s
func getBackoffDuration(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 0 // First retry is immediate
	case 2:
		return 100 * time.Millisecond
	case 3:
		return 1 * time.Second
	case 4:
		return 5 * time.Second
	default:
		return 10 * time.Second
	}
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
				backoff := getBackoffDuration(retries)
				if backoff > 0 {
					slog.Warn(fmt.Sprintf("SFTP operation failed (attempt %d/%d): %v, waiting %v before reconnect", retries, maxRetries, err, backoff))
					time.Sleep(backoff)
				} else {
					slog.Warn(fmt.Sprintf("SFTP operation failed (attempt %d/%d): %v, attempting immediate reconnect", retries, maxRetries, err))
				}
				err = preRetry()
				if err != nil {
					slog.Error(fmt.Sprintf("SFTP reconnect failed: %v", err))
					return empty, err
				}
				continue
			}
			slog.Error(fmt.Sprintf("SFTP operation failed after %d retries: %v", retries, err))
			return empty, err
		}
		return t, nil
	}
}

func New(addr string, clientConfig *ssh.ClientConfig, root string) (partstore.PartStore, error) {
	bs := &sftpPartStore{
		addr:         addr,
		clientConfig: clientConfig,
		root:         root,
		client:       nil,
		tracer:       otel.Tracer("internal/storage/metadatapart/partstore/sftp"),
	}
	return bs, nil
}

func (s *sftpPartStore) Start(ctx context.Context) error {
	if err := s.reconnectSftpClient(); err != nil {
		return err
	}
	return s.ensureRootDir()
}

func (s *sftpPartStore) Stop(ctx context.Context) error {
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

func (s *sftpPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := s.tracer.Start(ctx, "sftpPartStore.PutPart")
	defer span.End()

	filename := s.getFilename(partId)
	f, err := doRetriableOperation(func() (*sftp.File, error) {
		return s.client.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY)
	}, maxStpRetries, s.reconnectSftpClient, nil)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.ReadFrom(reader)
	if err != nil {
		return err
	}
	return nil
}

func (s *sftpPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	_, span := s.tracer.Start(ctx, "sftpPartStore.GetPart")
	defer span.End()

	filename := s.getFilename(partId)
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
	           return nil, partstore.ErrPartNotFound
	       }
	       return nil, err
	   }
	*/

	f := ioutils.NewLazyReadSeekCloser(func() (io.ReadSeekCloser, error) {
		return doRetriableOperation(func() (*sftp.File, error) {
			f, err := s.client.OpenFile(filename, os.O_RDONLY)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil, partstore.ErrPartNotFound
				}
				return nil, err
			}
			return f, nil
		}, maxStpRetries, s.reconnectSftpClient, func(err error) bool {
			return errors.Is(err, partstore.ErrPartNotFound)
		})
	})
	return f, nil
}

func (s *sftpPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "sftpPartStore.GetPartIds")
	defer span.End()

	dirEntries, err := doRetriableOperation(func() ([]os.FileInfo, error) {
		return s.client.ReadDir(s.root)
	}, maxStpRetries, s.reconnectSftpClient, nil)
	if err != nil {
		return nil, err
	}
	partIds := []partstore.PartId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if partId, ok := s.tryGetPartIdFromFilename(dirEntry.Name()); ok {
			partIds = append(partIds, *partId)
		}
	}
	return partIds, nil
}

func (s *sftpPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	_, span := s.tracer.Start(ctx, "sftpPartStore.DeletePart")
	defer span.End()

	filename := s.getFilename(partId)
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
