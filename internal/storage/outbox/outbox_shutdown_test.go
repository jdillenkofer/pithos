package outbox

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type cancelAwareStorage struct {
	storage.Storage
	processingStarted chan struct{}
	stopped           chan struct{}
	startOnce         sync.Once
	stopOnce          sync.Once
}

func newCancelAwareStorage() *cancelAwareStorage {
	return &cancelAwareStorage{
		processingStarted: make(chan struct{}),
		stopped:           make(chan struct{}),
	}
}

func (s *cancelAwareStorage) Start(context.Context) error { return nil }

func (s *cancelAwareStorage) Stop(context.Context) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func (s *cancelAwareStorage) GetBucketVersioningConfiguration(context.Context, storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	return nil, storage.ErrNoSuchBucket
}

func (s *cancelAwareStorage) PutObject(ctx context.Context, _ storage.BucketName, _ storage.ObjectKey, _ *string, _ io.Reader, _ *storage.ChecksumInput, _ *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	s.startOnce.Do(func() { close(s.processingStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestStopCancelsInFlightReplayBeforeStoppingInnerStorage(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	require.NoError(t, err)
	defer db.Close()
	repo, err := repositoryFactory.NewStorageOutboxEntryRepository(db)
	require.NoError(t, err)
	inner := newCancelAwareStorage()
	store, err := NewStorage(db, "default", inner, repo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)

	_, err = store.PutObject(ctx, storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, bytes.NewReader([]byte("blocked replay")), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))

	select {
	case <-inner.processingStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("outbox replay did not start")
	}

	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	require.NoError(t, store.Stop(stopCtx))
	select {
	case <-inner.stopped:
	default:
		t.Fatal("inner storage was not stopped after the worker exited")
	}
}
