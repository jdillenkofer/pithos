package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type cancelAwarePartStore struct {
	processingStarted chan struct{}
	stopped           chan struct{}
	startOnce         sync.Once
	stopOnce          sync.Once
}

func newCancelAwarePartStore() *cancelAwarePartStore {
	return &cancelAwarePartStore{
		processingStarted: make(chan struct{}),
		stopped:           make(chan struct{}),
	}
}

func (s *cancelAwarePartStore) Start(context.Context) error { return nil }

func (s *cancelAwarePartStore) Stop(context.Context) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func (s *cancelAwarePartStore) PutPart(ctx context.Context, _ database.Tx, _ partstore.PartId, _ io.Reader) error {
	s.startOnce.Do(func() { close(s.processingStarted) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *cancelAwarePartStore) GetPart(context.Context, database.Tx, partstore.PartId) (io.ReadCloser, error) {
	return nil, partstore.ErrPartNotFound
}

func (s *cancelAwarePartStore) GetPartIds(context.Context, database.Tx) ([]partstore.PartId, error) {
	return nil, nil
}

func (s *cancelAwarePartStore) DeletePart(ctx context.Context, _ database.Tx, _ partstore.PartId) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestStopCancelsInFlightReplayBeforeStoppingInnerStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	require.NoError(t, err)
	defer db.Close()
	repo, err := repositoryFactory.NewPartOutboxEntryRepository(db)
	require.NoError(t, err)
	inner := newCancelAwarePartStore()
	store, err := New(db, "default", inner, repo, prometheus.NewRegistry(), 30*time.Second)
	require.NoError(t, err)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("blocked replay")))
	}))
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
		t.Fatal("inner store was not stopped after the worker exited")
	}
}
