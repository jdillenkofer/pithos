package metadatapart

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/compression"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type gatedCompositionStore struct {
	partstore.PartStore
	active                   atomic.Bool
	started, release, closed chan struct{}
	once                     sync.Once
}

func (s *gatedCompositionStore) GetPart(ctx context.Context, tx database.Tx, id partstore.PartId) (io.ReadCloser, error) {
	r, err := s.PartStore.GetPart(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	// Deliberately hide Seek so Tink exercises its streaming buffer path.
	return &gatedCompositionReader{store: s, inner: r}, nil
}

type gatedCompositionReader struct {
	store *gatedCompositionStore
	inner io.ReadCloser
}

func (r *gatedCompositionReader) Read(p []byte) (int, error) {
	if r.store.active.Load() {
		r.store.once.Do(func() { close(r.store.started) })
		<-r.store.release
	}
	return r.inner.Read(p)
}
func (r *gatedCompositionReader) Close() error {
	if r.store.active.Load() {
		close(r.store.closed)
	}
	return r.inner.Close()
}

func TestBoundedReaderRealCompressionEncryptionCleanup(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, algorithm := range []compression.Algorithm{compression.AlgorithmGzip, compression.AlgorithmZstd} {
		t.Run(string(algorithm), func(t *testing.T) {
			ctx := context.Background()
			fs, err := filesystem.New(t.TempDir())
			require.NoError(t, err)
			gate := &gatedCompositionStore{PartStore: fs, started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			encrypted, err := tink.NewWithLocalKMS("test-password", gate, nil)
			require.NoError(t, err)
			composed, err := compression.NewWithConfig(encrypted, compression.Config{Algorithm: algorithm})
			require.NoError(t, err)
			require.NoError(t, composed.Start(ctx))
			defer composed.Stop(ctx)
			payload := make([]byte, 2*1024*1024)
			_, err = rand.Read(payload[64*1024:])
			require.NoError(t, err)
			id, err := partstore.NewRandomPartId()
			require.NoError(t, err)
			require.NoError(t, composed.PutPart(ctx, nil, *id, bytes.NewReader(payload)))
			base, cancel := context.WithCancel(ctx)
			boundedCtx := manualDeadline{base}
			reader, err := openBoundedPart(boundedCtx, func() (io.ReadCloser, error) { return composed.GetPart(boundedCtx, nil, *id) })
			require.NoError(t, err)
			gate.active.Store(true)
			done := make(chan error, 1)
			go func() { _, err := io.Copy(io.Discard, reader); done <- err }()
			select {
			case <-gate.started:
			case <-time.After(5 * time.Second):
				t.Fatal("decoder never reached gated transport")
			}
			cancel()
			require.ErrorIs(t, <-done, context.DeadlineExceeded)
			// Close must return without resetting a live gzip/zstd/Tink decoder.
			require.NoError(t, reader.Close())
			select {
			case <-gate.closed:
				t.Fatal("transport closed while composed Read was active")
			default:
			}
			close(gate.release)
			select {
			case <-gate.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not follow Read completion")
			}
			gate.active.Store(false)
			// Reuse pooled decoders on another request and verify no cross-request damage.
			next, err := composed.GetPart(ctx, nil, *id)
			require.NoError(t, err)
			actual, err := io.ReadAll(next)
			require.NoError(t, err)
			require.Equal(t, payload, actual)
			require.NoError(t, next.Close())
		})
	}
}
