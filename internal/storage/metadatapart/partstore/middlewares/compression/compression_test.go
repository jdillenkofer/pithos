package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestCompressionPartStoreMiddleware_CompressibleContent(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	middleware, err := New(inner)
	if err != nil {
		t.Fatal(err)
	}

	content := bytes.Repeat([]byte("compress-me-"), 4096)
	err = partstore.Tester(middleware, db, content)
	assert.Nil(t, err)
}

func TestCompressionPartStoreMiddleware_IncompressibleContent(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	middleware, err := New(inner)
	if err != nil {
		t.Fatal(err)
	}

	content := make([]byte, 256*1024)
	rng := rand.New(rand.NewSource(42))
	_, err = rng.Read(content)
	if err != nil {
		t.Fatal(err)
	}

	err = partstore.Tester(middleware, db, content)
	assert.Nil(t, err)
}

func TestCompressionPartStoreMiddleware_Zstd(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	middleware, err := NewWithConfig(inner, Config{Algorithm: AlgorithmZstd})
	if err != nil {
		t.Fatal(err)
	}

	content := bytes.Repeat([]byte("compress-me-zstd-"), 4096)
	err = partstore.Tester(middleware, db, content)
	assert.Nil(t, err)
}

func TestCompressionPartStoreMiddleware_InvalidAlgorithm(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewWithConfig(inner, Config{Algorithm: "invalid"})
	assert.NotNil(t, err)
}

func TestCompressionPartStoreMiddleware_InvalidMaxCompressionRatio(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewWithConfig(inner, Config{MaxCompressionRatio: 1.1})
	assert.NotNil(t, err)
}

func TestEstimateSampleCompressionRatio(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewWithConfig(inner, Config{Algorithm: AlgorithmGzip})
	if err != nil {
		t.Fatal(err)
	}
	mw := store.(*PartStoreMiddleware)

	compressible := bytes.Repeat([]byte("abcabcabcabc"), 1024)
	ratio, err := mw.estimateSampleCompressionRatio(compressible)
	if err != nil {
		t.Fatal(err)
	}
	assert.Less(t, ratio, 0.95)
}

func TestCompressionPartStoreMiddleware_CrossAlgorithmRead(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	gzipStore, err := NewWithConfig(inner, Config{Algorithm: AlgorithmGzip})
	if err != nil {
		t.Fatal(err)
	}
	zstdStore, err := NewWithConfig(inner, Config{Algorithm: AlgorithmZstd})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := gzipStore.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer gzipStore.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("cross-algo-content-"), 4096)

	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return gzipStore.PutPart(ctx, tx, *partId, bytes.NewReader(content))
	}); err != nil {
		t.Fatal(err)
	}

	var got []byte
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := zstdStore.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		got, err = io.ReadAll(rc)
		rc.Close()
		return err
	}); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, content, got)
}

func TestCompressionPartStoreMiddleware_ReadsLegacyPlainDataWithoutHeader(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := New(inner)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("legacy-data-without-compression-header")

	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return inner.PutPart(ctx, tx, *partId, bytes.NewReader(content))
	}); err != nil {
		t.Fatal(err)
	}

	var got []byte
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := store.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		got, err = io.ReadAll(rc)
		rc.Close()
		return err
	}); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, content, got)
}

func TestCompressionPartStoreMiddleware_InvalidHeaderFallsBackToPlainData(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-compression-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
		}
	}()

	inner, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := New(inner)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatal(err)
	}

	header := newHeader(AlgorithmZstd)
	header[31] ^= 0xFF
	payload := []byte("payload-following-invalid-header")
	content := append(header[:], payload...)

	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return inner.PutPart(ctx, tx, *partId, bytes.NewReader(content))
	}); err != nil {
		t.Fatal(err)
	}

	var got []byte
	if err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := store.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		got, err = io.ReadAll(rc)
		rc.Close()
		return err
	}); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, content, got)
}

func TestCompressionPartStoreMiddleware_PayloadLargerThanSample(t *testing.T) {
	ctx := context.Background()
	content := compressiblePayload(3*defaultSampleSize + 123)
	for _, algorithm := range []Algorithm{AlgorithmZstd, AlgorithmGzip} {
		t.Run(string(algorithm), func(t *testing.T) {
			inner := &benchmarkPartStore{parts: map[partstore.PartId][]byte{}}
			store, err := NewWithConfig(inner, Config{Algorithm: algorithm})
			assert.NoError(t, err)
			partId, err := partstore.NewRandomPartId()
			assert.NoError(t, err)

			for i := 0; i < 3; i++ {
				assert.NoError(t, store.PutPart(ctx, nil, *partId, bytes.NewReader(content)))
				stored := inner.parts[*partId]
				assert.Less(t, len(stored), len(content)/2)

				algorithmInHeader, ok, err := parseHeader(stored[:headerSize])
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, algorithm, algorithmInHeader)

				// The stored stream must stay readable by a fresh standard decoder.
				var decoder io.Reader
				switch algorithm {
				case AlgorithmGzip:
					decoder, err = gzip.NewReader(bytes.NewReader(stored[headerSize:]))
				case AlgorithmZstd:
					decoder, err = zstd.NewReader(bytes.NewReader(stored[headerSize:]))
				}
				assert.NoError(t, err)
				decoded, err := io.ReadAll(decoder)
				assert.NoError(t, err)
				assert.Equal(t, content, decoded)

				rc, err := store.GetPart(ctx, nil, *partId)
				assert.NoError(t, err)
				data, err := io.ReadAll(rc)
				assert.NoError(t, err)
				assert.NoError(t, rc.Close())
				assert.Equal(t, content, data)
			}
		})
	}
}

type failingPartStore struct {
	benchmarkPartStore
}

var errInnerPutFailed = errors.New("inner put failed")

func (s *failingPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	if _, err := reader.Read(make([]byte, 16)); err != nil {
		return err
	}
	return errInnerPutFailed
}

func TestCompressionPartStoreMiddleware_InnerPutFailsEarly(t *testing.T) {
	content := compressiblePayload(4 * defaultSampleSize)
	store, err := New(&failingPartStore{})
	assert.NoError(t, err)
	partId, err := partstore.NewRandomPartId()
	assert.NoError(t, err)
	err = store.PutPart(context.Background(), nil, *partId, bytes.NewReader(content))
	assert.ErrorIs(t, err, errInnerPutFailed)
}
