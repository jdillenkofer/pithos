package entropy

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestEntropyCompressionPartStoreMiddleware_CompressibleContent(t *testing.T) {
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

func TestEntropyCompressionPartStoreMiddleware_IncompressibleContent(t *testing.T) {
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

func TestCalculateEntropy(t *testing.T) {
	lowEntropy := bytes.Repeat([]byte{0x00}, 4096)
	highEntropy := make([]byte, 4096)
	rng := rand.New(rand.NewSource(99))
	_, err := rng.Read(highEntropy)
	if err != nil {
		t.Fatal(err)
	}

	assert.Less(t, calculateEntropy(lowEntropy), 1.0)
	assert.Greater(t, calculateEntropy(highEntropy), 7.0)
}

func TestEntropyCompressionPartStoreMiddleware_Zstd(t *testing.T) {
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

func TestEntropyCompressionPartStoreMiddleware_InvalidAlgorithm(t *testing.T) {
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

func TestEntropyCompressionPartStoreMiddleware_InvalidMaxEntropy(t *testing.T) {
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

	_, err = NewWithConfig(inner, Config{MaxEntropy: 8.1})
	assert.NotNil(t, err)
}

func TestEntropyCompressionPartStoreMiddleware_InvalidMaxCompressionRatio(t *testing.T) {
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
	mw := store.(*EntropyCompressionPartStoreMiddleware)

	compressible := bytes.Repeat([]byte("abcabcabcabc"), 1024)
	ratio, err := mw.estimateSampleCompressionRatio(compressible)
	if err != nil {
		t.Fatal(err)
	}
	assert.Less(t, ratio, 0.95)
}

func TestEntropyCompressionPartStoreMiddleware_CrossAlgorithmRead(t *testing.T) {
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

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := gzipStore.PutPart(ctx, tx, *partId, bytes.NewReader(content)); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rc, err := zstdStore.GetPart(ctx, tx, *partId)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, content, got)
}
