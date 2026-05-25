package erasurecoding

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

type faultyPartStore struct {
	partstore.PartStore

	mu      sync.RWMutex
	missing map[string]struct{}
}

func newFaultyPartStore(inner partstore.PartStore) *faultyPartStore {
	return &faultyPartStore{
		PartStore: inner,
		missing:   make(map[string]struct{}),
	}
}

func (f *faultyPartStore) markMissing(partId partstore.PartId) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.missing[partId.String()] = struct{}{}
}

func (f *faultyPartStore) markAvailable(partId partstore.PartId) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.missing, partId.String())
}

func (f *faultyPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	f.mu.RLock()
	_, isMissing := f.missing[partId.String()]
	f.mu.RUnlock()
	if isMissing {
		return nil, partstore.ErrPartNotFound
	}

	return f.PartStore.GetPart(ctx, tx, partId)
}

func (f *faultyPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	err := f.PartStore.PutPart(ctx, tx, partId, reader)
	if err == nil {
		f.markAvailable(partId)
	}
	return err
}

func (f *faultyPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	err := f.PartStore.DeletePart(ctx, tx, partId)
	if err == nil {
		f.markAvailable(partId)
	}
	return err
}

func createTestStore(t *testing.T, shardCount int) (partstore.PartStore, []*faultyPartStore, database.Database) {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	assert.Nil(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(storagePath) })

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	assert.Nil(t, err)
	t.Cleanup(func() { _ = db.Close() })

	stores := make([]partstore.PartStore, 0, shardCount)
	faultyStores := make([]*faultyPartStore, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		root := filepath.Join(storagePath, "parts", string(rune('a'+i)))
		s, err := filesystem.New(root)
		assert.Nil(t, err)
		faultyStore := newFaultyPartStore(s)
		stores = append(stores, faultyStore)
		faultyStores = append(faultyStores, faultyStore)
	}
	// Disable background healing here so only the tested read path performs repairs.
	ec, err := NewWithPartStores(2, 1, 64*1024, stores, WithHealScanInterval(0))
	assert.Nil(t, err)
	return ec, faultyStores, db
}

func TestErasureCodingPartStoreRoundtrip(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, _, db := createTestStore(t, 3)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	defer store.Stop(ctx)

	partId, _ := partstore.NewRandomPartId()
	data := bytes.Repeat([]byte("abc123"), 20000)

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	err := store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	assert.Eventually(t, func() bool {
		tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		rc, err := store.GetPart(ctx, tx, *partId)
		if err != nil {
			_ = tx.Commit()
			return false
		}
		out, err := io.ReadAll(rc)
		_ = rc.Close()
		_ = tx.Commit()
		if err != nil {
			return false
		}
		return bytes.Equal(out, data)
	}, 2*time.Second, 40*time.Millisecond)
}

func TestErasureCodingPartStoreAllowsConcurrentHealthyReads(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, _, db := createTestStore(t, 3)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	defer store.Stop(ctx)

	partId, _ := partstore.NewRandomPartId()
	data := bytes.Repeat([]byte("concurrent-read"), 10000)

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	err := store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	firstReader, err := store.GetPart(ctx, nil, *partId)
	if !assert.Nil(t, err) {
		return
	}
	defer firstReader.Close()

	done := make(chan error, 1)
	go func() {
		secondReader, err := store.GetPart(ctx, nil, *partId)
		if err != nil {
			done <- err
			return
		}
		out, err := io.ReadAll(secondReader)
		closeErr := secondReader.Close()
		if err != nil {
			done <- err
			return
		}
		if closeErr != nil {
			done <- closeErr
			return
		}
		if !bytes.Equal(out, data) {
			done <- fmt.Errorf("second read returned %d bytes, want %d", len(out), len(data))
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(2 * time.Second):
		assert.Fail(t, "second read did not complete while first read was still open")
	}
}

func TestErasureCodingPartStoreCanReconstructMissingShard(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, shardStores, db := createTestStore(t, 3)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	defer store.Stop(ctx)

	partId, _ := partstore.NewRandomPartId()
	data := bytes.Repeat([]byte("payload"), 25000)

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	err := store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	shardStores[0].markMissing(*partId)

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err := store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	out, err := io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Equal(t, data, out)
	assert.Nil(t, tx.Commit())

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err = shardStores[0].GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	_, err = io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	shardStores[1].markMissing(*partId)

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err = store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	out, err = io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Equal(t, data, out)
	assert.Nil(t, tx.Commit())

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err = shardStores[1].GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	_, err = io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())
}

func TestErasureCodingPartStoreBackgroundHealScanRepairsMissingShards(t *testing.T) {
	testutils.SkipIfIntegration(t)

	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	assert.Nil(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(storagePath) })

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	assert.Nil(t, err)
	t.Cleanup(func() { _ = db.Close() })

	stores := make([]partstore.PartStore, 0, 3)
	shardStores := make([]*faultyPartStore, 0, 3)
	for i := 0; i < 3; i++ {
		root := filepath.Join(storagePath, "parts", string(rune('a'+i)))
		s, err := filesystem.New(root)
		assert.Nil(t, err)
		faultyStore := newFaultyPartStore(s)
		stores = append(stores, faultyStore)
		shardStores = append(shardStores, faultyStore)
	}
	store, err := NewWithPartStores(2, 1, 64*1024, stores, WithHealScanInterval(30*time.Millisecond))
	assert.Nil(t, err)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	defer store.Stop(ctx)

	partId, _ := partstore.NewRandomPartId()
	data := bytes.Repeat([]byte("scan-heal"), 20000)

	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	err = store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	shardStores[0].markMissing(*partId)

	assert.Eventually(t, func() bool {
		tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		rc, err := shardStores[0].GetPart(ctx, tx, *partId)
		if err != nil {
			_ = tx.Commit()
			return false
		}
		_, err = io.ReadAll(rc)
		_ = rc.Close()
		_ = tx.Commit()
		return err == nil
	}, 2*time.Second, 40*time.Millisecond)

	shardStores[1].markMissing(*partId)

	assert.Eventually(t, func() bool {
		tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		rc, err := shardStores[1].GetPart(ctx, tx, *partId)
		if err != nil {
			_ = tx.Commit()
			return false
		}
		_, err = io.ReadAll(rc)
		_ = rc.Close()
		_ = tx.Commit()
		return err == nil
	}, 2*time.Second, 40*time.Millisecond)

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err := store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	out, err := io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Equal(t, data, out)
	assert.Nil(t, tx.Commit())
}
