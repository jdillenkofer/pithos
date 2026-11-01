package erasurecoding

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
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

func (f *faultyPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	f.mu.RLock()
	_, isMissing := f.missing[partId.String()]
	f.mu.RUnlock()
	if isMissing {
		return nil, partstore.ErrPartNotFound
	}

	return f.PartStore.GetPart(ctx, tx, partId)
}

func (f *faultyPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	err := f.PartStore.PutPart(ctx, tx, partId, reader)
	if err == nil {
		f.markAvailable(partId)
	}
	return err
}

func (f *faultyPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
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

	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	})
	assert.Nil(t, err)

	assert.Eventually(t, func() bool {
		var out []byte
		err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rc, err := store.GetPart(ctx, tx, *partId)
			if err != nil {
				return err
			}
			out, err = io.ReadAll(rc)
			_ = rc.Close()
			return err
		})
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

	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	})
	assert.Nil(t, err)

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

	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	})
	assert.Nil(t, err)

	shardStores[0].markMissing(*partId)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := store.GetPart(ctx, tx, *partId)
		assert.Nil(t, err)
		out, err := io.ReadAll(rc)
		_ = rc.Close()
		assert.Nil(t, err)
		assert.Equal(t, data, out)
		return nil
	})
	assert.Nil(t, err)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := shardStores[0].GetPart(ctx, tx, *partId)
		assert.Nil(t, err)
		_, err = io.ReadAll(rc)
		_ = rc.Close()
		assert.Nil(t, err)
		return nil
	})
	assert.Nil(t, err)

	shardStores[1].markMissing(*partId)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := store.GetPart(ctx, tx, *partId)
		assert.Nil(t, err)
		out, err := io.ReadAll(rc)
		_ = rc.Close()
		assert.Nil(t, err)
		assert.Equal(t, data, out)
		return nil
	})
	assert.Nil(t, err)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := shardStores[1].GetPart(ctx, tx, *partId)
		assert.Nil(t, err)
		_, err = io.ReadAll(rc)
		_ = rc.Close()
		assert.Nil(t, err)
		return nil
	})
	assert.Nil(t, err)
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

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	})
	assert.Nil(t, err)

	shardStores[0].markMissing(*partId)

	assert.Eventually(t, func() bool {
		err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rc, err := shardStores[0].GetPart(ctx, tx, *partId)
			if err != nil {
				return err
			}
			_, err = io.ReadAll(rc)
			_ = rc.Close()
			return err
		})
		return err == nil
	}, 10*time.Second, 40*time.Millisecond)

	shardStores[1].markMissing(*partId)

	assert.Eventually(t, func() bool {
		err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			rc, err := shardStores[1].GetPart(ctx, tx, *partId)
			if err != nil {
				return err
			}
			_, err = io.ReadAll(rc)
			_ = rc.Close()
			return err
		})
		return err == nil
	}, 10*time.Second, 40*time.Millisecond)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rc, err := store.GetPart(ctx, tx, *partId)
		assert.Nil(t, err)
		out, err := io.ReadAll(rc)
		_ = rc.Close()
		assert.Nil(t, err)
		assert.Equal(t, data, out)
		return nil
	})
	assert.Nil(t, err)
}

func TestErasureCodingPartStoreHealsMissingParityShard(t *testing.T) {
	testutils.SkipIfIntegration(t)

	store, shardStores, db := createTestStore(t, 3)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	defer store.Stop(ctx)

	partId, _ := partstore.NewRandomPartId()
	data := bytes.Repeat([]byte("parity-heal"), 25000)

	err := database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	})
	assert.Nil(t, err)

	readAll := func() []byte {
		rc, err := store.GetPart(ctx, nil, *partId)
		if !assert.Nil(t, err) {
			return nil
		}
		out, err := io.ReadAll(rc)
		assert.Nil(t, err)
		assert.Nil(t, rc.Close())
		return out
	}

	// Lose the parity shard; the read heals it.
	shardStores[2].markMissing(*partId)
	assert.Equal(t, data, readAll())

	// The healed parity shard must be able to stand in for a lost data shard.
	shardStores[0].markMissing(*partId)
	assert.Equal(t, data, readAll())
}

func newMemoryErasureCodingStore(t *testing.T, dataShards, parityShards int) (partstore.PartStore, []*benchmarkPartStore) {
	t.Helper()
	shardStores := make([]*benchmarkPartStore, dataShards+parityShards)
	stores := make([]partstore.PartStore, len(shardStores))
	for i := range shardStores {
		shardStores[i] = newBenchmarkPartStore(0, false)
		stores[i] = shardStores[i]
	}
	store, err := NewWithPartStores(dataShards, parityShards, 64*1024, stores, WithHealScanInterval(0))
	assert.Nil(t, err)
	return store, shardStores
}

// frameOffset returns where frame stripeIndex starts in a shard of full stripes.
func frameOffset(stripeIndex int) int {
	return shardHeaderSize + stripeIndex*(frameHeaderSize+64*1024)
}

func readPart(t *testing.T, store partstore.PartStore, partId partstore.PartId) ([]byte, error) {
	t.Helper()
	rc, err := store.GetPart(context.Background(), nil, partId)
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(rc)
	assert.Nil(t, rc.Close())
	return out, err
}

func TestErasureCodingPartStoreToleratesShardFailuresMidStream(t *testing.T) {
	store, shardStores := newMemoryErasureCodingStore(t, 4, 2)
	partId, _ := partstore.NewRandomPartId()
	data := make([]byte, 4*1024*1024+12345)
	rand.New(rand.NewSource(1)).Read(data)
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, bytes.NewReader(data)))

	// Corrupt a payload byte of data shard 1 in stripe 3.
	shardStores[1].parts[*partId][frameOffset(3)+frameHeaderSize+10] ^= 0xff
	out, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, data, out)

	// Additionally truncate data shard 2 after stripe 5, which needs the
	// second parity shard to skip ahead.
	shardStores[2].parts[*partId] = shardStores[2].parts[*partId][:frameOffset(6)]
	out, err = readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, data, out)

	// A third failure exceeds the parity.
	shardStores[0].parts[*partId][frameOffset(8)+frameHeaderSize] ^= 0xff
	_, err = readPart(t, store, *partId)
	assert.NotNil(t, err)
}

func TestErasureCodingPartStoreDetectsTruncatedDataShardsAtFrameBoundary(t *testing.T) {
	store, shardStores := newMemoryErasureCodingStore(t, 1, 1)
	partId, _ := partstore.NewRandomPartId()
	data := make([]byte, 8*64*1024)
	rand.New(rand.NewSource(1)).Read(data)
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, bytes.NewReader(data)))

	shardStores[0].parts[*partId] = shardStores[0].parts[*partId][:frameOffset(4)]
	out, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, data, out)
}

func TestErasureCodingPartStoreRoundtripsEdgeSizes(t *testing.T) {
	store, _ := newMemoryErasureCodingStore(t, 3, 2)
	for _, size := range []int{0, 1, 64*1024 - 1, 3 * 64 * 1024, 3*64*1024 + 1, 10*3*64*1024 + 7} {
		partId, _ := partstore.NewRandomPartId()
		data := make([]byte, size)
		rand.New(rand.NewSource(int64(size))).Read(data)
		assert.Nil(t, store.PutPart(context.Background(), nil, *partId, bytes.NewReader(data)))
		out, err := readPart(t, store, *partId)
		assert.Nil(t, err)
		assert.Equal(t, len(data), len(out), "size %d", size)
		assert.True(t, bytes.Equal(data, out), "size %d", size)
	}
}

type capabilityPartStore struct {
	partstore.PartStore
	capabilities partstore.Capabilities
}

func (s capabilityPartStore) Capabilities() partstore.Capabilities { return s.capabilities }

func TestErasureCodingPartStoreReadsTxFreeOnlyIfItCanHealTxFree(t *testing.T) {
	testutils.SkipIfIntegration(t)

	readOnly := partstore.NewCapabilities(partstore.CapabilityTxFreeGetPart)
	readWrite := partstore.NewCapabilities(partstore.CapabilityTxFreeGetPart, partstore.CapabilityTxFreePutPart)
	for _, tc := range []struct {
		name     string
		shards   []partstore.Capabilities
		expected partstore.Capabilities
	}{
		{name: "tx-free reads and writes", shards: []partstore.Capabilities{readWrite, readWrite, readWrite}, expected: readWrite},
		{name: "tx-free reads only", shards: []partstore.Capabilities{readOnly, readOnly, readOnly}, expected: 0},
		{name: "one shard reads tx-free only", shards: []partstore.Capabilities{readWrite, readOnly, readWrite}, expected: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stores := make([]partstore.PartStore, len(tc.shards))
			for i, capabilities := range tc.shards {
				stores[i] = capabilityPartStore{capabilities: capabilities}
			}
			ec, err := NewWithPartStores(2, 1, 64*1024, stores, WithHealScanInterval(0))
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, partstore.CapabilitiesOf(ec))
		})
	}
}
