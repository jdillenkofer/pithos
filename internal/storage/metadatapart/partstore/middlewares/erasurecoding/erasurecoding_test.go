package erasurecoding

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func createTestStore(t *testing.T, shardCount int) (partstore.PartStore, []partstore.PartStore, database.Database) {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	assert.Nil(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(storagePath) })

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	assert.Nil(t, err)
	t.Cleanup(func() { _ = db.Close() })

	stores := make([]partstore.PartStore, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		root := filepath.Join(storagePath, "parts", string(rune('a'+i)))
		s, err := filesystem.New(root)
		assert.Nil(t, err)
		stores = append(stores, s)
	}
	// Disable background healing here because these tests delete shard files directly,
	// bypassing middleware locks and otherwise introducing timing-dependent races.
	ec, err := NewWithPartStores(2, 1, 64*1024, stores, WithHealScanInterval(0))
	assert.Nil(t, err)
	return ec, stores, db
}

func deleteShardPartWithRetry(t *testing.T, ctx context.Context, db database.Database, shardStore partstore.PartStore, partId partstore.PartId) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
		err := shardStore.DeletePart(ctx, tx, partId)
		if err == nil {
			assert.Nil(t, tx.Commit())
			return
		}
		_ = tx.Rollback()

		if runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(32)) && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		assert.Nil(t, err)
		return
	}
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

	// Background healing reads shards concurrently; on Windows this can briefly
	// block direct file deletion, so retry to keep this test deterministic.
	deleteShardPartWithRetry(t, ctx, db, shardStores[0], *partId)

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

	// Same rationale as above: direct shard deletion races active heal reads.
	deleteShardPartWithRetry(t, ctx, db, shardStores[1], *partId)

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

	shardStores := make([]partstore.PartStore, 0, 3)
	for i := 0; i < 3; i++ {
		root := filepath.Join(storagePath, "parts", string(rune('a'+i)))
		s, err := filesystem.New(root)
		assert.Nil(t, err)
		shardStores = append(shardStores, s)
	}
	store, err := NewWithPartStores(2, 1, 64*1024, shardStores, WithHealScanInterval(30*time.Millisecond))
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

	deleteShardPartWithRetry(t, ctx, db, shardStores[0], *partId)

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

	deleteShardPartWithRetry(t, ctx, db, shardStores[1], *partId)

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
