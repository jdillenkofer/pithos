package erasurecoding

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	ec, err := NewWithPartStores(2, 1, 64*1024, stores)
	assert.Nil(t, err)
	return ec, stores, db
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

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err := store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	out, err := io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Equal(t, data, out)
	assert.Nil(t, tx.Commit())
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

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	err = shardStores[0].DeletePart(ctx, tx, *partId)
	assert.Nil(t, err)
	assert.Nil(t, tx.Commit())

	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	rc, err := store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	out, err := io.ReadAll(rc)
	_ = rc.Close()
	assert.Nil(t, err)
	assert.Equal(t, data, out)
	assert.Nil(t, tx.Commit())
}
