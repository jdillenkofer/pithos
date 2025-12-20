package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestSqlPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()
	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartContentRepository: %s", err))
		os.Exit(1)
	}
	sqlPartStore, err := New(db, partContentRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}
	content := []byte("SqlPartStore")
	err = partstore.Tester(sqlPartStore, db, content)
	assert.Nil(t, err)
}

func TestSqlPartStore_Chunking(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db, partContentRepository)
	if err != nil {
		t.Fatal(err)
	}

	// Create data larger than chunkSize (64MB)
	// We'll use 64MB + 1MB = 65MB
	const size = 65 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	partId, _ := partstore.NewRandomPartId()
	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})

	// Put the large part
	err = store.PutPart(ctx, tx, *partId, bytes.NewReader(data))
	assert.Nil(t, err)
	tx.Commit()

	// Verify chunks in DB
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	chunks, err := partContentRepository.FindPartContentChunksById(ctx, tx, *partId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(chunks), "Expected 2 chunks for 65MB data with 64MB chunkSize")
	assert.Equal(t, 0, chunks[0].ChunkIndex)
	assert.Equal(t, 1, chunks[1].ChunkIndex)
	assert.Equal(t, 64*1024*1024, len(chunks[0].Content))
	assert.Equal(t, 1*1024*1024, len(chunks[1].Content))
	tx.Commit()

	// Get the part back and verify content
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	reader, err := store.GetPart(ctx, tx, *partId)
	assert.Nil(t, err)
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, data, retrievedData)
	tx.Commit()
}

