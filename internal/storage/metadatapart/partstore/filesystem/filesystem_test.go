package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestFilesystemPartStoreCanConvertFilenameAndPartId(t *testing.T) {
	testutils.SkipIfIntegration(t)

	filesystemPartStore := filesystemPartStore{root: "."}
	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatalf("Failed to generate PartId: %v", err)
	}
	filename := filesystemPartStore.getFilename(*partId)
	partId2, ok := filesystemPartStore.tryGetPartIdFromFilename(filename)
	assert.True(t, ok)
	assert.Equal(t, *partId, *partId2)
}

func TestFilesystemPartStore(t *testing.T) {
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
	filesystemPartStore, err := New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}
	content := []byte("FilesystemPartStore")
	err = partstore.Tester(filesystemPartStore, db, content)
	assert.Nil(t, err)
}

func TestFilesystemPartStoreRollbackDoesNotPublishStagedChanges(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	storagePath := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ps, err := New(storagePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatal(err)
	}

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		return ps.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("new")))
	})
	if err != nil {
		t.Fatal(err)
	}

	errRollback := fmt.Errorf("rollback")
	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := ps.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("staged"))); err != nil {
			return err
		}
		return errRollback
	})
	assert.ErrorIs(t, err, errRollback)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		rc, err := ps.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		defer rc.Close()
		content, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		assert.Equal(t, []byte("new"), content)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := ps.DeletePart(ctx, tx, *partId); err != nil {
			return err
		}
		return errRollback
	})
	assert.ErrorIs(t, err, errRollback)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		rc, err := ps.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		_ = rc.Close()
		return nil
	})
	assert.NoError(t, err)
}
