package tape

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	tapedev "github.com/jdillenkofer/pithos/internal/tape"
	"github.com/jdillenkofer/pithos/internal/tape/simulator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

// testRecordSize is tiny so even short test content spans multiple records.
const testRecordSize = 8

func newTapeStore(t *testing.T, tapePath string) partstore.PartStore {
	t.Helper()
	store, err := New(func(ctx context.Context) (tapedev.Device, error) {
		return simulator.Open(ctx, tapePath, simulator.Options{})
	}, WithRecordSize(testRecordSize))
	require.NoError(t, err)
	return store
}

func newStartedTapeStore(t *testing.T, tapePath string) partstore.PartStore {
	t.Helper()
	store := newTapeStore(t, tapePath)
	require.NoError(t, store.Start(context.Background()))
	return store
}

func openTestDb(t *testing.T) database.Database {
	t.Helper()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func readPart(t *testing.T, store partstore.PartStore, partId partstore.PartId) ([]byte, error) {
	t.Helper()
	rc, err := store.GetPart(context.Background(), nil, partId)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func TestTapePartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)

	db := openTestDb(t)
	store := newTapeStore(t, filepath.Join(t.TempDir(), "tape.sim"))
	content := []byte("TapePartStore content spanning multiple tape records")
	err := partstore.Tester(store, db, content)
	assert.Nil(t, err)
}

func TestTapePartStoreRebuildAfterRestart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	store := newStartedTapeStore(t, tapePath)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := []byte("part A content")
	contentB := []byte("part B content, a bit longer")

	require.NoError(t, store.PutPart(ctx, nil, *partA, bytes.NewReader(contentA)))
	require.NoError(t, store.PutPart(ctx, nil, *partB, bytes.NewReader(contentB)))
	require.NoError(t, store.DeletePart(ctx, nil, *partA))
	require.NoError(t, store.Stop(ctx))

	restarted := newStartedTapeStore(t, tapePath)
	defer restarted.Stop(ctx)

	partIds, err := restarted.GetPartIds(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []partstore.PartId{*partB}, partIds)

	content, err := readPart(t, restarted, *partB)
	require.NoError(t, err)
	require.Equal(t, contentB, content)

	_, err = readPart(t, restarted, *partA)
	require.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestTapePartStoreDeleteOnlyAppends(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	store := newStartedTapeStore(t, tapePath)
	defer store.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, bytes.NewReader([]byte("some part content"))))

	infoBefore, err := os.Stat(tapePath)
	require.NoError(t, err)

	require.NoError(t, store.DeletePart(ctx, nil, *partId))

	infoAfter, err := os.Stat(tapePath)
	require.NoError(t, err)
	require.Greater(t, infoAfter.Size(), infoBefore.Size(), "delete must only append a tombstone")

	partIds, err := store.GetPartIds(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, partIds)
}

func TestTapePartStoreRollbackDoesNotPublishStagedChanges(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	db := openTestDb(t)
	store := newStartedTapeStore(t, tapePath)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("new")))
	})
	require.NoError(t, err)

	errRollback := fmt.Errorf("rollback")
	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.PutPart(ctx, tx, *partId, bytes.NewReader([]byte("staged"))); err != nil {
			return err
		}
		return errRollback
	})
	assert.ErrorIs(t, err, errRollback)

	content, err := readPart(t, store, *partId)
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), content)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.DeletePart(ctx, tx, *partId); err != nil {
			return err
		}
		return errRollback
	})
	assert.ErrorIs(t, err, errRollback)

	content, err = readPart(t, store, *partId)
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), content)

	// The rolled-back overwrite must stay invisible across a restart: its
	// tombstone only invalidates the rolled-back copy, not the committed one.
	require.NoError(t, store.Stop(ctx))
	restarted := newStartedTapeStore(t, tapePath)
	defer restarted.Stop(ctx)

	content, err = readPart(t, restarted, *partId)
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), content)
}

func TestTapePartStoreOverwriteKeepsNewestAcrossRestart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	store := newStartedTapeStore(t, tapePath)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, bytes.NewReader([]byte("version 1"))))
	require.NoError(t, store.PutPart(ctx, nil, *partId, bytes.NewReader([]byte("version 2"))))

	content, err := readPart(t, store, *partId)
	require.NoError(t, err)
	require.Equal(t, []byte("version 2"), content)

	require.NoError(t, store.Stop(ctx))
	restarted := newStartedTapeStore(t, tapePath)
	defer restarted.Stop(ctx)

	content, err = readPart(t, restarted, *partId)
	require.NoError(t, err)
	require.Equal(t, []byte("version 2"), content)
}

func TestTapePartStoreInterleavedReaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	store := newStartedTapeStore(t, filepath.Join(t.TempDir(), "tape.sim"))
	defer store.Stop(ctx)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := bytes.Repeat([]byte("a"), 5*testRecordSize+3)
	contentB := bytes.Repeat([]byte("b"), 4*testRecordSize+1)

	require.NoError(t, store.PutPart(ctx, nil, *partA, bytes.NewReader(contentA)))
	require.NoError(t, store.PutPart(ctx, nil, *partB, bytes.NewReader(contentB)))

	readerA, err := store.GetPart(ctx, nil, *partA)
	require.NoError(t, err)
	defer readerA.Close()
	readerB, err := store.GetPart(ctx, nil, *partB)
	require.NoError(t, err)
	defer readerB.Close()

	// Alternate small reads on one goroutine: both readers force the head
	// back and forth, and neither may block the other.
	var gotA, gotB bytes.Buffer
	doneA, doneB := false, false
	buf := make([]byte, 3)
	for !doneA || !doneB {
		if !doneA {
			n, err := readerA.Read(buf)
			gotA.Write(buf[:n])
			if err == io.EOF {
				doneA = true
			} else {
				require.NoError(t, err)
			}
		}
		if !doneB {
			n, err := readerB.Read(buf)
			gotB.Write(buf[:n])
			if err == io.EOF {
				doneB = true
			} else {
				require.NoError(t, err)
			}
		}
	}
	require.Equal(t, contentA, gotA.Bytes())
	require.Equal(t, contentB, gotB.Bytes())
}

func TestTapePartStoreTruncatedTailIsSealedOnStart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	store := newStartedTapeStore(t, tapePath)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := []byte("committed part content")
	require.NoError(t, store.PutPart(ctx, nil, *partA, bytes.NewReader(contentA)))
	require.NoError(t, store.Stop(ctx))

	// Simulate a crash mid-PutPart: a data segment without its terminating
	// filemark, appended directly on the device.
	truncatedId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	device, err := simulator.Open(ctx, tapePath, simulator.Options{})
	require.NoError(t, err)
	require.NoError(t, device.SeekToEOD(ctx))
	require.NoError(t, device.WriteRecord(ctx, encodeDataHeader(*truncatedId)))
	require.NoError(t, device.WriteRecord(ctx, []byte("partial ")))
	require.NoError(t, device.Close())

	restarted := newStartedTapeStore(t, tapePath)

	partIds, err := restarted.GetPartIds(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []partstore.PartId{*partA}, partIds)

	content, err := readPart(t, restarted, *partA)
	require.NoError(t, err)
	require.Equal(t, contentA, content)

	// The sealed tape stays fully usable and consistent across another
	// restart.
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentB := []byte("appended after sealing")
	require.NoError(t, restarted.PutPart(ctx, nil, *partB, bytes.NewReader(contentB)))
	require.NoError(t, restarted.Stop(ctx))

	final := newStartedTapeStore(t, tapePath)
	defer final.Stop(ctx)

	partIds, err = final.GetPartIds(ctx, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []partstore.PartId{*partA, *partB}, partIds)
	content, err = readPart(t, final, *partB)
	require.NoError(t, err)
	require.Equal(t, contentB, content)
}

func TestTapePartStoreEmptyPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	store := newStartedTapeStore(t, filepath.Join(t.TempDir(), "tape.sim"))
	defer store.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, bytes.NewReader(nil)))

	content, err := readPart(t, store, *partId)
	require.NoError(t, err)
	require.Empty(t, content)
}
