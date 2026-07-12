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

func newTapeStore(t *testing.T, tapePath, journalPath string) *tapePartStore {
	t.Helper()
	store, err := New(func(ctx context.Context) (tapedev.Device, error) {
		return simulator.Open(ctx, tapePath, simulator.Options{})
	}, WithRecordSize(testRecordSize), WithJournalDir(journalPath))
	require.NoError(t, err)
	return store.(*tapePartStore)
}

func newStartedTapeStore(t *testing.T, tapePath, journalPath string) *tapePartStore {
	t.Helper()
	store := newTapeStore(t, tapePath, journalPath)
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
	root := t.TempDir()
	store := newTapeStore(t, filepath.Join(root, "tape.sim"), filepath.Join(root, "journal"))
	content := []byte("TapePartStore content spanning multiple tape records")
	err := partstore.Tester(store, db, content)
	assert.Nil(t, err)
}

func TestTapePartStoreRecoversWithoutDatabase(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := []byte("part A content")
	contentB := []byte("part B content, a bit longer")

	require.NoError(t, store.PutPart(ctx, nil, *partA, partstore.PutPartOptions{}, bytes.NewReader(contentA)))
	require.NoError(t, store.PutPart(ctx, nil, *partB, partstore.PutPartOptions{}, bytes.NewReader(contentB)))
	store.runMigration(ctx, true)
	require.Equal(t, locationTape, store.index[*partA].location)
	require.Equal(t, locationTape, store.index[*partB].location)
	require.NoError(t, store.DeletePart(ctx, nil, *partA))
	require.NoError(t, store.Stop(ctx))

	restarted := newStartedTapeStore(t, tapePath, journalPath)
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

func TestTapePartStoreDeleteIsDurable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, partstore.PutPartOptions{}, bytes.NewReader([]byte("some part content"))))

	require.NoError(t, store.DeletePart(ctx, nil, *partId))
	require.NoError(t, store.Stop(ctx))

	restarted := newStartedTapeStore(t, tapePath, journalPath)
	defer restarted.Stop(ctx)
	partIds, err := restarted.GetPartIds(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, partIds)
}

func TestTapePartStoreRollbackDoesNotPublishStagedChanges(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	db := openTestDb(t)
	store := newStartedTapeStore(t, tapePath, journalPath)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, *partId, partstore.PutPartOptions{}, bytes.NewReader([]byte("new")))
	})
	require.NoError(t, err)

	errRollback := fmt.Errorf("rollback")
	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := store.PutPart(ctx, tx, *partId, partstore.PutPartOptions{}, bytes.NewReader([]byte("staged"))); err != nil {
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

	// The compensating activation must restore the previous generation across
	// restart; the rolled-back overwrite must never become visible.
	require.NoError(t, store.Stop(ctx))
	restarted := newStartedTapeStore(t, tapePath, journalPath)
	defer restarted.Stop(ctx)

	content, err = readPart(t, restarted, *partId)
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), content)
}

func TestTapePartStoreOverwriteKeepsNewestAcrossRestart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, partstore.PutPartOptions{}, bytes.NewReader([]byte("version 1"))))
	require.NoError(t, store.PutPart(ctx, nil, *partId, partstore.PutPartOptions{}, bytes.NewReader([]byte("version 2"))))

	content, err := readPart(t, store, *partId)
	require.NoError(t, err)
	require.Equal(t, []byte("version 2"), content)

	require.NoError(t, store.Stop(ctx))
	restarted := newStartedTapeStore(t, tapePath, journalPath)
	defer restarted.Stop(ctx)

	content, err = readPart(t, restarted, *partId)
	require.NoError(t, err)
	require.Equal(t, []byte("version 2"), content)
}

func TestTapePartStoreInterleavedReaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	store := newStartedTapeStore(t, filepath.Join(root, "tape.sim"), filepath.Join(root, "journal"))
	defer store.Stop(ctx)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := bytes.Repeat([]byte("a"), 5*testRecordSize+3)
	contentB := bytes.Repeat([]byte("b"), 4*testRecordSize+1)

	require.NoError(t, store.PutPart(ctx, nil, *partA, partstore.PutPartOptions{}, bytes.NewReader(contentA)))
	require.NoError(t, store.PutPart(ctx, nil, *partB, partstore.PutPartOptions{}, bytes.NewReader(contentB)))

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
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partA, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentA := []byte("committed part content")
	require.NoError(t, store.PutPart(ctx, nil, *partA, partstore.PutPartOptions{}, bytes.NewReader(contentA)))
	store.runMigration(ctx, true)
	require.Equal(t, locationTape, store.index[*partA].location)
	require.NoError(t, store.Stop(ctx))

	// Simulate a crash mid-PutPart: a data segment without its terminating
	// filemark, appended directly on the device.
	device, err := simulator.Open(ctx, tapePath, simulator.Options{})
	require.NoError(t, err)
	require.NoError(t, device.SeekToEOD(ctx))
	tornRecord, err := encodeSegmentRecord(segKindHeader, 999, []byte("truncated v2 header"))
	require.NoError(t, err)
	require.NoError(t, device.WriteRecord(ctx, tornRecord))
	require.NoError(t, device.Close())

	restarted := newStartedTapeStore(t, tapePath, journalPath)

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
	require.NoError(t, restarted.PutPart(ctx, nil, *partB, partstore.PutPartOptions{}, bytes.NewReader(contentB)))
	restarted.runMigration(ctx, true)
	require.NoError(t, restarted.Stop(ctx))

	final := newStartedTapeStore(t, tapePath, journalPath)
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
	root := t.TempDir()
	store := newStartedTapeStore(t, filepath.Join(root, "tape.sim"), filepath.Join(root, "journal"))
	defer store.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partId, partstore.PutPartOptions{}, bytes.NewReader(nil)))

	content, err := readPart(t, store, *partId)
	require.NoError(t, err)
	require.Empty(t, content)
}

func TestTapePartStoreStagesTapeReadsOnDisk(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, filepath.Join(root, "tape.sim"), journalPath)
	defer store.Stop(ctx)

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := bytes.Repeat([]byte("cache me"), 128)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)))
	store.runMigration(ctx, true)
	require.Equal(t, locationTape, store.index[*partID].location)

	got, err := readPart(t, store, *partID)
	require.NoError(t, err)
	require.Equal(t, content, got)

	entries, err := os.ReadDir(filepath.Join(journalPath, "read-cache"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
