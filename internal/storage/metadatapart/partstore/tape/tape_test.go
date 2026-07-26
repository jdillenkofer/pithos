package tape

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type observedTapeDevice struct {
	tapedev.Device
	readDelay   time.Duration
	locateCalls atomic.Int64
	readCalls   atomic.Int64
}

func (d *observedTapeDevice) LocateBlock(ctx context.Context, block uint64) error {
	d.locateCalls.Add(1)
	return d.Device.LocateBlock(ctx, block)
}

func (d *observedTapeDevice) ReadRecord(ctx context.Context, p []byte) (int, error) {
	if d.readDelay > 0 {
		timer := time.NewTimer(d.readDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		}
	}
	d.readCalls.Add(1)
	return d.Device.ReadRecord(ctx, p)
}

type corruptManifestDevice struct {
	tapedev.Device
	corrupted bool
}

func (d *corruptManifestDevice) ReadRecord(ctx context.Context, p []byte) (int, error) {
	n, err := d.Device.ReadRecord(ctx, p)
	if err != nil || d.corrupted {
		return n, err
	}
	record, decodeErr := decodeSegmentRecord(p[:n])
	if decodeErr == nil && record.kind == segKindIndexChunk {
		p[n-1] ^= 0xff
		d.corrupted = true
	}
	return n, err
}

type filemarkFailingDevice struct {
	tapedev.Device
	err error
}

func (d *filemarkFailingDevice) WriteFilemarks(context.Context, int) error {
	return d.err
}

type rewindFailingDevice struct {
	tapedev.Device
	err error
}

func (d *rewindFailingDevice) Rewind(context.Context) error {
	return d.err
}

type hookTx struct {
	preCommit   []func(context.Context) error
	afterCommit []func(context.Context) error
	rollback    []func(context.Context) error
}

func (tx *hookTx) SqlTx() *sql.Tx { return nil }
func (tx *hookTx) DBHandle() any  { return nil }

func (tx *hookTx) OnPreCommit(fn func(context.Context) error) {
	tx.preCommit = append(tx.preCommit, fn)
}

func (tx *hookTx) OnAfterCommit(fn func(context.Context) error) {
	tx.afterCommit = append(tx.afterCommit, fn)
}

func (tx *hookTx) OnRollback(fn func(context.Context) error) {
	tx.rollback = append(tx.rollback, fn)
}

func (tx *hookTx) commit(ctx context.Context) error {
	for _, fn := range tx.preCommit {
		if err := fn(ctx); err != nil {
			for _, rollback := range tx.rollback {
				_ = rollback(ctx)
			}
			return err
		}
	}
	for _, fn := range tx.afterCommit {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	return nil
}

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

func TestTapePartStoreStartCanRetryAfterInitializationFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")

	// Initialize the cartridge so the retry only needs to reopen it.
	initial := newStartedTapeStore(t, tapePath, journalPath)
	require.NoError(t, initial.Stop(ctx))

	startErr := errors.New("rewind failed")
	var opens atomic.Int64
	storeValue, err := New(func(ctx context.Context) (tapedev.Device, error) {
		device, err := simulator.Open(ctx, tapePath, simulator.Options{})
		if err != nil {
			return nil, err
		}
		if opens.Add(1) == 1 {
			return &rewindFailingDevice{Device: device, err: startErr}, nil
		}
		return device, nil
	}, WithRecordSize(testRecordSize), WithJournalDir(journalPath))
	require.NoError(t, err)
	store := storeValue.(*tapePartStore)

	require.ErrorIs(t, store.Start(ctx), startErr)
	require.Nil(t, store.device)
	require.Nil(t, store.journal)
	require.Error(t, store.checkStarted())

	require.NoError(t, store.Start(ctx))
	require.NoError(t, store.Stop(ctx))
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

func TestTapePartStoreSerializesInterleavedOverwrites(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("initial"))))

	first := &hookTx{}
	second := &hookTx{}
	require.NoError(t, store.PutPart(ctx, first, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("first"))))
	require.NoError(t, store.PutPart(ctx, second, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("second"))))

	require.NoError(t, first.commit(ctx))
	require.NoError(t, second.commit(ctx))
	content, err := readPart(t, store, *partID)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), content)

	require.NoError(t, store.Stop(ctx))
	restarted := newStartedTapeStore(t, tapePath, journalPath)
	defer restarted.Stop(ctx)

	content, err = readPart(t, restarted, *partID)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), content)
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
	previousSegment := store.catalog.PreviousSegment
	nextSequence := store.catalog.Segments[len(store.catalog.Segments)-1].NextSequence
	require.NoError(t, store.Stop(ctx))

	// Simulate a crash after a valid next-segment header but before its data
	// filemark and manifest.
	device, err := simulator.Open(ctx, tapePath, simulator.Options{})
	require.NoError(t, err)
	require.NoError(t, device.SeekToEOD(ctx))
	tornRecord, err := encodeSegmentRecord(segKindHeader, nextSequence, encodeSegmentHeader(segmentHeaderPayload{
		segmentID:       [16]byte{0x99},
		previousSegment: previousSegment,
		sequenceStart:   nextSequence,
	}))
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

func TestTapePartStoreFailsStartWhenTruncatedTailCannotBeSealed(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("committed"))))
	store.runMigration(ctx, true)
	previousSegment := store.catalog.PreviousSegment
	nextSequence := store.catalog.Segments[len(store.catalog.Segments)-1].NextSequence
	require.NoError(t, store.Stop(ctx))

	device, err := simulator.Open(ctx, tapePath, simulator.Options{})
	require.NoError(t, err)
	require.NoError(t, device.SeekToEOD(ctx))
	tornRecord, err := encodeSegmentRecord(segKindHeader, nextSequence, encodeSegmentHeader(segmentHeaderPayload{
		segmentID:       [16]byte{0x99},
		previousSegment: previousSegment,
		sequenceStart:   nextSequence,
	}))
	require.NoError(t, err)
	require.NoError(t, device.WriteRecord(ctx, tornRecord))
	require.NoError(t, device.Close())

	sealErr := errors.New("injected filemark failure")
	restarted, err := New(func(ctx context.Context) (tapedev.Device, error) {
		device, err := simulator.Open(ctx, tapePath, simulator.Options{})
		if err != nil {
			return nil, err
		}
		return &filemarkFailingDevice{Device: device, err: sealErr}, nil
	}, WithRecordSize(testRecordSize), WithJournalDir(journalPath))
	require.NoError(t, err)
	require.ErrorIs(t, restarted.Start(ctx), sealErr)
}

func TestTapeCatalogCorruptionRebuildsFromCompactManifests(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)
	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := bytes.Repeat([]byte("catalog recovery"), 100)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)))
	store.runMigration(ctx, true)
	require.NoError(t, store.Stop(ctx))

	require.NoError(t, os.WriteFile(filepath.Join(journalPath, catalogFileName), []byte("corrupt catalog"), 0o600))
	restarted := newStartedTapeStore(t, tapePath, journalPath)
	defer restarted.Stop(ctx)
	got, err := readPart(t, restarted, *partID)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestTapeCatalogRejectsDifferentPhysicalMedia(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)
	require.NoError(t, store.Stop(ctx))
	require.NoError(t, os.Rename(tapePath, filepath.Join(root, "original-tape.sim")))

	restarted := newTapeStore(t, tapePath, journalPath)
	err := restarted.Start(ctx)
	require.ErrorIs(t, err, ErrWrongTapeMedia)
}

func TestTapeManifestCorruptionFailsClosedWithoutSealing(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)
	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("payload"))))
	store.runMigration(ctx, true)
	require.NoError(t, store.Stop(ctx))
	require.NoError(t, os.Remove(filepath.Join(journalPath, catalogFileName)))
	before, err := os.Stat(tapePath)
	require.NoError(t, err)

	restartedStore, err := New(func(ctx context.Context) (tapedev.Device, error) {
		device, err := simulator.Open(ctx, tapePath, simulator.Options{})
		if err != nil {
			return nil, err
		}
		return &corruptManifestDevice{Device: device}, nil
	}, WithRecordSize(testRecordSize), WithJournalDir(journalPath))
	require.NoError(t, err)
	err = restartedStore.Start(ctx)
	require.ErrorIs(t, err, ErrCorruptTape)
	after, statErr := os.Stat(tapePath)
	require.NoError(t, statErr)
	require.Equal(t, before.Size(), after.Size())
}

func TestTapeDeleteSurvivesWithoutJournalHistory(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	store := newStartedTapeStore(t, tapePath, journalPath)
	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader([]byte("delete me"))))
	store.runMigration(ctx, true)
	require.NoError(t, store.DeletePart(ctx, nil, *partID))
	store.runMigration(ctx, true) // metadata-only segment persists the tombstone
	require.NoError(t, store.Stop(ctx))

	entries, err := os.ReadDir(journalPath)
	require.NoError(t, err)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == journalFileExtensionForTest {
			require.NoError(t, os.Remove(filepath.Join(journalPath, entry.Name())))
		}
	}
	require.NoError(t, os.Remove(filepath.Join(journalPath, catalogFileName)))
	restarted := newStartedTapeStore(t, tapePath, journalPath)
	_, err = restarted.GetPart(ctx, nil, *partID)
	require.ErrorIs(t, err, partstore.ErrPartNotFound)

	newPart, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, restarted.PutPart(ctx, nil, *newPart, partstore.PutPartOptions{}, bytes.NewReader([]byte("after journal rebuild"))))
	restarted.runMigration(ctx, true)
	require.NoError(t, restarted.Stop(ctx))

	again := newStartedTapeStore(t, tapePath, journalPath)
	defer again.Stop(ctx)
	_, err = again.GetPart(ctx, nil, *partID)
	require.ErrorIs(t, err, partstore.ErrPartNotFound)
	got, err := readPart(t, again, *newPart)
	require.NoError(t, err)
	require.Equal(t, []byte("after journal rebuild"), got)
}

const journalFileExtensionForTest = ".pj"

func TestTapeEndOfMediaKeepsJournalCopyReadable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "small-tape.sim")
	journalPath := filepath.Join(root, "journal")
	storeValue, err := New(func(ctx context.Context) (tapedev.Device, error) {
		return simulator.Open(ctx, tapePath, simulator.Options{Capacity: 4 << 10})
	}, WithRecordSize(1024), WithJournalDir(journalPath))
	require.NoError(t, err)
	store := storeValue.(*tapePartStore)
	require.NoError(t, store.Start(ctx))

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := bytes.Repeat([]byte("eot-safe"), 2048)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)))

	store.runMigration(ctx, true)
	status, err := store.OperationalStatus()
	require.NoError(t, err)
	require.True(t, status.EndOfMedia)
	require.Contains(t, status.LastMigrationError, tapedev.ErrEndOfTape.Error())
	require.Equal(t, 1, status.JournalBacklogParts)
	require.Equal(t, uint64(len(content)), status.JournalBacklogBytes)
	got, err := readPart(t, store, *partID)
	require.NoError(t, err)
	require.Equal(t, content, got)
	require.NoError(t, store.Stop(ctx))

	restartedValue, err := New(func(ctx context.Context) (tapedev.Device, error) {
		return simulator.Open(ctx, tapePath, simulator.Options{})
	}, WithRecordSize(1024), WithJournalDir(journalPath))
	require.NoError(t, err)
	restarted := restartedValue.(*tapePartStore)
	require.NoError(t, restarted.Start(ctx))
	defer restarted.Stop(ctx)
	got, err = readPart(t, restarted, *partID)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestTapeReadCacheSingleflightValidationAndBound(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	tapePath := filepath.Join(root, "tape.sim")
	journalPath := filepath.Join(root, "journal")
	var observed *observedTapeDevice
	storeValue, err := New(func(ctx context.Context) (tapedev.Device, error) {
		device, err := simulator.Open(ctx, tapePath, simulator.Options{})
		if err != nil {
			return nil, err
		}
		observed = &observedTapeDevice{Device: device, readDelay: time.Millisecond}
		return observed, nil
	}, WithRecordSize(1024), WithJournalDir(journalPath), WithReadCacheMaxBytes(30<<10))
	require.NoError(t, err)
	store := storeValue.(*tapePartStore)
	require.NoError(t, store.Start(ctx))
	defer store.Stop(ctx)

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := bytes.Repeat([]byte("cache-data"), 2048)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)))
	store.runMigration(ctx, true)
	beforeLocates := observed.locateCalls.Load()

	const readers = 12
	start := make(chan struct{})
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := readPart(t, store, *partID)
			if err == nil && !bytes.Equal(got, content) {
				err = fmt.Errorf("cache returned wrong content")
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), observed.locateCalls.Load()-beforeLocates)

	entry := store.index[*partID]
	cachePath := filepath.Join(store.cacheDir, entry.generation.String()+".part")
	corrupt := bytes.Repeat([]byte{0xff}, len(content))
	require.NoError(t, os.WriteFile(cachePath, corrupt, 0o600))
	future := time.Now().Add(time.Second)
	require.NoError(t, os.Chtimes(cachePath, future, future))
	beforeLocates = observed.locateCalls.Load()
	got, err := readPart(t, store, *partID)
	require.NoError(t, err)
	require.Equal(t, content, got)
	require.Equal(t, int64(1), observed.locateCalls.Load()-beforeLocates)

	// An active reader pins its cache entry. A second recall may temporarily
	// exceed quota, then closing the pinned reader immediately evicts to bound.
	pinned, err := store.GetPart(ctx, nil, *partID)
	require.NoError(t, err)
	partB, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	contentB := bytes.Repeat([]byte("other-data"), 2048)
	require.NoError(t, store.PutPart(ctx, nil, *partB, partstore.PutPartOptions{}, bytes.NewReader(contentB)))
	store.runMigration(ctx, true)
	got, err = readPart(t, store, *partB)
	require.NoError(t, err)
	require.Equal(t, contentB, got)
	require.NoError(t, pinned.Close())
	store.mu.Lock()
	cacheBytes := store.cacheBytes
	store.mu.Unlock()
	require.LessOrEqual(t, cacheBytes, store.cacheMaxBytes)
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

func TestTapePartStoreRetriesStaleJournalLocatorFromTape(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	root := t.TempDir()
	store := newStartedTapeStore(t, filepath.Join(root, "tape.sim"), filepath.Join(root, "journal"))
	defer store.Stop(ctx)

	partID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := bytes.Repeat([]byte("survives journal compaction"), 64)
	require.NoError(t, store.PutPart(ctx, nil, *partID, partstore.PutPartOptions{}, bytes.NewReader(content)))
	stale := store.index[*partID]

	store.runMigration(ctx, true)
	require.Equal(t, locationTape, store.index[*partID].location)

	// Model a GetPart that copied the journal locator just before migration
	// published the tape location, with compaction removing the old file before
	// OpenPayload. A nonexistent file makes that interleaving deterministic.
	stale.journalLoc.FileIndex = ^uint64(0)
	reader, err := store.openPartAtEntry(ctx, *partID, stale, store.journal)
	require.NoError(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)
}
