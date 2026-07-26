package journal

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func mustPartID(t *testing.T) partstore.PartId {
	t.Helper()
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	return *id
}

func mustGen(t *testing.T) GenerationID {
	t.Helper()
	g, err := NewGenerationID()
	require.NoError(t, err)
	return g
}

func writePart(t *testing.T, j *Journal, partID partstore.PartId, gen GenerationID, data []byte) Locator {
	t.Helper()
	loc, err := j.WritePart(context.Background(), PartInput{Generation: gen, PartID: partID}, bytes.NewReader(data))
	require.NoError(t, err)
	return loc
}

func readPayload(t *testing.T, j *Journal, loc Locator) []byte {
	t.Helper()
	rc, err := j.OpenPayload(loc)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	return got
}

func TestWriteActivateReadRoundtrip(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	gen := mustGen(t)
	data := bytes.Repeat([]byte("payload-"), 5000)
	loc := writePart(t, j, partID, gen, data)
	_, err = j.Activate(ctx, partID, gen, nil)
	require.NoError(t, err)

	require.Equal(t, data, readPayload(t, j, loc))
	require.NoError(t, j.Close())

	// Recover from disk with no in-memory state.
	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Len(t, scan.Live, 1)
	liveGen, ok := scan.Live[partID]
	require.True(t, ok)
	require.Equal(t, gen, liveGen)

	part := scan.Parts[gen]
	require.NotNil(t, part)
	require.Equal(t, uint64(len(data)), part.Length)

	// A fresh journal can serve the recovered payload.
	j2, err := Open(Options{Dir: dir})
	require.NoError(t, err)
	defer j2.Close()
	require.Equal(t, data, readPayload(t, j2, part.Location))
}

func TestEmptyPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)
	defer j.Close()

	partID := mustPartID(t)
	gen := mustGen(t)
	loc := writePart(t, j, partID, gen, nil)
	_, err = j.Activate(ctx, partID, gen, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), loc.Length)
	require.Empty(t, readPayload(t, j, loc))
}

func TestOverwriteSupersedesGenerationAndRejectsStale(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	genA := mustGen(t)
	genB := mustGen(t)
	writePart(t, j, partID, genA, []byte("A"))
	_, err = j.Activate(ctx, partID, genA, nil)
	require.NoError(t, err)

	writePart(t, j, partID, genB, []byte("B"))
	_, err = j.Activate(ctx, partID, genB, &genA)
	require.NoError(t, err)

	// A stale activation expecting genA must not resurrect it over genB.
	staleGen := mustGen(t)
	writePart(t, j, partID, staleGen, []byte("stale"))
	_, err = j.Activate(ctx, partID, staleGen, &genA)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Equal(t, genB, scan.Live[partID])
}

func TestDeleteWithExpectedGeneration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	genA := mustGen(t)
	writePart(t, j, partID, genA, []byte("A"))
	_, err = j.Activate(ctx, partID, genA, nil)
	require.NoError(t, err)

	// A delete for the wrong generation is ignored.
	otherGen := mustGen(t)
	_, err = j.Delete(ctx, partID, &otherGen)
	require.NoError(t, err)

	_, err = j.Delete(ctx, partID, &genA)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	scan, err := Scan(dir)
	require.NoError(t, err)
	_, live := scan.Live[partID]
	require.False(t, live)
}

func TestDeleteDoesNotEraseNewerRewrite(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	genA := mustGen(t)
	genB := mustGen(t)
	writePart(t, j, partID, genA, []byte("A"))
	_, err = j.Activate(ctx, partID, genA, nil)
	require.NoError(t, err)
	writePart(t, j, partID, genB, []byte("B"))
	_, err = j.Activate(ctx, partID, genB, &genA)
	require.NoError(t, err)

	// A delayed delete targeting the old generation must not remove genB.
	_, err = j.Delete(ctx, partID, &genA)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Equal(t, genB, scan.Live[partID])
}

func TestRecoveryIgnoresTornTail(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	gen := mustGen(t)
	writePart(t, j, partID, gen, []byte("durable"))
	_, err = j.Activate(ctx, partID, gen, nil)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	// Append a torn record (valid-looking start, truncated body) to the file.
	files, err := sortedFileIndices(dir)
	require.NoError(t, err)
	path := filepath.Join(dir, fileName(files[len(files)-1]))
	torn, err := encodeRecord(kindPartData, 0, 999999, bytes.Repeat([]byte("x"), 4096))
	require.NoError(t, err)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write(torn[:len(torn)-100]) // cut off the tail
	require.NoError(t, err)
	require.NoError(t, f.Close())

	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Equal(t, gen, scan.Live[partID])
	require.Len(t, scan.Parts, 1)
}

func TestRecoveryTruncationAtEveryLength(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partA := mustPartID(t)
	genA := mustGen(t)
	writePart(t, j, partA, genA, []byte("first"))
	_, err = j.Activate(ctx, partA, genA, nil)
	require.NoError(t, err)

	// Capture the file length after part A is fully durable and activated.
	files, err := sortedFileIndices(dir)
	require.NoError(t, err)
	path := filepath.Join(dir, fileName(files[0]))
	afterA, err := os.Stat(path)
	require.NoError(t, err)
	lenAfterA := afterA.Size()

	partB := mustPartID(t)
	genB := mustGen(t)
	writePart(t, j, partB, genB, []byte("second"))
	_, err = j.Activate(ctx, partB, genB, nil)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	full, err := os.ReadFile(path)
	require.NoError(t, err)

	// Truncating anywhere within part B's records must keep part A intact and
	// never surface a half-written part B.
	for cut := lenAfterA; cut <= int64(len(full)); cut++ {
		require.NoError(t, os.WriteFile(path, full[:cut], 0o644))
		scan, err := Scan(dir)
		require.NoError(t, err)
		require.Equal(t, genA, scan.Live[partA], "cut=%d", cut)
		if _, ok := scan.Parts[genB]; ok {
			// If part B's payload survived, its activation may or may not have;
			// either way part A must remain live.
			require.Contains(t, []int{1, 2}, len(scan.Parts))
		}
	}
}

func TestGroupCommitConcurrent(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{
		Dir:         dir,
		Durability:  DurabilityGroupCommit,
		GroupCommit: GroupCommitPolicy{MaxDelay: 2_000_000}, // 2ms
	})
	require.NoError(t, err)

	const writers = 16
	type result struct {
		partID partstore.PartId
		gen    GenerationID
		data   []byte
	}
	results := make([]result, writers)
	var wg sync.WaitGroup
	for i := range writers {
		results[i] = result{partID: mustPartID(t), gen: mustGen(t), data: bytes.Repeat([]byte{byte('a' + i)}, 1000+i)}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := results[i]
			_, err := j.WritePart(ctx, PartInput{Generation: r.gen, PartID: r.partID}, bytes.NewReader(r.data))
			require.NoError(t, err)
			_, err = j.Activate(ctx, r.partID, r.gen, nil)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	require.NoError(t, j.Close())

	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Len(t, scan.Live, writers)
	for _, r := range results {
		require.Equal(t, r.gen, scan.Live[r.partID])
		part := scan.Parts[r.gen]
		require.NotNil(t, part)
		require.Equal(t, uint64(len(r.data)), part.Length)
	}
}

func TestGroupCommitMaxBytesWakesWithoutMaxDelay(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dir := t.TempDir()
	j, err := Open(Options{
		Dir:         dir,
		Durability:  DurabilityGroupCommit,
		GroupCommit: GroupCommitPolicy{MaxDelay: 5 * time.Second, MaxBytes: 1},
	})
	require.NoError(t, err)
	defer j.Close()

	started := time.Now()
	partID := mustPartID(t)
	gen := mustGen(t)
	writePart(t, j, partID, gen, []byte("threshold wakes the group"))
	_, err = j.Activate(context.Background(), partID, gen, nil)
	require.NoError(t, err)
	require.Less(t, time.Since(started), 3*time.Second)
}

func TestSnapshotUsesIncrementalDurableState(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)

	partID := mustPartID(t)
	gen := mustGen(t)
	writePart(t, j, partID, gen, []byte("durable"))
	_, err = j.Activate(context.Background(), partID, gen, nil)
	require.NoError(t, err)

	// Destroy the path after publication. Snapshot must not rescan payload
	// bytes from disk on every migration planning pass.
	files, err := sortedFileIndices(dir)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, fileName(files[len(files)-1])), []byte("corrupt"), 0o644))
	snapshot, err := j.Snapshot()
	require.NoError(t, err)
	require.Equal(t, gen, snapshot.Live[partID])
	require.Contains(t, snapshot.Parts, gen)
	_ = j.Close()
}

func TestObjectLayoutBeforePartEnrichesFutureReplay(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)
	defer j.Close()

	objectID := partstore.DeriveObjectId("bucket", "key", "upload")
	partA, partB := mustPartID(t), mustPartID(t)
	require.NoError(t, j.FinalizeObjectLayout(context.Background(), objectID, []partstore.PartId{partA, partB}))

	genA, genB := mustGen(t), mustGen(t)
	writePart(t, j, partA, genA, []byte("a"))
	_, err = j.Activate(context.Background(), partA, genA, nil)
	require.NoError(t, err)
	writePart(t, j, partB, genB, []byte("b"))
	_, err = j.Activate(context.Background(), partB, genB, nil)
	require.NoError(t, err)

	snapshot, err := j.Snapshot()
	require.NoError(t, err)
	require.Equal(t, &objectID, snapshot.Parts[genA].ObjectID)
	require.Equal(t, uint64(1), *snapshot.Parts[genA].PartNumber)
	require.Equal(t, uint64(2), *snapshot.Parts[genA].PartCount)
	require.Equal(t, uint64(2), *snapshot.Parts[genB].PartNumber)
}

func TestObjectLayoutAllowsDeduplicatedRepeatedPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	j, err := Open(Options{Dir: t.TempDir()})
	require.NoError(t, err)
	defer j.Close()

	objectID := partstore.DeriveObjectId("bucket", "repeated", "upload")
	partID := mustPartID(t)
	require.NoError(t, j.FinalizeObjectLayout(context.Background(), objectID, []partstore.PartId{partID, partID}))
	generation := mustGen(t)
	writePart(t, j, partID, generation, []byte("same bytes used twice"))
	_, err = j.Activate(context.Background(), partID, generation, nil)
	require.NoError(t, err)

	snapshot, err := j.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), *snapshot.Parts[generation].PartNumber)
	require.Equal(t, uint64(2), *snapshot.Parts[generation].PartCount)
}

func TestScanRejectsCompleteRecordCorruption(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dir := t.TempDir()
	j, err := Open(Options{Dir: dir})
	require.NoError(t, err)
	partID, gen := mustPartID(t), mustGen(t)
	writePart(t, j, partID, gen, []byte("payload"))
	_, err = j.Activate(context.Background(), partID, gen, nil)
	require.NoError(t, err)
	require.NoError(t, j.Close())

	files, err := sortedFileIndices(dir)
	require.NoError(t, err)
	path := filepath.Join(dir, fileName(files[len(files)-1]))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff // corrupt a complete payload CRC
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = Scan(dir)
	require.ErrorContains(t, err, "corrupt record payload")
}

func TestCompactionPreservesDeletedState(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir, MaxFileBytes: 256})
	require.NoError(t, err)
	defer j.Close()

	deletedPart, deletedGen := mustPartID(t), mustGen(t)
	writePart(t, j, deletedPart, deletedGen, bytes.Repeat([]byte("x"), 400))
	_, err = j.Activate(ctx, deletedPart, deletedGen, nil)
	require.NoError(t, err)
	_, err = j.Delete(ctx, deletedPart, &deletedGen)
	require.NoError(t, err)

	// The next part rolls to another file, making the deleted part's file
	// reclaimable.
	writePart(t, j, mustPartID(t), mustGen(t), []byte("roll"))
	require.NoError(t, j.Compact(ctx))

	snapshot, err := j.Snapshot()
	require.NoError(t, err)
	_, live := snapshot.Live[deletedPart]
	require.False(t, live)
	foundDelete := false
	for _, op := range snapshot.Ops {
		foundDelete = foundDelete || op.IsDelete() && op.PartID.Equal(deletedPart)
	}
	require.True(t, foundDelete)
}

func TestCompactionProtectsTransactionStagedGeneration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(Options{Dir: dir, MaxFileBytes: 256})
	require.NoError(t, err)
	defer j.Close()

	stagedPart, stagedGen := mustPartID(t), mustGen(t)
	stagedLoc, err := j.StagePart(ctx, PartInput{Generation: stagedGen, PartID: stagedPart}, bytes.NewReader(bytes.Repeat([]byte("s"), 400)))
	require.NoError(t, err)

	// An unrelated durable write rolls the file and can make the staged bytes
	// durable before their owning database transaction reaches pre-commit.
	otherPart, otherGen := mustPartID(t), mustGen(t)
	writePart(t, j, otherPart, otherGen, []byte("other"))
	_, err = j.Activate(ctx, otherPart, otherGen, nil)
	require.NoError(t, err)
	require.NoError(t, j.Compact(ctx))
	require.Equal(t, bytes.Repeat([]byte("s"), 400), readPayload(t, j, stagedLoc))

	_, err = j.Activate(ctx, stagedPart, stagedGen, nil)
	require.NoError(t, err)
	snapshot, err := j.Snapshot()
	require.NoError(t, err)
	require.Equal(t, stagedGen, snapshot.Live[stagedPart])
}

func TestFileRollingKeepsPartsRecoverable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	dir := t.TempDir()
	// Tiny file limit forces a roll between parts.
	j, err := Open(Options{Dir: dir, MaxFileBytes: 512})
	require.NoError(t, err)

	const n = 20
	ids := make([]partstore.PartId, n)
	gens := make([]GenerationID, n)
	for i := range n {
		ids[i] = mustPartID(t)
		gens[i] = mustGen(t)
		writePart(t, j, ids[i], gens[i], bytes.Repeat([]byte("z"), 400))
		_, err = j.Activate(ctx, ids[i], gens[i], nil)
		require.NoError(t, err)
	}
	require.NoError(t, j.Close())

	// More than one segment file should exist.
	files, err := sortedFileIndices(dir)
	require.NoError(t, err)
	require.Greater(t, len(files), 1)

	scan, err := Scan(dir)
	require.NoError(t, err)
	require.Len(t, scan.Live, n)
	for i := range n {
		require.Equal(t, gens[i], scan.Live[ids[i]])
	}
}
