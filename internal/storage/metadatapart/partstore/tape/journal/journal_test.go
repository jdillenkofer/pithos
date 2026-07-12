package journal

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
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

func TestFileRollingKeepsPartsRecoverable(t *testing.T) {
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
