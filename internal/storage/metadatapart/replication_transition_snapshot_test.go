package metadatapart

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	sqlpartstore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type transitionSnapshotKey struct{}

// Gate after consuming the eager first SQL chunk, before the next SQL query.
// Only the coordinator's context is gated; the direct writer uses its own tx.
type transitionSnapshotStore struct {
	partstore.PartStore
	afterFirstChunk func()
	reads           []int
}

func (s *transitionSnapshotStore) Capabilities() partstore.Capabilities {
	return partstore.CapabilitiesOf(s.PartStore)
}

func (s *transitionSnapshotStore) GetPart(ctx context.Context, tx database.Tx, id partstore.PartId) (io.ReadCloser, error) {
	r, err := s.PartStore.GetPart(ctx, tx, id)
	if err != nil || ctx.Value(transitionSnapshotKey{}) == nil {
		return r, err
	}
	s.reads = append(s.reads, 0)
	return &transitionSnapshotReader{ReadCloser: r, store: s, index: len(s.reads) - 1}, nil
}

type transitionSnapshotReader struct {
	io.ReadCloser
	store *transitionSnapshotStore
	index int
}

func (r *transitionSnapshotReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.store.reads[r.index] += n
	if n > 0 && r.store.afterFirstChunk != nil {
		gate := r.store.afterFirstChunk
		r.store.afterFirstChunk = nil
		gate()
	}
	return n, err
}

func TestReplicationPostgresTransitionStableLazySnapshot(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newPostgresTestStorage(t)
	defer done()
	repo, err := repositoryfactory.NewPartContentRepository(st.db)
	require.NoError(t, err)
	cold, err := sqlpartstore.New(st.db, repo, sqlpartstore.WithPartStoreId("cold"))
	require.NoError(t, err)
	archive, err := sqlpartstore.New(st.db, repo, sqlpartstore.WithPartStoreId("archive"))
	require.NoError(t, err)
	require.NoError(t, cold.Start(t.Context()))
	defer cold.Stop(context.Background())
	require.NoError(t, archive.Start(t.Context()))
	defer archive.Stop(context.Background())
	hotGate := &transitionSnapshotStore{PartStore: st.partStores.Default()}
	archiveGate := &transitionSnapshotStore{PartStore: archive}
	originalStores := st.partStores
	st.partStores, err = partstore.NewNamedPartStores(hotGate, map[string]partstore.PartStore{"cold": cold, "archive": archiveGate}, map[string]string{"GLACIER": "cold", "DEEP_ARCHIVE": "archive"})
	require.NoError(t, err)
	defer func() { st.partStores = originalStores }()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket, key := storage.MustNewBucketName("transition-snapshot"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	_, err = coordinator.PutObject(t.Context(), bucket, key, nil, strings.NewReader("abcdefghi"), nil, &storage.PutObjectOptions{Tags: map[string]string{"tag": "value"}})
	require.NoError(t, err)
	before, err := st.HeadObject(t.Context(), bucket, key, nil)
	require.NoError(t, err)
	// Keep real size/checksums/registry references, but use tiny physical chunks
	// to exercise production lazy SQL reads without a 768MB fixture.
	var oldID partstore.PartId
	require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
		obj, err := st.metadataStore.HeadObject(ctx, tx.SqlTx(), bucket, key)
		if err != nil {
			return err
		}
		oldID = obj.Parts[0].Id
		if err := repo.DeletePartContentById(ctx, tx.SqlTx(), partcontent.DefaultPartStoreId, oldID); err != nil {
			return err
		}
		for i, chunk := range []string{"abc", "def", "ghi"} {
			if err := repo.SavePartContent(ctx, tx.SqlTx(), partcontent.DefaultPartStoreId, &partcontent.Entity{Id: &oldID, ChunkIndex: i, Content: []byte(chunk)}); err != nil {
				return err
			}
		}
		return nil
	}))
	registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
	require.NoError(t, err)
	hotGate.afterFirstChunk = func() {
		require.Equal(t, []int{3}, hotGate.reads, "writer runs between chunk queries")
		// Bypass the replication mutex. The transition drops the old references;
		// emulate GC after its grace window to commit physical chunk deletion.
		require.NoError(t, st.TransitionObjectStorageClass(t.Context(), bucket, key, "DEEP_ARCHIVE", nil))
		require.Equal(t, "abcdefghi", readObjectContent(t, st, bucket, key, before.VersionID))
		require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
			_, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1 WHERE part_id=$2", time.Now().UTC().Add(-2*time.Hour), oldID.String())
			require.NoError(t, err)
			condemned, err := registry.Condemn(ctx, tx.SqlTx(), oldID, time.Now().UTC().Add(-time.Hour))
			require.NoError(t, err)
			require.True(t, condemned)
			require.NoError(t, hotGate.DeletePart(ctx, tx, oldID))
			chunk, err := repo.FindPartContentChunkByIndex(ctx, tx.SqlTx(), partcontent.DefaultPartStoreId, oldID, 1)
			require.NoError(t, err)
			require.Nil(t, chunk, "old lazy chunks really were deleted and committed")
			return nil
		}))
	}
	ctx := context.WithValue(t.Context(), transitionSnapshotKey{}, true)
	require.NoError(t, coordinator.TransitionObjectStorageClass(ctx, bucket, key, "GLACIER", nil))
	require.Equal(t, []int{9}, hotGate.reads, "the first attempt must finish the old snapshot, not silently accept EOF")
	require.Equal(t, []int{9}, archiveGate.reads, "the conflict must retry with fresh current source parts")
	require.Equal(t, "abcdefghi", readObjectContent(t, st, bucket, key, before.VersionID))
	after, err := st.HeadObject(t.Context(), bucket, key, nil)
	require.NoError(t, err)
	class := "GLACIER"
	before.StorageClass = &class
	require.False(t, after.LastModified.Before(before.LastModified))
	before.LastModified = after.LastModified
	require.Equal(t, before, after, "transition preserves size, checksums, version, tags, and other content metadata")
	require.Equal(t, 1, journalCount(t, st, "TransitionObjectStorageClass"))
	versions, err := st.ListObjectVersions(t.Context(), bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
		ids, err := cold.GetPartIds(ctx, tx)
		require.NoError(t, err)
		require.Len(t, ids, 1, "rolled-back target parts must not leak")
		return nil
	}))
}
