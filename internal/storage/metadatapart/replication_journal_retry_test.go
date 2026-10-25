package metadatapart

import (
	"context"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Sequence advancement survives rollback, so the first journal data insert
// fails inside PostgreSQL, after the input reader has already been consumed.
// Subsequent attempts must use a fresh reader and a fresh transaction.
func installJournalConflict(t *testing.T, st *metadataPartStorage) func() {
	t.Helper()
	require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
		for _, statement := range []string{
			"CREATE SEQUENCE journal_fault_calls",
			`CREATE FUNCTION journal_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('journal_fault_calls') = 1 THEN RAISE EXCEPTION 'injected journal conflict' USING ERRCODE = '40001'; END IF; RETURN NEW; END $$`,
			"CREATE TRIGGER journal_fault BEFORE INSERT ON replication_data FOR EACH ROW EXECUTE FUNCTION journal_fault()",
		} {
			if _, err := tx.SqlTx().ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}))
	return func() {
		require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
			_, err := tx.SqlTx().ExecContext(ctx, "ALTER SEQUENCE journal_fault_calls RESTART WITH 1")
			return err
		}))
	}
}

func TestReplicationPostgresLazySnapshotJournalRetries(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newPostgresTestStorage(t)
	defer done()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket := storage.MustNewBucketName("snapshot-retry")
	src, copied, completed := storage.MustNewObjectKey("source"), storage.MustNewObjectKey("copied"), storage.MustNewObjectKey("completed")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	_, err := coordinator.PutObject(t.Context(), bucket, src, nil, strings.NewReader("full snapshot content"), nil, nil)
	require.NoError(t, err)
	arm := installJournalConflict(t, st)
	copyResult, err := coordinator.CopyObject(t.Context(), bucket, src, bucket, copied, nil)
	require.NoError(t, err)
	require.Equal(t, "full snapshot content", readObjectContent(t, st, bucket, copied, copyResult.VersionID))
	require.Equal(t, 1, journalCount(t, st, "CopyObject"))
	upload, err := coordinator.CreateMultipartUpload(t.Context(), bucket, completed, nil, nil, nil)
	require.NoError(t, err)
	part, err := coordinator.UploadPart(t.Context(), bucket, completed, upload.UploadId, 1, strings.NewReader("multipart snapshot"), nil)
	require.NoError(t, err)
	arm()
	result, err := coordinator.CompleteMultipartUpload(t.Context(), bucket, completed, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{{PartNumber: 1, ETag: part.ETag}}})
	require.NoError(t, err)
	require.Equal(t, "multipart snapshot", readObjectContent(t, st, bucket, completed, result.VersionID))
	require.Equal(t, 1, journalCount(t, st, "CompleteMultipartUpload"))
	partCopyKey := storage.MustNewObjectKey("part-copy")
	upload, err = coordinator.CreateMultipartUpload(t.Context(), bucket, partCopyKey, nil, nil, nil)
	require.NoError(t, err)
	arm()
	partCopy, err := coordinator.UploadPartCopy(t.Context(), bucket, src, bucket, partCopyKey, upload.UploadId, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partCopy.ETag)
	require.Equal(t, 1, journalCount(t, st, "UploadPartCopy"))
	result, err = coordinator.CompleteMultipartUpload(t.Context(), bucket, partCopyKey, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{{PartNumber: 1, ETag: partCopy.ETag}}})
	require.NoError(t, err)
	require.Equal(t, "full snapshot content", readObjectContent(t, st, bucket, partCopyKey, result.VersionID))
	versions, err := st.ListObjectVersions(t.Context(), bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 4, "each retried mutation publishes exactly one version")
}

type snapshotCountingPrimary struct {
	replicaFaultStorage
	remote bool
	copies atomic.Int32
	opens  atomic.Int32
	closes atomic.Int32
}

func (s *snapshotCountingPrimary) Database() database.Database {
	if s.remote {
		return nil
	}
	return s.replicaFaultStorage.Database()
}
func (s *snapshotCountingPrimary) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	s.copies.Add(1)
	return s.Next.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
}
func (s *snapshotCountingPrimary) GetObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	obj, readers, err := s.Next.GetObject(ctx, bucket, key, ranges, opts)
	for i, reader := range readers {
		s.opens.Add(1)
		readers[i] = ioutils.NewReadCloserWithCloseHook(reader, func() error { s.closes.Add(1); return nil })
	}
	return obj, readers, err
}

func TestReplicationPostgresRemoteFinalizationRetry(t *testing.T) {
	testutils.SkipIfIntegration(t)
	primary, donePrimary := newTestStorage(t)
	defer donePrimary()
	journal, doneJournal := newPostgresTestStorage(t)
	defer doneJournal()
	p := &snapshotCountingPrimary{replicaFaultStorage: replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}, remote: true}
	coordinator, err := replication.NewStorageWithOptions(p, nil, replication.Options{ReplicationID: "remote-retry", JournalDatabase: journal.db, Registerer: prometheus.NewRegistry()})
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(t.Context()))
	defer func() { require.NoError(t, coordinator.Stop(context.Background())) }()
	bucket := storage.MustNewBucketName("remote-snapshot")
	src, dst := storage.MustNewObjectKey("source"), storage.MustNewObjectKey("destination")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket))
	_, err = coordinator.PutObject(t.Context(), bucket, src, nil, strings.NewReader("remote complete body"), nil, nil)
	require.NoError(t, err)
	installJournalConflict(t, journal)
	result, err := coordinator.CopyObject(t.Context(), bucket, src, bucket, dst, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, p.copies.Load(), "never reissue the remote mutation")
	require.EqualValues(t, 2, p.opens.Load(), "reopen the snapshot, not its exhausted reader")
	require.Equal(t, p.opens.Load(), p.closes.Load())
	require.Equal(t, "remote complete body", readObjectContent(t, primary, bucket, dst, result.VersionID))
	require.Equal(t, 1, journalCount(t, journal, "CopyObject"))
}

func TestReplicationPostgresReconcileSnapshotRetry(t *testing.T) {
	testutils.SkipIfIntegration(t)
	primary, done := newPostgresTestStorage(t)
	defer done()
	secondary, doneSecondary := newTestStorage(t)
	defer doneSecondary()
	bucket, key := storage.MustNewBucketName("reconcile-retry"), storage.MustNewObjectKey("key")
	require.NoError(t, primary.CreateBucket(t.Context(), bucket))
	_, err := primary.PutObject(t.Context(), bucket, key, nil, strings.NewReader("reconciled body"), nil, nil)
	require.NoError(t, err)
	p := &snapshotCountingPrimary{replicaFaultStorage: replicaFaultStorage{DelegatingStorage: delegator.Wrap(primary)}}
	s := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(secondary)}
	coordinator, err := replication.NewStorageWithOptions(p, []storage.Storage{s}, replication.Options{ReplicationID: "reconcile-retry", SecondaryIDs: []string{"replica"}, Registerer: prometheus.NewRegistry()})
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(t.Context()))
	defer func() { require.NoError(t, coordinator.Stop(context.Background())) }()
	installJournalConflict(t, primary)
	require.NoError(t, replication.ReconcileStorage(t.Context(), coordinator, "reconcile-retry", []storage.BucketName{bucket}, false))
	require.EqualValues(t, 2, p.opens.Load())
	require.Equal(t, p.opens.Load(), p.closes.Load())
	require.EqualValues(t, 1, s.writes.Load(), "no replica mutation before the successful journal commit")
	require.Equal(t, "reconciled body", readObjectContent(t, secondary, bucket, key, nil))
	require.Equal(t, 1, journalCount(t, primary, "PutObject"))
	require.Equal(t, 1, journalCount(t, primary, "PrepareReconcileBucket"))
}
