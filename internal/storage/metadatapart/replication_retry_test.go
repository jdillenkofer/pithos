package metadatapart

import (
	"context"
	"database/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlpartstore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// This adapter changes only lifecycle ownership, and explicitly opts in. The
// general delegator intentionally does not forward the replay-safety promise.
type retryLifecycleStorage struct{ delegator.DelegatingStorage }

func (*retryLifecycleStorage) Start(context.Context) error { return nil }
func (*retryLifecycleStorage) Stop(context.Context) error  { return nil }
func (s *retryLifecycleStorage) RetrySafeTransactions() bool {
	return s.Next.(interface{ RetrySafeTransactions() bool }).RetrySafeTransactions()
}

func retryCoordinator(t *testing.T, primary *metadataPartStorage) (storage.Storage, func()) {
	t.Helper()
	p := &retryLifecycleStorage{delegator.Wrap(primary)}
	coordinator, err := replication.NewStorageWithOptions(p, nil, replication.Options{ReplicationID: "retry", Registerer: prometheus.NewRegistry()})
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(t.Context()))
	return coordinator, func() { require.NoError(t, coordinator.Stop(context.Background())) }
}

type retryMetadataStore struct {
	metadatastore.MetadataStore
	calls  int
	before func(context.Context, *sql.Tx) error
	after  func(context.Context, *sql.Tx) error
}

func (*retryMetadataStore) RetrySafeTransactions() bool { return true }
func (s *retryMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucket metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) (*metadatastore.PartMutationResult, error) {
	s.calls++
	if s.before != nil {
		if err := s.before(ctx, tx); err != nil {
			return nil, err
		}
	}
	result, err := s.MetadataStore.PutObject(ctx, tx, bucket, obj, opts)
	if err == nil && s.after != nil {
		err = s.after(ctx, tx)
	}
	return result, err
}

func journalCount(t *testing.T, st *metadataPartStorage, operation string) int {
	t.Helper()
	var count int
	require.NoError(t, database.WithTx(t.Context(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		return tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM replication_operations WHERE operation=$1", operation).Scan(&count)
	}))
	return count
}

func TestReplicationRetriesConsumedInputAfterRollback(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var st *metadataPartStorage
			var done func()
			if backend == "postgres" {
				st, done = newPostgresTestStorage(t)
			} else {
				st, done = newTestStorage(t)
			}
			defer done()
			coordinator, stop := retryCoordinator(t, st)
			defer stop()
			bucket, key := storage.MustNewBucketName("retry-input"), storage.MustNewObjectKey("key")
			require.NoError(t, coordinator.CreateBucket(t.Context(), bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
			metadata := &retryMetadataStore{MetadataStore: st.metadataStore}
			metadata.after = func(ctx context.Context, tx *sql.Tx) error {
				if metadata.calls != 1 {
					return nil
				}
				if backend == "postgres" {
					_, err := tx.ExecContext(ctx, "DO $$ BEGIN RAISE EXCEPTION 'injected rollback' USING ERRCODE = '40001'; END $$")
					return err
				}
				return &pgconn.PgError{Code: "40001"}
			}
			st.metadataStore = metadata
			observations := 0
			ctx := storage.WithObjectLockObserver(t.Context(), func(storage.ObjectLockObservation) { observations++ })
			// strings.Reader is deliberately hidden behind Reader: callers need not
			// supply a seekable or otherwise replayable request stream.
			input := struct{ io.Reader }{strings.NewReader("one complete request body")}
			result, err := coordinator.PutObject(ctx, bucket, key, nil, input, nil, nil)
			require.NoError(t, err)
			require.Equal(t, 2, metadata.calls)
			require.Equal(t, 1, observations, "rolled-back observations must not escape")
			require.Equal(t, "one complete request body", readObjectContent(t, st, bucket, key, result.VersionID))
			versions, err := st.ListObjectVersions(t.Context(), bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
			require.NoError(t, err)
			require.Len(t, versions.Versions, 1)
			require.Equal(t, 1, journalCount(t, st, "PutObject"))
			require.NoError(t, database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
				ids, err := st.partStores.Default().GetPartIds(ctx, tx)
				require.NoError(t, err)
				require.Len(t, ids, 1, "failed attempt must not leave SQL content")
				return nil
			}))
		})
	}
}

func TestReplicationPostgresConcurrentBucketWriter(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newPostgresTestStorage(t)
	defer done()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket := storage.MustNewBucketName("retry-copy")
	src, dst := storage.MustNewObjectKey("source"), storage.MustNewObjectKey("destination")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	_, err := coordinator.PutObject(t.Context(), bucket, src, nil, strings.NewReader("snapshot content"), nil, nil)
	require.NoError(t, err)
	metadata := &retryMetadataStore{MetadataStore: st.metadataStore}
	var firstConflict error
	metadata.before = func(ctx context.Context, tx *sql.Tx) error {
		if metadata.calls != 1 {
			return nil
		}
		// The copy has already pinned its RR snapshot and acquired source part
		// references. Commit a real concurrent bucket update before its FOR SHARE.
		writerDone := make(chan error, 1)
		go func() {
			writerDone <- database.WithTx(t.Context(), st.db, nil, func(ctx context.Context, tx database.Tx) error {
				_, err := tx.SqlTx().ExecContext(ctx, "UPDATE buckets SET updated_at = CURRENT_TIMESTAMP WHERE name=$1", bucket.String())
				return err
			})
		}()
		require.NoError(t, <-writerDone)
		return nil
	}
	metadata.after = nil
	// Capture the server's actual conflict, rather than synthesizing SQLSTATE.
	st.metadataStore = &captureRetryMetadataStore{retryMetadataStore: metadata, conflict: &firstConflict}
	result, err := coordinator.CopyObject(t.Context(), bucket, src, bucket, dst, nil)
	require.NoError(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, firstConflict, &pgErr)
	require.Equal(t, "40001", pgErr.Code)
	require.Equal(t, 2, metadata.calls)
	require.Equal(t, "snapshot content", readObjectContent(t, st, bucket, dst, result.VersionID))
	require.Equal(t, 1, journalCount(t, st, "CopyObject"))
	versions, err := st.ListObjectVersions(t.Context(), bucket, storage.ListObjectVersionsOptions{MaxKeys: 100})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 2, "source and exactly one destination version")
}

type transitionRetryMetadataStore struct {
	metadatastore.MetadataStore
	calls int
}

func (*transitionRetryMetadataStore) RetrySafeTransactions() bool { return true }
func (s *transitionRetryMetadataStore) TransitionObject(ctx context.Context, tx *sql.Tx, bucket metadatastore.BucketName, key metadatastore.ObjectKey, version *string, etag, class string, parts []metadatastore.Part) (*metadatastore.PartMutationResult, error) {
	s.calls++
	result, err := s.MetadataStore.TransitionObject(ctx, tx, bucket, key, version, etag, class, parts)
	if err == nil && s.calls == 1 {
		// Fail after consuming the lazy source and updating metadata. Retrying is
		// permitted only if all configured stores roll back their writes.
		err = &pgconn.PgError{Code: "40001"}
	}
	return result, err
}

func TestReplicationTransitionRetrySafety(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, external := range []bool{false, true} {
			name := backend + "/sql"
			if external {
				name = backend + "/external"
			}
			t.Run(name, func(t *testing.T) {
				var st *metadataPartStorage
				var done func()
				if backend == "postgres" {
					st, done = newPostgresTestStorage(t)
				} else {
					st, done = newTestStorage(t)
				}
				defer done()
				stores := st.partStores
				var target partstore.PartStore
				if external {
					fs, err := filesystem.New(t.TempDir())
					require.NoError(t, err)
					require.NoError(t, fs.Start(t.Context()))
					defer fs.Stop(context.Background())
					target = fs
				} else {
					repo, err := repositoryfactory.NewPartContentRepository(st.db)
					require.NoError(t, err)
					target, err = sqlpartstore.New(st.db, repo, sqlpartstore.WithPartStoreId("cold"))
					require.NoError(t, err)
					require.NoError(t, target.Start(t.Context()))
					defer target.Stop(context.Background())
				}
				var err error
				st.partStores, err = partstore.NewNamedPartStores(stores.Default(), map[string]partstore.PartStore{"cold": target}, map[string]string{"GLACIER": "cold"})
				require.NoError(t, err)
				defer func() { st.partStores = stores }()
				coordinator, stop := retryCoordinator(t, st)
				defer stop()
				bucket, key := storage.MustNewBucketName("transition-retry"), storage.MustNewObjectKey("key")
				require.NoError(t, coordinator.CreateBucket(t.Context(), bucket))
				_, err = coordinator.PutObject(t.Context(), bucket, key, nil, strings.NewReader("complete transition body"), nil, nil)
				require.NoError(t, err)
				metadata := &transitionRetryMetadataStore{MetadataStore: st.metadataStore}
				st.metadataStore = metadata
				err = coordinator.TransitionObjectStorageClass(t.Context(), bucket, key, "GLACIER", nil)
				if external {
					var pgErr *pgconn.PgError
					require.ErrorAs(t, err, &pgErr)
					require.Equal(t, "40001", pgErr.Code)
					require.Equal(t, 1, metadata.calls, "never replay external transition writes")
					require.Equal(t, 0, journalCount(t, st, "TransitionObjectStorageClass"))
					_, cold := st.partStores.StoreForClass("GLACIER")
					ids, err := cold.GetPartIds(t.Context(), nil)
					require.NoError(t, err)
					require.Len(t, ids, 1, "exactly one external effect, not one per retry")
				} else {
					require.NoError(t, err)
					require.Equal(t, 2, metadata.calls)
					require.Equal(t, 1, journalCount(t, st, "TransitionObjectStorageClass"))
				}
				require.Equal(t, "complete transition body", readObjectContent(t, st, bucket, key, nil))
				obj, err := st.HeadObject(t.Context(), bucket, key, nil)
				require.NoError(t, err)
				if external {
					require.Nil(t, obj.StorageClass, "failed external transition must roll back metadata")
				} else {
					require.Equal(t, "GLACIER", *obj.StorageClass)
				}
			})
		}
	}
}

type captureRetryMetadataStore struct {
	*retryMetadataStore
	conflict *error
}

func (s *captureRetryMetadataStore) PutObject(ctx context.Context, tx *sql.Tx, bucket metadatastore.BucketName, obj *metadatastore.Object, opts *metadatastore.PutObjectOptions) (*metadatastore.PartMutationResult, error) {
	result, err := s.retryMetadataStore.PutObject(ctx, tx, bucket, obj, opts)
	if err != nil && *s.conflict == nil {
		*s.conflict = err
	}
	return result, err
}

func TestReplicationDoesNotReplayExternalPartWrites(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newPostgresTestStorage(t)
	defer done()
	fs, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, fs.Start(t.Context()))
	defer fs.Stop(context.Background())
	stores := st.partStores
	st.partStores, err = partstore.NewNamedPartStores(fs, nil, nil)
	require.NoError(t, err)
	defer func() { st.partStores = stores }()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket, key := storage.MustNewBucketName("external-retry"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket))
	metadata := &retryMetadataStore{MetadataStore: st.metadataStore}
	metadata.after = func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "DO $$ BEGIN RAISE EXCEPTION 'external effect already happened' USING ERRCODE = '40001'; END $$")
		return err
	}
	st.metadataStore = metadata
	_, err = coordinator.PutObject(t.Context(), bucket, key, nil, strings.NewReader("external body"), nil, nil)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "40001", pgErr.Code)
	require.Equal(t, 1, metadata.calls)
	ids, err := fs.GetPartIds(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, ids, 1, "never double-publish external content on rollback")
	require.Equal(t, 0, journalCount(t, st, "PutObject"))
	_, err = st.HeadObject(t.Context(), bucket, key, nil)
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestReplicationDoesNotInferMiddlewareReplaySafety(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newTestStorage(t)
	defer done()
	p := &replicaFaultStorage{DelegatingStorage: delegator.Wrap(st)}
	coordinator, err := replication.NewStorageWithOptions(p, nil, replication.Options{ReplicationID: "middleware", Registerer: prometheus.NewRegistry()})
	require.NoError(t, err)
	require.NoError(t, coordinator.Start(t.Context()))
	defer coordinator.Stop(context.Background())
	bucket, key := storage.MustNewBucketName("middleware"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket))
	metadata := &retryMetadataStore{MetadataStore: st.metadataStore, after: func(context.Context, *sql.Tx) error {
		return &pgconn.PgError{Code: "40001"}
	}}
	st.metadataStore = metadata
	_, err = coordinator.PutObject(t.Context(), bucket, key, nil, strings.NewReader("body"), nil, nil)
	require.Error(t, err)
	require.EqualValues(t, 1, p.writes.Load(), "Database does not imply middleware effects can be replayed")
	require.Equal(t, 1, metadata.calls)
	require.Equal(t, 0, journalCount(t, st, "PutObject"))
}

func TestReplicationPreservesFinalDenialObservation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newTestStorage(t)
	defer done()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket, key := storage.MustNewBucketName("denial"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket, storage.CreateBucketOptions{ObjectLockEnabled: true}))
	result, err := coordinator.PutObject(t.Context(), bucket, key, nil, strings.NewReader("body"), nil, nil)
	require.NoError(t, err)
	require.NoError(t, coordinator.PutObjectLegalHold(t.Context(), bucket, key, storage.LegalHoldOn, &storage.ObjectLockOptions{VersionID: result.VersionID}))
	var observations []storage.ObjectLockObservation
	ctx := storage.WithObjectLockObserver(t.Context(), func(o storage.ObjectLockObservation) { observations = append(observations, o) })
	_, err = coordinator.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{VersionID: result.VersionID})
	require.ErrorIs(t, err, storage.ErrObjectLockAccessDenied)
	require.Len(t, observations, 1, "a final denial remains visible to audit observers")
	require.ErrorIs(t, observations[0].Err, storage.ErrObjectLockAccessDenied)
}

func TestReplicationRejectsAmbientWriteSnapshots(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, done := newTestStorage(t)
	defer done()
	coordinator, stop := retryCoordinator(t, st)
	defer stop()
	bucket, key := storage.MustNewBucketName("ambient"), storage.MustNewObjectKey("key")
	require.NoError(t, coordinator.CreateBucket(t.Context(), bucket))
	tx, err := st.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	defer tx.Rollback(t.Context())
	ctx := database.ContextWithTx(t.Context(), tx.Child())
	_, err = coordinator.PutObject(ctx, bucket, key, nil, strings.NewReader("body"), nil, nil)
	require.ErrorContains(t, err, "own commit boundary")
	err = replication.ReconcileStorage(ctx, coordinator, "retry", []storage.BucketName{bucket}, false)
	require.ErrorContains(t, err, "own commit boundary")
}
