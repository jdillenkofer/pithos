package gc

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	pgxdatabase "github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partdedupindex"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	metadatastoresql "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	partstoresql "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

type contextBlockingDatabase struct {
	beginStarted chan struct{}
}

func (db *contextBlockingDatabase) BeginTx(ctx context.Context, _ *sql.TxOptions) (*database.TxController, error) {
	select {
	case <-db.beginStarted:
	default:
		close(db.beginStarted)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*contextBlockingDatabase) PingContext(context.Context) error { return nil }
func (*contextBlockingDatabase) Close() error                      { return nil }
func (*contextBlockingDatabase) GetDatabaseType() database.DatabaseType {
	return database.DB_TYPE_SQLITE
}

type gcTestEnv struct {
	db            database.Database
	metadataStore metadatastore.MetadataStore
	physicalStore partstore.PartStore
	registryRepo  partregistry.Repository
	dedupRepo     partdedupindex.Repository
	collector     *partGC
}

func newGCTestEnv(t *testing.T, db database.Database) *gcTestEnv {
	t.Helper()
	bucketRepo, err := repositoryfactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepo, err := repositoryfactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepo, err := repositoryfactory.NewPartRepository(db)
	require.NoError(t, err)
	registryRepo, err := repositoryfactory.NewPartRegistryRepository(db)
	require.NoError(t, err)
	dedupRepo, err := repositoryfactory.NewPartDedupIndexRepository(db)
	require.NoError(t, err)
	tagRepo, err := repositoryfactory.NewTagRepository(db)
	require.NoError(t, err)
	userMetadataRepo, err := repositoryfactory.NewUserMetadataRepository(db)
	require.NoError(t, err)
	metadataStore, err := metadatastoresql.New(db, bucketRepo, objectRepo, partRepo, tagRepo, userMetadataRepo)
	require.NoError(t, err)
	partContentRepo, err := repositoryfactory.NewPartContentRepository(db)
	require.NoError(t, err)
	physicalStore, err := partstoresql.New(db, partContentRepo)
	require.NoError(t, err)
	stores, err := partstore.NewNamedPartStores(physicalStore, nil, nil)
	require.NoError(t, err)
	collector, err := New(db, metadataStore, stores, registryRepo, dedupRepo)
	require.NoError(t, err)
	return &gcTestEnv{
		db:            db,
		metadataStore: metadataStore,
		physicalStore: physicalStore,
		registryRepo:  registryRepo,
		dedupRepo:     dedupRepo,
		collector:     collector.(*partGC),
	}
}

func newSqliteGCTestEnv(t *testing.T) *gcTestEnv {
	t.Helper()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "gc.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return newGCTestEnv(t, db)
}

func newPostgresGCTestEnv(t *testing.T) *gcTestEnv {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:18.4-alpine3.24@sha256:1b1689b20d16a014a3d195653381cf2caa75a41a92d93b255a9d6ea29fd353aa",
		postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dbURL, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := pgxdatabase.OpenDatabase(dbURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return newGCTestEnv(t, db)
}

func TestRunGCLoopCancellationInterruptsActiveCollection(t *testing.T) {
	db := &contextBlockingDatabase{beginStarted: make(chan struct{})}
	collector, err := New(db, nil, nil, nil, nil)
	require.NoError(t, err)
	partCollector := collector.(*partGC)
	partCollector.writeOperations.Add(1)

	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		collector.RunGCLoop(&stop)
		close(done)
	}()

	select {
	case <-db.beginStarted:
	case <-time.After(time.Second):
		t.Fatal("garbage collection did not start")
	}
	stop.Store(true)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("garbage collection did not stop after cancellation")
	}
}

func TestRunGCReconcilesRegistryFromPartsTable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	env := newSqliteGCTestEnv(t)

	bucket := metadatastore.MustNewBucketName("bucket")
	liveIDs := make([]partstore.PartId, 2)
	for i := range liveIDs {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		liveIDs[i] = *id
	}
	require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if err := env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket); err != nil {
			return err
		}
		for i, id := range liveIDs {
			if err := env.physicalStore.PutPart(ctx, tx, id, bytes.NewReader([]byte{byte(i)})); err != nil {
				return err
			}
			_, err := env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{
				Key:   metadatastore.MustNewObjectKey(string(rune('a' + i))),
				ETag:  "etag",
				Size:  1,
				Parts: []metadatastore.Part{{Id: id, ETag: "etag", Size: 1}},
			}, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}))

	orphanID, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if _, err := tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET ref_count = 99 WHERE part_id = $1", liveIDs[0].String()); err != nil {
			return err
		}
		if _, err := tx.SqlTx().ExecContext(ctx, "DELETE FROM part_registry WHERE part_id = $1", liveIDs[1].String()); err != nil {
			return err
		}
		_, err := tx.SqlTx().ExecContext(ctx, "INSERT INTO part_registry (part_id, ref_count, created_at, updated_at) VALUES ($1, 7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", orphanID.String())
		if err != nil {
			return err
		}
		_, err = tx.SqlTx().ExecContext(ctx, "INSERT INTO part_dedup_index (part_store_name, checksum_sha256, size, etag, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, part_id, created_at, updated_at) VALUES ('default', 'sha256', 1, 'etag', 'crc32', 'crc32c', 'crc64', 'sha1', $1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", orphanID.String())
		return err
	}))

	require.NoError(t, env.collector.runGC())

	require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		entities, err := env.registryRepo.FindAllEntities(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		counts := map[partstore.PartId]int64{}
		for _, entity := range entities {
			counts[entity.PartId] = entity.RefCount
		}
		assert.Equal(t, map[partstore.PartId]int64{liveIDs[0]: 1, liveIDs[1]: 1}, counts)
		ids, err := env.physicalStore.GetPartIds(ctx, tx)
		if err != nil {
			return err
		}
		assert.ElementsMatch(t, liveIDs, ids)
		indexedIDs, err := env.dedupRepo.FindAllPartIds(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		assert.Empty(t, indexedIDs)
		return nil
	}))
}

// runGCBackfillScenario seeds parts as they existed before the dedup index:
// two identical parts stored twice, one part without stored checksums and one
// part whose index entry already exists. GC must index exactly the smallest
// duplicate part id, skip the checksum-less part, keep the existing entry and
// stay idempotent across runs.
func runGCBackfillScenario(t *testing.T, env *gcTestEnv) {
	t.Helper()
	ctx := context.Background()
	bucket := metadatastore.MustNewBucketName("bucket")

	newPartID := func() partstore.PartId {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		return *id
	}
	dupIDs := []partstore.PartId{newPartID(), newPartID()}
	noChecksumID := newPartID()
	preIndexedID := newPartID()

	fullPart := func(id partstore.PartId, sha256 string) metadatastore.Part {
		return metadatastore.Part{
			Id:                id,
			ETag:              "etag-" + sha256,
			Size:              3,
			ChecksumCRC32:     ptrutils.ToPtr("crc32-" + sha256),
			ChecksumCRC32C:    ptrutils.ToPtr("crc32c-" + sha256),
			ChecksumCRC64NVME: ptrutils.ToPtr("crc64-" + sha256),
			ChecksumSHA1:      ptrutils.ToPtr("sha1-" + sha256),
			ChecksumSHA256:    ptrutils.ToPtr(sha256),
		}
	}
	putObject := func(tx database.Tx, key string, part metadatastore.Part) error {
		if err := env.physicalStore.PutPart(ctx, tx, part.Id, bytes.NewReader([]byte("abc"))); err != nil {
			return err
		}
		_, err := env.metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{
			Key:   metadatastore.MustNewObjectKey(key),
			ETag:  part.ETag,
			Size:  part.Size,
			Parts: []metadatastore.Part{part},
		}, nil)
		return err
	}

	require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if err := env.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket); err != nil {
			return err
		}
		if err := putObject(tx, "dup-1", fullPart(dupIDs[0], "dup-sha")); err != nil {
			return err
		}
		if err := putObject(tx, "dup-2", fullPart(dupIDs[1], "dup-sha")); err != nil {
			return err
		}
		if err := putObject(tx, "no-checksums", metadatastore.Part{Id: noChecksumID, ETag: "etag", Size: 3}); err != nil {
			return err
		}
		preIndexedPart := fullPart(preIndexedID, "pre-sha")
		if err := putObject(tx, "pre-indexed", preIndexedPart); err != nil {
			return err
		}
		inserted, err := env.dedupRepo.TryInsert(ctx, tx.SqlTx(), &partdedupindex.Entity{
			PartStoreName:     "",
			ChecksumSHA256:    *preIndexedPart.ChecksumSHA256,
			Size:              preIndexedPart.Size,
			ETag:              preIndexedPart.ETag,
			ChecksumCRC32:     *preIndexedPart.ChecksumCRC32,
			ChecksumCRC32C:    *preIndexedPart.ChecksumCRC32C,
			ChecksumCRC64NVME: *preIndexedPart.ChecksumCRC64NVME,
			ChecksumSHA1:      *preIndexedPart.ChecksumSHA1,
			PartId:            preIndexedID,
		})
		if err != nil {
			return err
		}
		require.True(t, inserted)
		return nil
	}))

	canonicalDupID := dupIDs[0]
	if dupIDs[1].String() < canonicalDupID.String() {
		canonicalDupID = dupIDs[1]
	}

	for range 2 {
		require.NoError(t, env.collector.runGC())
		require.NoError(t, database.WithTx(ctx, env.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			indexedIDs, err := env.dedupRepo.FindAllPartIds(ctx, tx.SqlTx())
			if err != nil {
				return err
			}
			assert.ElementsMatch(t, []partstore.PartId{canonicalDupID, preIndexedID}, indexedIDs)
			entry, err := env.dedupRepo.FindEntry(ctx, tx.SqlTx(), "", "dup-sha", 3)
			if err != nil {
				return err
			}
			require.NotNil(t, entry)
			assert.Equal(t, canonicalDupID, entry.PartId)
			assert.Equal(t, "etag-dup-sha", entry.ETag)
			assert.Equal(t, "crc32-dup-sha", entry.ChecksumCRC32)
			assert.Equal(t, "crc32c-dup-sha", entry.ChecksumCRC32C)
			assert.Equal(t, "crc64-dup-sha", entry.ChecksumCRC64NVME)
			assert.Equal(t, "sha1-dup-sha", entry.ChecksumSHA1)
			return nil
		}))
	}
}

func TestRunGCBackfillsDedupIndex(t *testing.T) {
	testutils.SkipIfIntegration(t)
	runGCBackfillScenario(t, newSqliteGCTestEnv(t))
}

func TestPostgresRunGCBackfillsDedupIndex(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	runGCBackfillScenario(t, newPostgresGCTestEnv(t))
}
