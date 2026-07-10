package gc

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	metadatastoresql "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	partstoresql "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunGCReconcilesRegistryFromPartsTable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "gc.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

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

	bucket := metadatastore.MustNewBucketName("bucket")
	liveIDs := make([]partstore.PartId, 2)
	for i := range liveIDs {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		liveIDs[i] = *id
	}
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if err := metadataStore.CreateBucket(ctx, tx.SqlTx(), bucket); err != nil {
			return err
		}
		for i, id := range liveIDs {
			if err := physicalStore.PutPart(ctx, tx, id, bytes.NewReader([]byte{byte(i)})); err != nil {
				return err
			}
			_, err := metadataStore.PutObject(ctx, tx.SqlTx(), bucket, &metadatastore.Object{
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
	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
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

	require.NoError(t, collector.(*partGC).runGC())

	require.NoError(t, database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		entities, err := registryRepo.FindAllEntities(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		counts := map[partstore.PartId]int64{}
		for _, entity := range entities {
			counts[entity.PartId] = entity.RefCount
		}
		assert.Equal(t, map[partstore.PartId]int64{liveIDs[0]: 1, liveIDs[1]: 1}, counts)
		ids, err := physicalStore.GetPartIds(ctx, tx)
		if err != nil {
			return err
		}
		assert.ElementsMatch(t, liveIDs, ids)
		indexedIDs, err := dedupRepo.FindAllPartIds(ctx, tx.SqlTx())
		if err != nil {
			return err
		}
		assert.Empty(t, indexedIDs)
		return nil
	}))
}
