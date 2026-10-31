package metadatapart

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/gc"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// A deduplicated write never deletes its fresh duplicate itself; the next GC
// sweep reclaims it without retention, as no reader can hold it, while the
// shared part stays live.
func TestDedupDuplicateIsReclaimedByGC(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var st *metadataPartStorage
			var cleanup func()
			if backend == "postgres" {
				st, cleanup = newPostgresTestStorage(t)
			} else {
				st, cleanup = newTestStorage(t)
			}
			defer cleanup()
			ctx := context.Background()
			registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
			require.NoError(t, err)
			dedup, err := repositoryfactory.NewPartDedupIndexRepository(st.db)
			require.NoError(t, err)
			collector, err := gc.New(st.db, st.metadataStore, st.partStores, registry, dedup, time.Hour, 5*time.Millisecond)
			require.NoError(t, err)

			bucket := storage.MustNewBucketName("bucket")
			firstKey := storage.MustNewObjectKey("first")
			secondKey := storage.MustNewObjectKey("second")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			content := []byte("deduplicated content")
			_, err = st.PutObject(ctx, bucket, firstKey, nil, bytes.NewReader(content), nil, nil)
			require.NoError(t, err)
			_, err = st.PutObject(ctx, bucket, secondKey, nil, bytes.NewReader(content), nil, nil)
			require.NoError(t, err)
			require.Len(t, physicalPartIDs(t, st), 2)
			require.Equal(t, []int64{0, 2}, registryRefCounts(t, st))

			var stop atomic.Bool
			done := make(chan struct{})
			go func() { defer close(done); collector.RunGCLoop(&stop) }()
			defer func() { stop.Store(true); <-done }()
			require.Eventually(t, func() bool { return len(physicalPartIDs(t, st)) == 1 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []int64{2}, registryRefCounts(t, st))
			require.Equal(t, string(content), readObjectContent(t, st, bucket, firstKey, nil))
			require.Equal(t, string(content), readObjectContent(t, st, bucket, secondKey, nil))
		})
	}
}
