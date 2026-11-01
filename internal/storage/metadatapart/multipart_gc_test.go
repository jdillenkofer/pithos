package metadatapart

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/gc"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// Completing a multipart upload with a subset of the uploaded parts builds the
// object from the listed parts only, renumbers them, and leaves the unlisted
// parts to GC.
func TestCompleteMultipartUploadWithSubsetReleasesUnusedPartsToGC(t *testing.T) {
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
			stopStorageGC(t, st)
			ctx := context.Background()
			registry, err := repositoryfactory.NewPartRegistryRepository(st.db)
			require.NoError(t, err)
			dedup, err := repositoryfactory.NewPartDedupIndexRepository(st.db)
			require.NoError(t, err)
			collector, err := gc.New(st.db, st.metadataStore, st.partStores, registry, dedup, st.readerLeases.store, 5*time.Millisecond, 5*time.Millisecond)
			require.NoError(t, err)

			bucket := storage.MustNewBucketName("bucket")
			key := storage.MustNewObjectKey("key")
			require.NoError(t, st.CreateBucket(ctx, bucket))
			upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, nil)
			require.NoError(t, err)

			contents := map[int32]string{1: strings.Repeat("a", 5*1024*1024), 2: "unused second", 3: strings.Repeat("b", 5*1024*1024), 5: "fifth"}
			etags := map[int32]string{}
			for _, partNumber := range []int32{1, 2, 3, 5} {
				result, err := st.UploadPart(ctx, bucket, key, upload.UploadId, partNumber, bytes.NewReader([]byte(contents[partNumber])), nil)
				require.NoError(t, err)
				etags[partNumber] = result.ETag
			}
			require.Len(t, physicalPartIDs(t, st), 4)

			opts := &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: etags[1]},
				{PartNumber: 3, ETag: etags[3]},
				{PartNumber: 5, ETag: etags[5]},
			}}
			result, err := st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, opts)
			require.NoError(t, err)
			require.Regexp(t, `-3"?$`, result.ETag)
			require.Equal(t, contents[1]+contents[3]+contents[5], readObjectContent(t, st, bucket, key, nil))

			var stop atomic.Bool
			done := make(chan struct{})
			go func() { defer close(done); collector.RunGCLoop(&stop) }()
			defer func() { stop.Store(true); <-done }()
			require.Eventually(t, func() bool { return len(physicalPartIDs(t, st)) == 3 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []int64{1, 1, 1}, registryRefCounts(t, st))
			require.Equal(t, contents[1]+contents[3]+contents[5], readObjectContent(t, st, bucket, key, nil))
		})
	}
}

func TestCompleteMultipartUploadRejectsEmptyPartList(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	upload, err := st.CreateMultipartUpload(ctx, bucket, key, nil, nil, nil)
	require.NoError(t, err)
	_, err = st.UploadPart(ctx, bucket, key, upload.UploadId, 1, bytes.NewReader([]byte("part")), nil)
	require.NoError(t, err)

	_, err = st.CompleteMultipartUpload(ctx, bucket, key, upload.UploadId, nil, &storage.CompleteMultipartUploadOptions{Parts: []storage.CompleteMultipartUploadPart{}})
	require.ErrorIs(t, err, storage.ErrMalformedXML)
	parts, err := st.ListParts(ctx, bucket, key, upload.UploadId, storage.ListPartsOptions{MaxParts: 10})
	require.NoError(t, err)
	require.Len(t, parts.Parts, 1)
}
