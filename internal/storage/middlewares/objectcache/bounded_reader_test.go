package objectcache

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type deadlineReader struct {
	io.ReadCloser
	deadline time.Time
}

func (r *deadlineReader) Deadline() time.Time { return r.deadline }

type boundedStorage struct {
	storage.Storage
	reader io.ReadCloser
}

func (s boundedStorage) GetObject(context.Context, storage.BucketName, storage.ObjectKey, []storage.ByteRange, *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	return &storage.Object{Size: 5}, []io.ReadCloser{s.reader}, nil
}
func TestBoundedStreamBypassesCachePipeAndPreservesDeadline(t *testing.T) {
	testutils.SkipIfIntegration(t)
	deadline := time.Now().Add(time.Minute)
	reader := ioutils.NewStatsReadCloser(&deadlineReader{io.NopCloser(strings.NewReader("hello")), deadline}, nil)
	cache := newMemoryCache()
	mw, err := NewStorageMiddleware(boundedStorage{reader: reader}, cache, Options{MaxObjectSizeBytes: 1024, CacheReadErrorsAsMiss: true})
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	for range 2 {
		_, readers, err := mw.GetObject(context.Background(), bucket, key, nil, nil)
		require.NoError(t, err)
		require.Same(t, reader, readers[0])
		require.Equal(t, deadline, readers[0].(interface{ Deadline() time.Time }).Deadline())
	}
	require.Empty(t, cache.data)
	require.NoError(t, reader.Close())
}
