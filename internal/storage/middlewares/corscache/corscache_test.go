package corscache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

// countingStorage records how often the CORS methods reach the backend.
type countingStorage struct {
	delegator.DelegatingStorage
	getCalls int
	config   *storage.BucketCORSConfiguration
	getErr   error
}

func newCountingStorage(config *storage.BucketCORSConfiguration, getErr error) *countingStorage {
	return &countingStorage{
		DelegatingStorage: delegator.Wrap(nil),
		config:            config,
		getErr:            getErr,
	}
}

func (c *countingStorage) GetBucketCORSConfiguration(_ context.Context, _ storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	c.getCalls++
	return c.config, c.getErr
}

func (c *countingStorage) PutBucketCORSConfiguration(_ context.Context, _ storage.BucketName, _ *storage.BucketCORSConfiguration) error {
	return nil
}

func (c *countingStorage) DeleteBucketCORSConfiguration(_ context.Context, _ storage.BucketName) error {
	return nil
}

func TestGetCachesSuccessfulReads(t *testing.T) {
	testutils.SkipIfIntegration(t)

	backend := newCountingStorage(&storage.BucketCORSConfiguration{Rules: []storage.CORSRule{{AllowedOrigins: []string{"*"}}}}, nil)
	mw := NewStorageMiddleware(backend)
	bucket := storage.MustNewBucketName("test-bucket")

	for i := 0; i < 5; i++ {
		config, err := mw.GetBucketCORSConfiguration(context.Background(), bucket)
		require.NoError(t, err)
		require.Len(t, config.Rules, 1)
	}
	assert.Equal(t, 1, backend.getCalls, "backend should be hit once and then served from cache")
}

func TestGetCachesNoSuchConfiguration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	backend := newCountingStorage(nil, storage.ErrNoSuchCORSConfiguration)
	mw := NewStorageMiddleware(backend)
	bucket := storage.MustNewBucketName("test-bucket")

	for i := 0; i < 5; i++ {
		_, err := mw.GetBucketCORSConfiguration(context.Background(), bucket)
		assert.ErrorIs(t, err, storage.ErrNoSuchCORSConfiguration)
	}
	assert.Equal(t, 1, backend.getCalls, "negative result should be cached")
}

func TestGetDoesNotCacheTransientErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)

	backend := newCountingStorage(nil, storage.ErrNoSuchBucket)
	mw := NewStorageMiddleware(backend)
	bucket := storage.MustNewBucketName("test-bucket")

	for i := 0; i < 3; i++ {
		_, err := mw.GetBucketCORSConfiguration(context.Background(), bucket)
		assert.ErrorIs(t, err, storage.ErrNoSuchBucket)
	}
	assert.Equal(t, 3, backend.getCalls, "transient errors must not be cached")
}

func TestPutAndDeleteInvalidateCache(t *testing.T) {
	testutils.SkipIfIntegration(t)

	backend := newCountingStorage(&storage.BucketCORSConfiguration{Rules: []storage.CORSRule{{AllowedOrigins: []string{"*"}}}}, nil)
	mw := NewStorageMiddleware(backend)
	bucket := storage.MustNewBucketName("test-bucket")

	_, err := mw.GetBucketCORSConfiguration(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, 1, backend.getCalls)

	require.NoError(t, mw.PutBucketCORSConfiguration(context.Background(), bucket, backend.config))
	_, err = mw.GetBucketCORSConfiguration(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, 2, backend.getCalls, "Put should invalidate the cached entry")

	require.NoError(t, mw.DeleteBucketCORSConfiguration(context.Background(), bucket))
	_, err = mw.GetBucketCORSConfiguration(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, 3, backend.getCalls, "Delete should invalidate the cached entry")
}

func TestGetExpiresAfterTTL(t *testing.T) {
	testutils.SkipIfIntegration(t)

	backend := newCountingStorage(&storage.BucketCORSConfiguration{Rules: []storage.CORSRule{{AllowedOrigins: []string{"*"}}}}, nil)
	mw := NewStorageMiddlewareWithTTL(backend, 20*time.Millisecond)
	bucket := storage.MustNewBucketName("test-bucket")

	_, err := mw.GetBucketCORSConfiguration(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, 1, backend.getCalls)

	time.Sleep(40 * time.Millisecond)

	_, err = mw.GetBucketCORSConfiguration(context.Background(), bucket)
	require.NoError(t, err)
	assert.Equal(t, 2, backend.getCalls, "entry should be refreshed after the TTL elapses")
}
