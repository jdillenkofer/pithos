package lua

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	delegator.DelegatingStorage
	createdBuckets []storage.BucketName
	putContent     string
}

func (s *testStorage) Start(ctx context.Context) error {
	return nil
}

func (s *testStorage) Stop(ctx context.Context) error {
	return nil
}

func (s *testStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	s.createdBuckets = append(s.createdBuckets, bucketName)
	return nil
}

func (s *testStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	return []storage.Bucket{{
		Name:         storage.MustNewBucketName("bucket"),
		CreationDate: time.Unix(0, 0).UTC(),
	}}, nil
}

func (s *testStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	content, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	s.putContent = string(content)
	return &storage.PutObjectResult{}, nil
}

func (s *testStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	return &storage.Object{
		Key:          key,
		LastModified: time.Unix(0, 0).UTC(),
		ETag:         "etag",
		Size:         int64(len(s.putContent)),
	}, []io.ReadCloser{ioutils.NewByteReadSeekCloser([]byte(s.putContent))}, nil
}

func TestMissingLuaFunctionDelegatesToInnerStorage(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, ``)
	require.NoError(t, err)

	err = store.CreateBucket(context.Background(), storage.MustNewBucketName("bucket"))
	require.NoError(t, err)
	require.Len(t, inner.createdBuckets, 1)
	assert.True(t, storage.MustNewBucketName("bucket").Equals(inner.createdBuckets[0]))
}

func TestLuaFunctionCanRewriteArguments(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function CreateBucket(ctx, bucketName)
  return innerStorage.CreateBucket(ctx, "rewritten")
end
`)
	require.NoError(t, err)

	err = store.CreateBucket(context.Background(), storage.MustNewBucketName("bucket"))
	require.NoError(t, err)
	require.Len(t, inner.createdBuckets, 1)
	assert.True(t, storage.MustNewBucketName("rewritten").Equals(inner.createdBuckets[0]))
}

func TestWithTransactionRoutesOperationsThroughLuaHooks(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function CreateBucket(ctx, bucketName)
  return innerStorage.CreateBucket(ctx, "rewritten")
end
`)
	require.NoError(t, err)

	txStore, ok := store.(storage.TransactionalStorage)
	require.True(t, ok)

	err = txStore.WithTransaction(context.Background(), nil, func(ctx context.Context, txStorage storage.Storage) error {
		return txStorage.CreateBucket(ctx, storage.MustNewBucketName("bucket"))
	})
	require.NoError(t, err)
	require.Len(t, inner.createdBuckets, 1)
	assert.True(t, storage.MustNewBucketName("rewritten").Equals(inner.createdBuckets[0]))
}

func TestLuaFunctionCanTransformPutObjectStreamByChunk(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
  local mapped = {
    read = function(self, size)
      local chunk, err = data:read(3)
      if chunk == nil then
        return nil, err
      end
      return string.upper(chunk), nil
    end,
    close = function(self)
      return data:close()
    end
  }
  return innerStorage.PutObject(ctx, bucketName, key, contentType, mapped, checksumInput, opts)
end
`)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("abcdef"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "ABCDEF", inner.putContent)
}

func TestLuaFunctionCanTransformGetObjectStreamByChunk(t *testing.T) {
	inner := &testStorage{putContent: "abcdef"}
	store, err := NewStorageMiddleware(inner, `
function GetObject(ctx, bucketName, key, ranges, opts)
  local object, readers, err = innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
  if err ~= nil then
    return nil, nil, err
  end
  local source = readers[1]
  readers[1] = {
    read = function(self, size)
      local chunk, readErr = source:read(2)
      if chunk == nil then
        return nil, readErr
      end
      return string.upper(chunk), nil
    end,
    close = function(self)
      return source:close()
    end
  }
  return object, readers, nil
end
`)
	require.NoError(t, err)

	_, readers, err := store.GetObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	assert.Equal(t, "ABCDEF", string(content))
}
