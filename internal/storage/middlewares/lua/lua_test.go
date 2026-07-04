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
	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	delegator.DelegatingStorage
	createdBuckets []storage.BucketName
	putContent     string
	contentType    *string
	putOpts        *storage.PutObjectOptions
	headOpts       *storage.HeadObjectOptions
	getRanges      []storage.ByteRange
	startCount     int
	stopCount      int
}

func (s *testStorage) Start(ctx context.Context) error {
	s.startCount++
	return nil
}

func (s *testStorage) Stop(ctx context.Context) error {
	s.stopCount++
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
	s.contentType = contentType
	s.putOpts = opts
	return &storage.PutObjectResult{}, nil
}

func (s *testStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	s.headOpts = opts
	return &storage.Object{
		Key:          key,
		ContentType:  s.contentType,
		LastModified: time.Unix(0, 0).UTC(),
		ETag:         "etag",
		Size:         int64(len(s.putContent)),
	}, nil
}

func (s *testStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	s.getRanges = ranges
	if len(ranges) > 1 {
		readers := make([]io.ReadCloser, 0, len(ranges))
		for _, byteRange := range ranges {
			start := int64(0)
			end := int64(len(s.putContent))
			if byteRange.Start != nil {
				start = *byteRange.Start
			}
			if byteRange.End != nil {
				end = *byteRange.End
			}
			readers = append(readers, ioutils.NewByteReadSeekCloser([]byte(s.putContent[start:end])))
		}
		return &storage.Object{
			Key:          key,
			LastModified: time.Unix(0, 0).UTC(),
			ETag:         "etag",
			Size:         int64(len(s.putContent)),
		}, readers, nil
	}
	return &storage.Object{
		Key:          key,
		ContentType:  s.contentType,
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

func TestLifecycleMethodsDelegateToInnerStorageWithoutLuaHooks(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function Start(ctx)
  return "NoSuchBucket"
end

function Stop(ctx)
  return "NoSuchBucket"
end
`)
	require.NoError(t, err)

	err = store.Start(context.Background())
	require.NoError(t, err)
	err = store.Stop(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.startCount)
	assert.Equal(t, 1, inner.stopCount)
}

func TestMissingPutObjectFunctionDelegatesTypedNilArgumentsToInnerStorage(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, ``)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("content"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "content", inner.putContent)
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

func TestLuaFunctionCanReplacePutObjectStreamWithStaticReader(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
  local emitted = false
  local static = {
    read = function(self, size)
      if emitted then
        return nil, nil
      end
      emitted = true
      return "static-content", nil
    end,
    close = function(self)
      return nil
    end
  }
  return innerStorage.PutObject(ctx, bucketName, key, contentType, static, checksumInput, opts)
end
`)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("ignored-content"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "static-content", inner.putContent)
}

func TestLuaFunctionCanReturnStorageError(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function CreateBucket(ctx, bucketName)
  return "NoSuchBucket"
end
`)
	require.NoError(t, err)

	err = store.CreateBucket(context.Background(), storage.MustNewBucketName("bucket"))
	assert.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

func TestNewStorageMiddlewareRejectsInvalidLuaSyntax(t *testing.T) {
	_, err := NewStorageMiddleware(&testStorage{}, `function CreateBucket(ctx, bucketName)`)
	require.Error(t, err)
}

func TestLuaRuntimeErrorIsReturned(t *testing.T) {
	store, err := NewStorageMiddleware(&testStorage{}, `
function CreateBucket(ctx, bucketName)
  error("boom")
end
`)
	require.NoError(t, err)

	err = store.CreateBucket(context.Background(), storage.MustNewBucketName("bucket"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestLuaFunctionCanMutateReturnedObjectMetadata(t *testing.T) {
	inner := &testStorage{putContent: "abcdef"}
	store, err := NewStorageMiddleware(inner, `
function HeadObject(ctx, bucketName, key, opts)
  local object, err = innerStorage.HeadObject(ctx, bucketName, key, opts)
  if err ~= nil then
    return nil, err
  end
  object.contentType = "text/lua"
  return object, nil
end
`)
	require.NoError(t, err)

	object, err := store.HeadObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil)
	require.NoError(t, err)
	require.NotNil(t, object.ContentType)
	assert.Equal(t, "text/lua", *object.ContentType)
}

func TestLuaFunctionRoundTripsOptionsAndByteRanges(t *testing.T) {
	ifMatch := "etag"
	writeOffset := int64(6)
	inner := &testStorage{putContent: "abcdef"}
	store, err := NewStorageMiddleware(inner, `
function PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
  if opts.ifNoneMatchStar ~= true or opts.ifMatchETag ~= "etag" then
    return nil, "PreconditionFailed"
  end
  return innerStorage.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
end

function HeadObject(ctx, bucketName, key, opts)
  if opts.ifMatchETag ~= "etag" then
    return nil, "PreconditionFailed"
  end
  return innerStorage.HeadObject(ctx, bucketName, key, opts)
end

function AppendObject(ctx, bucketName, key, data, checksumInput, opts)
  if opts.writeOffset ~= 6 then
    return nil, "InvalidWriteOffset"
  end
  return { eTag = "etag", size = 12 }, nil
end

function GetObject(ctx, bucketName, key, ranges, opts)
  if ranges[1].start ~= 1 or ranges[1]["end"] ~= 4 then
    return nil, nil, "InvalidRange"
  end
  return innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
end
`)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("content"), nil, &storage.PutObjectOptions{
		IfNoneMatchStar: true,
		IfMatchETag:     &ifMatch,
	})
	require.NoError(t, err)
	require.NotNil(t, inner.putOpts)
	assert.True(t, inner.putOpts.IfNoneMatchStar)
	assert.Equal(t, "etag", *inner.putOpts.IfMatchETag)

	_, err = store.HeadObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), &storage.HeadObjectOptions{IfMatchETag: &ifMatch})
	require.NoError(t, err)
	require.NotNil(t, inner.headOpts)
	assert.Equal(t, "etag", *inner.headOpts.IfMatchETag)

	appendResult, err := store.AppendObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), strings.NewReader("append"), nil, &storage.AppendObjectOptions{WriteOffset: &writeOffset})
	require.NoError(t, err)
	assert.Equal(t, "etag", appendResult.ETag)
	assert.Equal(t, int64(12), appendResult.Size)

	start := int64(1)
	end := int64(4)
	_, _, err = store.GetObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), []storage.ByteRange{{Start: &start, End: &end}}, nil)
	require.NoError(t, err)
	require.Len(t, inner.getRanges, 1)
	assert.Equal(t, int64(1), *inner.getRanges[0].Start)
	assert.Equal(t, int64(4), *inner.getRanges[0].End)
}

func TestLuaFunctionCanTransformMultipleGetObjectReaders(t *testing.T) {
	inner := &testStorage{putContent: "abcdef"}
	store, err := NewStorageMiddleware(inner, `
function GetObject(ctx, bucketName, key, ranges, opts)
  local object, readers, err = innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
  if err ~= nil then
    return nil, nil, err
  end
  for i = 1, 2 do
    local source = readers[i]
    readers[i] = {
      read = function(self, size)
        local chunk, readErr = source:read(size)
        if chunk == nil then
          return nil, readErr
        end
        return string.upper(chunk), nil
      end,
      close = function(self)
        return source:close()
      end
    }
  end
  return object, readers, nil
end
`)
	require.NoError(t, err)

	start1 := int64(0)
	end1 := int64(3)
	start2 := int64(3)
	end2 := int64(6)
	_, readers, err := store.GetObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), []storage.ByteRange{
		{Start: &start1, End: &end1},
		{Start: &start2, End: &end2},
	}, nil)
	require.NoError(t, err)
	require.Len(t, readers, 2)

	first, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	second, err := io.ReadAll(readers[1])
	require.NoError(t, err)
	assert.Equal(t, "ABC", string(first))
	assert.Equal(t, "DEF", string(second))
}

func TestLuaReaderReadErrorPropagates(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
  local failing = {
    read = function(self, size)
      return nil, "read failed"
    end
  }
  return innerStorage.PutObject(ctx, bucketName, key, contentType, failing, checksumInput, opts)
end
`)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("ignored"), nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read failed")
}

func TestLuaReaderCloseErrorPropagates(t *testing.T) {
	store, err := NewStorageMiddleware(&testStorage{putContent: "abcdef"}, `
function GetObject(ctx, bucketName, key, ranges, opts)
  local object, readers, err = innerStorage.GetObject(ctx, bucketName, key, ranges, opts)
  if err ~= nil then
    return nil, nil, err
  end
  readers[1] = {
    read = function(self, size)
      return nil, nil
    end,
    close = function(self)
      return "close failed"
    end
  }
  return object, readers, nil
end
`)
	require.NoError(t, err)

	_, readers, err := store.GetObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	err = readers[0].Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

func TestLuaFunctionCanReturnStaticGetObjectReader(t *testing.T) {
	store, err := NewStorageMiddleware(&testStorage{putContent: "ignored"}, `
function GetObject(ctx, bucketName, key, ranges, opts)
  local emitted = false
  local reader = {
    read = function(self, size)
      if emitted then
        return nil, nil
      end
      emitted = true
      return "static-get-content", nil
    end
  }
  return {
    key = key,
    lastModified = "1970-01-01T00:00:00Z",
    eTag = "etag",
    size = 18
  }, { reader }, nil
end
`)
	require.NoError(t, err)

	object, readers, err := store.GetObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(18), object.Size)
	require.Len(t, readers, 1)
	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	assert.Equal(t, "static-get-content", string(content))
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

func TestLuaFunctionRoundTripsTagsAndUserMetadata(t *testing.T) {
	inner := &testStorage{}
	store, err := NewStorageMiddleware(inner, `
function PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
  if opts.tags.env ~= "prod" or opts.metadata.userMetadata.owner ~= "me" then
    return nil, "PreconditionFailed"
  end
  opts.tags.added = "yes"
  return innerStorage.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
end
`)
	require.NoError(t, err)

	_, err = store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key"), nil, strings.NewReader("content"), nil, &storage.PutObjectOptions{
		Tags:     map[string]string{"env": "prod"},
		Metadata: &storage.ObjectMetadata{UserMetadata: map[string]string{"owner": "me"}},
	})
	require.NoError(t, err)
	require.NotNil(t, inner.putOpts)
	assert.Equal(t, map[string]string{"env": "prod", "added": "yes"}, inner.putOpts.Tags)
	require.NotNil(t, inner.putOpts.Metadata)
	assert.Equal(t, map[string]string{"owner": "me"}, inner.putOpts.Metadata.UserMetadata)
}
