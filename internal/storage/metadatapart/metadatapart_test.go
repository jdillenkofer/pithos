package metadatapart

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataPartStorageWithSql(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartContentRepository: %s", err))
		os.Exit(1)
	}
	partStore, err := sqlPartStore.New(db, partContentRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}
	content := []byte("MetadataPartStorage")
	err = storage.Tester(metadataPartStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}

func TestMetadataPartStorageWithFilesystem(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()

	partStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}

	metadataPartStorage, err := NewStorage(db, metadataStore, partStore)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create MetadataPartStorage: %s", err))
		os.Exit(1)
	}
	content := []byte("MetadataPartStorage")
	err = storage.Tester(metadataPartStorage, []storage.BucketName{storage.MustNewBucketName("bucket")}, content)
	assert.Nil(t, err)
}

// newTestStorage creates a fresh MetadataPartStorage backed by a temporary SQLite DB and SQL part store.
// Returns the raw *metadataPartStorage (for access to internal methods) and a cleanup function.
func newTestStorage(t *testing.T) (*metadataPartStorage, func()) {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	require.NoError(t, err)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	require.NoError(t, err)

	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	require.NoError(t, err)
	partStore, err := sqlPartStore.New(db, partContentRepository)
	require.NoError(t, err)
	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepository, err := repositoryFactory.NewPartRepository(db)
	require.NoError(t, err)
	metaStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository)
	require.NoError(t, err)
	st, err := NewStorage(db, metaStore, partStore)
	require.NoError(t, err)
	mps := st.(*metadataPartStorage)

	ctx := context.Background()
	require.NoError(t, mps.Start(ctx))

	cleanup := func() {
		mps.Stop(ctx)
		db.Close()
		os.RemoveAll(storagePath)
	}
	return mps, cleanup
}

func TestConditionalDeleteObject_MatchingETag(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	result, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	etag := *result.ETag

	// Delete with correct ETag — should succeed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(etag)})
	require.NoError(t, err)

	// Object should be gone.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestConditionalDeleteObject_WrongETag(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Delete with wrong ETag — should return PreconditionFailed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr("wrong-etag")})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)

	// Object should still exist.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
}

func TestConditionalDeleteObject_NoObjectWithCondition(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete non-existent object with ETag condition — should return PreconditionFailed.
	_, err := st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr("any-etag")})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)
}

func TestConditionalDeleteObject_NoObjectNoCondition(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete non-existent object without condition — should silently succeed (S3 semantics).
	_, err := st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
}

func TestConditionalDeleteObjects_MixedConditions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	key1 := storage.MustNewObjectKey("obj1")
	key2 := storage.MustNewObjectKey("obj2")
	key3 := storage.MustNewObjectKey("obj3")

	res1, err := st.PutObject(ctx, bucket, key1, nil, bytes.NewReader([]byte("data1")), nil, nil)
	require.NoError(t, err)
	etag1 := *res1.ETag

	_, err = st.PutObject(ctx, bucket, key2, nil, bytes.NewReader([]byte("data2")), nil, nil)
	require.NoError(t, err)

	_, err = st.PutObject(ctx, bucket, key3, nil, bytes.NewReader([]byte("data3")), nil, nil)
	require.NoError(t, err)

	entries := []storage.DeleteObjectsInputEntry{
		{Key: key1, IfMatchETag: ptrutils.ToPtr(etag1)},        // correct etag → deleted
		{Key: key2, IfMatchETag: ptrutils.ToPtr("wrong-etag")}, // wrong etag → error entry
		{Key: key3}, // no condition → deleted
	}
	deleteResult, err := st.DeleteObjects(ctx, bucket, entries)
	require.NoError(t, err)
	require.Len(t, deleteResult.Entries, 3)

	// Find entries by key.
	byKey := make(map[string]storage.DeleteObjectsEntry)
	for _, e := range deleteResult.Entries {
		byKey[e.Key.String()] = e
	}

	assert.True(t, byKey["obj1"].Deleted, "obj1 should be deleted (correct etag)")
	assert.False(t, byKey["obj2"].Deleted, "obj2 should NOT be deleted (wrong etag)")
	assert.Equal(t, "PreconditionFailed", byKey["obj2"].ErrCode)
	assert.True(t, byKey["obj3"].Deleted, "obj3 should be deleted (no condition)")

	// Verify obj1 and obj3 are gone, obj2 still exists.
	_, err = st.HeadObject(ctx, bucket, key1, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
	_, err = st.HeadObject(ctx, bucket, key2, nil)
	require.NoError(t, err)
	_, err = st.HeadObject(ctx, bucket, key3, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestConditionalDeleteObject_WildcardMatchExistingObject(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.PutObject(ctx, bucket, key, nil, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Delete with If-Match: * on an existing object — should succeed.
	_, err = st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(storage.ETagWildcard)})
	require.NoError(t, err)

	// Object should be gone.
	_, err = st.HeadObject(ctx, bucket, key, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchKey)
}

func TestConditionalDeleteObject_WildcardMatchMissingObject(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("nonexistent")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Delete with If-Match: * on a non-existent object — should return PreconditionFailed.
	_, err := st.DeleteObject(ctx, bucket, key, &storage.DeleteObjectOptions{IfMatchETag: ptrutils.ToPtr(storage.ETagWildcard)})
	assert.ErrorIs(t, err, storage.ErrPreconditionFailed)
}

// --- AppendObject tests ---

func TestAppendObject_CreateOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Append to a non-existent key — should behave like PutObject.
	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.ETag)
	assert.Equal(t, int64(5), result.Size)

	obj, err := st.HeadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), obj.Size)
}

func TestAppendObject_AppendsToExisting(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(11), result.Size)

	// Read back and verify the concatenated content.
	_, readers, err := st.GetObject(ctx, bucket, key, nil, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)
	defer readers[0].Close()
	content, err := io.ReadAll(readers[0])
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(content))
}

func TestAppendObject_ETagIsMultipartStyle(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part1")), nil, nil)
	require.NoError(t, err)

	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("part2")), nil, nil)
	require.NoError(t, err)

	// After two appends the object has 2 parts, so ETag must end in "-2".
	// ETags are stored with surrounding double-quotes, e.g. `"abc123-2"`.
	assert.True(t, len(result.ETag) > 3 && result.ETag[len(result.ETag)-3:] == "-2\"",
		"expected multipart ETag ending in -2\", got %q", result.ETag)
}

func TestAppendObject_CorrectWriteOffset(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	first, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Append with the correct write offset (== current size) — should succeed.
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil,
		&storage.AppendObjectOptions{WriteOffset: &first.Size})
	require.NoError(t, err)
}

func TestAppendObject_WrongWriteOffset(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil, nil)
	require.NoError(t, err)

	// Append with a wrong write offset — should return ErrInvalidWriteOffset.
	wrongOffset := int64(999)
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte(" world")), nil,
		&storage.AppendObjectOptions{WriteOffset: &wrongOffset})
	assert.ErrorIs(t, err, storage.ErrInvalidWriteOffset)
}

func TestAppendObject_WriteOffsetZeroOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// WriteOffset == 0 on a non-existent object should succeed (create new object).
	zero := int64(0)
	result, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil,
		&storage.AppendObjectOptions{WriteOffset: &zero})
	require.NoError(t, err)
	assert.Equal(t, int64(5), result.Size)
}

func TestAppendObject_WriteOffsetNonZeroOnMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("obj")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// WriteOffset != 0 on a non-existent object should return ErrInvalidWriteOffset.
	nonZero := int64(5)
	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("hello")), nil,
		&storage.AppendObjectOptions{WriteOffset: &nonZero})
	assert.ErrorIs(t, err, storage.ErrInvalidWriteOffset)
}

func TestAppendObject_NoSuchBucket(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("nonexistent")
	key := storage.MustNewObjectKey("obj")

	_, err := st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("data")), nil, nil)
	assert.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

func TestAppendObject_TooManyParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	st, cleanup := newTestStorage(t)
	defer cleanup()

	bucket := storage.MustNewBucketName("bucket")
	key := storage.MustNewObjectKey("stress")
	require.NoError(t, st.CreateBucket(ctx, bucket))

	// Seed an object with exactly 10,000 parts directly via the metadata store
	// so we don't have to perform 10,000 real appends (which would be O(n²) in
	// SQLite and cause CI timeouts).
	const maxParts = 10_000
	parts := make([]metadatastore.Part, maxParts)
	partChecksumsList := make([]checksumutils.PartChecksums, maxParts)

	// ETag of a single byte "x", as produced by CalculateChecksumsStreaming.
	partData := []byte("x")
	_, partChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, bytes.NewReader(partData), func(r io.Reader) error { return nil })
	require.NoError(t, err)
	partETag := *partChecksums.ETag

	for i := range parts {
		pid, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		parts[i] = metadatastore.Part{
			Id:   *pid,
			ETag: partETag,
			Size: int64(len(partData)),
		}
		partChecksumsList[i] = checksumutils.PartChecksums{ETag: partETag, Size: int64(len(partData))}
	}

	objectChecksums, err := checksumutils.CalculateMultipartChecksums(partChecksumsList, checksumutils.ChecksumTypeFullObject)
	require.NoError(t, err)

	tx, err := st.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	for _, p := range parts {
		require.NoError(t, st.partStore.PutPart(ctx, tx, p.Id, bytes.NewReader(partData)))
	}
	err = st.metadataStore.PutObject(ctx, tx, bucket, &metadatastore.Object{
		Key:   key,
		ETag:  *objectChecksums.ETag,
		Size:  int64(maxParts) * int64(len(partData)),
		Parts: parts,
	}, nil)
	require.NoError(t, tx.Commit())
	require.NoError(t, err)

	// The next append must fail with ErrTooManyParts.
	_, err = st.AppendObject(ctx, bucket, key, bytes.NewReader([]byte("x")), nil, nil)
	assert.ErrorIs(t, err, storage.ErrTooManyParts)
}
