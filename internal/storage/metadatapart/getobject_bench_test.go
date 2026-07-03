package metadatapart

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	sqlMetadataStoreBench "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	filesystemPartStoreBench "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	tinkMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/encryption/tink"
)

func newTinkBenchStorage(b *testing.B) storage.Storage {
	b.Helper()
	storagePath := b.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	inner, err := filesystemPartStoreBench.New(filepath.Join(storagePath, "parts"))
	if err != nil {
		b.Fatal(err)
	}
	partStore, err := tinkMiddleware.NewWithLocalKMS("bench-password", inner, nil)
	if err != nil {
		b.Fatal(err)
	}

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	metadataStore, err := sqlMetadataStoreBench.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	if err != nil {
		b.Fatal(err)
	}
	st, err := NewStorage(db, metadataStore, partStore)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := st.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Stop(ctx) })
	return st
}

func putBenchObject(b *testing.B, st storage.Storage, bucketName storage.BucketName, name string, size int64) storage.ObjectKey {
	b.Helper()
	ctx := context.Background()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	key := storage.MustNewObjectKey(name)
	_, err := st.PutObject(ctx, bucketName, key, nil, ioutils.NewByteReadSeekCloser(data), nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	return key
}

func drainReaders(b *testing.B, readers []io.ReadCloser) int64 {
	b.Helper()
	if len(readers) == 0 {
		b.Fatal("no readers")
	}
	var total int64
	for _, r := range readers {
		n, err := ioutils.Copy(io.Discard, r)
		if err != nil {
			b.Fatal(err)
		}
		r.Close()
		total += n
	}
	return total
}

// BenchmarkTinkGetObjectSequential64MB measures full-object decrypt+read
// throughput through the encryption middleware.
func BenchmarkTinkGetObjectSequential64MB(b *testing.B) {
	st := newTinkBenchStorage(b)
	ctx := context.Background()
	bucketName := storage.MustNewBucketName("bench-bucket")
	if err := st.CreateBucket(ctx, bucketName); err != nil {
		b.Fatal(err)
	}
	const size = 64 * 1000 * 1000
	key := putBenchObject(b, st, bucketName, "seq-obj", size)

	b.SetBytes(size)
	b.ResetTimer()
	for b.Loop() {
		_, readers, err := st.GetObject(ctx, bucketName, key, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		if n := drainReaders(b, readers); n != size {
			b.Fatalf("short read: %d", n)
		}
	}
}

// BenchmarkTinkGetObjectRangeTail256MB measures a ranged read of the last
// 1MB of a 256MB encrypted object - the video-seek pattern.
func BenchmarkTinkGetObjectRangeTail256MB(b *testing.B) {
	st := newTinkBenchStorage(b)
	ctx := context.Background()
	bucketName := storage.MustNewBucketName("bench-bucket")
	if err := st.CreateBucket(ctx, bucketName); err != nil {
		b.Fatal(err)
	}
	const size = int64(256 * 1000 * 1000)
	const rangeLen = int64(1000 * 1000)
	key := putBenchObject(b, st, bucketName, "range-obj", size)

	start := size - rangeLen
	end := size

	b.SetBytes(rangeLen)
	b.ResetTimer()
	for b.Loop() {
		_, readers, err := st.GetObject(ctx, bucketName, key, []storage.ByteRange{{Start: &start, End: &end}}, nil)
		if err != nil {
			b.Fatal(err)
		}
		if n := drainReaders(b, readers); n != rangeLen {
			b.Fatalf("unexpected range read size: %d", n)
		}
	}
}

// BenchmarkTinkPutObject64MB measures encrypted upload throughput.
func BenchmarkTinkPutObject64MB(b *testing.B) {
	st := newTinkBenchStorage(b)
	ctx := context.Background()
	bucketName := storage.MustNewBucketName("bench-bucket")
	if err := st.CreateBucket(ctx, bucketName); err != nil {
		b.Fatal(err)
	}
	const size = 64 * 1000 * 1000
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}

	b.SetBytes(size)
	b.ResetTimer()
	i := 0
	for b.Loop() {
		key := storage.MustNewObjectKey(fmt.Sprintf("put-obj-%d", i))
		i++
		_, err := st.PutObject(ctx, bucketName, key, nil, ioutils.NewByteReadSeekCloser(data), nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
