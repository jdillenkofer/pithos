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
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
)

func newBenchmarkStorage(b *testing.B) storage.Storage {
	b.Helper()
	storagePath := b.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	partStore, err := filesystemPartStore.New(filepath.Join(storagePath, "parts"))
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
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
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

func benchmarkPutGet(b *testing.B, size int64) {
	st := newBenchmarkStorage(b)
	ctx := context.Background()
	bucketName := storage.MustNewBucketName("bench-bucket")
	if err := st.CreateBucket(ctx, bucketName); err != nil {
		b.Fatal(err)
	}
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}

	b.Run("Put", func(b *testing.B) {
		b.SetBytes(size)
		i := 0
		for b.Loop() {
			key := storage.MustNewObjectKey(fmt.Sprintf("obj-%d", i))
			i++
			_, err := st.PutObject(ctx, bucketName, key, nil, ioutils.NewByteReadSeekCloser(data), nil, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	key := storage.MustNewObjectKey("get-obj")
	if _, err := st.PutObject(ctx, bucketName, key, nil, ioutils.NewByteReadSeekCloser(data), nil, nil); err != nil {
		b.Fatal(err)
	}
	b.Run("Get", func(b *testing.B) {
		b.SetBytes(size)
		for b.Loop() {
			_, readers, err := st.GetObject(ctx, bucketName, key, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(readers) == 0 {
				b.Fatal("no readers")
			}
			if _, err := ioutils.Copy(io.Discard, readers[0]); err != nil {
				b.Fatal(err)
			}
			readers[0].Close()
		}
	})
}

func BenchmarkStoragePutGet1MB(b *testing.B) {
	benchmarkPutGet(b, 1000*1000)
}

func BenchmarkStoragePutGet16MB(b *testing.B) {
	benchmarkPutGet(b, 16*1000*1000)
}

// BenchmarkStorageGetObjectRangeTail256MB measures a ranged read of the last
// 1MB of a 256MB object on the plain filesystem part store.
func BenchmarkStorageGetObjectRangeTail256MB(b *testing.B) {
	st := newBenchmarkStorage(b)
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
