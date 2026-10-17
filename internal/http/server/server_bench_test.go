package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
)

const benchBucket = "bench-bucket"

// newBenchmarkHandler wires the HTTP server against a real sqlite metadata
// store and filesystem part store, so the benchmarks cover the full request
// path (routing, middlewares, authorization, storage, database).
func newBenchmarkHandler(b *testing.B) http.Handler {
	handler, _ := newBenchmarkServer(b)
	return handler
}

// newBenchmarkServer additionally returns the backing storage, so large
// fixtures can be seeded without going through HTTP.
func newBenchmarkServer(b *testing.B) (http.Handler, storage.Storage) {
	b.Helper()
	// Keep INFO-level request logging (as in production) but discard output so
	// the benchmark measures formatting cost without terminal I/O.
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})))
	b.Cleanup(func() { slog.SetDefault(previousLogger) })
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
	st, err := metadatapart.NewStorage(db, metadataStore, partStore)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := st.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Stop(ctx) })
	if err := st.CreateBucket(ctx, storage.MustNewBucketName(benchBucket)); err != nil {
		b.Fatal(err)
	}
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	if err != nil {
		b.Fatal(err)
	}
	return SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, st), st
}

func doBenchRequest(b *testing.B, handler http.Handler, method, target string, body []byte, wantCode int) *httptest.ResponseRecorder {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://s3.test"+target, reader)
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != wantCode {
		b.Fatalf("%s %s: got %d want %d: %s", method, target, w.Code, wantCode, w.Body.String())
	}
	return w
}

func benchPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	return data
}

func benchmarkHTTPPutObject(b *testing.B, size int) {
	handler := newBenchmarkHandler(b)
	data := benchPayload(size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		doBenchRequest(b, handler, "PUT", fmt.Sprintf("/%s/obj-%d", benchBucket, i), data, 200)
		i++
	}
}

func benchmarkHTTPGetObject(b *testing.B, size int) {
	handler := newBenchmarkHandler(b)
	doBenchRequest(b, handler, "PUT", "/"+benchBucket+"/obj", benchPayload(size), 200)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	for b.Loop() {
		doBenchRequest(b, handler, "GET", "/"+benchBucket+"/obj", nil, 200)
	}
}

func BenchmarkHTTPPutObject4KB(b *testing.B) { benchmarkHTTPPutObject(b, 4*1024) }
func BenchmarkHTTPPutObject1MB(b *testing.B) { benchmarkHTTPPutObject(b, 1000*1000) }
func BenchmarkHTTPGetObject4KB(b *testing.B) { benchmarkHTTPGetObject(b, 4*1024) }
func BenchmarkHTTPGetObject1MB(b *testing.B) { benchmarkHTTPGetObject(b, 1000*1000) }

func BenchmarkHTTPHeadObject(b *testing.B) {
	handler := newBenchmarkHandler(b)
	doBenchRequest(b, handler, "PUT", "/"+benchBucket+"/obj", benchPayload(1024), 200)
	b.ReportAllocs()
	for b.Loop() {
		doBenchRequest(b, handler, "HEAD", "/"+benchBucket+"/obj", nil, 200)
	}
}

func BenchmarkHTTPDeleteObject(b *testing.B) {
	handler := newBenchmarkHandler(b)
	data := benchPayload(1024)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		b.StopTimer()
		key := fmt.Sprintf("/%s/obj-%d", benchBucket, i)
		i++
		doBenchRequest(b, handler, "PUT", key, data, 200)
		b.StartTimer()
		doBenchRequest(b, handler, "DELETE", key, nil, 204)
	}
}

func benchmarkHTTPListObjectsV2(b *testing.B, query string) {
	handler := newBenchmarkHandler(b)
	data := benchPayload(16)
	for d := range 20 {
		for f := range 100 {
			doBenchRequest(b, handler, "PUT", fmt.Sprintf("/%s/dir-%02d/file-%03d", benchBucket, d, f), data, 200)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		doBenchRequest(b, handler, "GET", "/"+benchBucket+"?list-type=2&"+query, nil, 200)
	}
}

func BenchmarkHTTPListObjectsV2(b *testing.B) {
	benchmarkHTTPListObjectsV2(b, "max-keys=1000")
}

func BenchmarkHTTPListObjectsV2Delimiter(b *testing.B) {
	benchmarkHTTPListObjectsV2(b, "delimiter=/")
}

func BenchmarkHTTPListObjectsV2Prefix(b *testing.B) {
	benchmarkHTTPListObjectsV2(b, "prefix=dir-07/&max-keys=100")
}

// keysPerDir controls fixture size for the scalability benchmarks: a
// delimiter listing returns the same 20 common prefixes regardless of it, so
// the cost should not grow with it.
var keysPerDirSizes = []int{10, 100, 1000}

func newVersionedBenchmarkServer(b *testing.B, keysPerDir int) http.Handler {
	handler, st := newBenchmarkServer(b)
	ctx := context.Background()
	bucketName := storage.MustNewBucketName(benchBucket)
	status := storage.BucketVersioningStatusEnabled
	if err := st.PutBucketVersioningConfiguration(ctx, bucketName, &storage.BucketVersioningConfiguration{Status: &status}); err != nil {
		b.Fatal(err)
	}
	for d := range 20 {
		for f := range keysPerDir {
			key := storage.MustNewObjectKey(fmt.Sprintf("dir-%02d/file-%04d", d, f))
			if _, err := st.PutObject(ctx, bucketName, key, nil, bytes.NewReader([]byte("x")), nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	}
	return handler
}

// BenchmarkHTTPListObjectVersions pages the first 100 versions; the cost
// should not depend on how many versions follow.
func BenchmarkHTTPListObjectVersions(b *testing.B) {
	for _, keysPerDir := range keysPerDirSizes {
		b.Run(fmt.Sprintf("keysPerDir=%d", keysPerDir), func(b *testing.B) {
			handler := newVersionedBenchmarkServer(b, keysPerDir)
			b.ReportAllocs()
			for b.Loop() {
				doBenchRequest(b, handler, "GET", "/"+benchBucket+"?versions&max-keys=100", nil, 200)
			}
		})
	}
}

func BenchmarkHTTPListObjectVersionsDelimiter(b *testing.B) {
	for _, keysPerDir := range keysPerDirSizes {
		b.Run(fmt.Sprintf("keysPerDir=%d", keysPerDir), func(b *testing.B) {
			handler := newVersionedBenchmarkServer(b, keysPerDir)
			b.ReportAllocs()
			for b.Loop() {
				doBenchRequest(b, handler, "GET", "/"+benchBucket+"?versions&delimiter=/", nil, 200)
			}
		})
	}
}

func BenchmarkHTTPListMultipartUploadsDelimiter(b *testing.B) {
	for _, keysPerDir := range keysPerDirSizes {
		b.Run(fmt.Sprintf("keysPerDir=%d", keysPerDir), func(b *testing.B) {
			handler, st := newBenchmarkServer(b)
			ctx := context.Background()
			bucketName := storage.MustNewBucketName(benchBucket)
			for d := range 20 {
				for f := range keysPerDir {
					key := storage.MustNewObjectKey(fmt.Sprintf("dir-%02d/file-%04d", d, f))
					if _, err := st.CreateMultipartUpload(ctx, bucketName, key, nil, nil, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				doBenchRequest(b, handler, "GET", "/"+benchBucket+"?uploads&delimiter=/", nil, 200)
			}
		})
	}
}
