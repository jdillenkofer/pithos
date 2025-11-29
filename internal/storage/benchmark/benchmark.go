package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
)

type SizeBenchmark struct {
	SizeBytes                   int64
	UploadSpeedBytesPerSecond   float64
	DownloadSpeedBytesPerSecond float64
}

type BenchmarkResult struct {
	SizeBenchmarks []SizeBenchmark
}

func BenchmarkStorage(ctx context.Context, st storage.Storage) (*BenchmarkResult, error) {
	// Create random bucket name
	bucketNamePtr, err := generateRandomUnusedBucketName(st)
	if err != nil {
		return nil, err
	}
	bucketName := *bucketNamePtr
	err = st.CreateBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanupObjectsInBucket(ctx, st, bucketName)
		st.DeleteBucket(ctx, bucketName)
	}()

	// Define sizes to test: 1MB, 10MB, 50MB, 100MB, 250MB
	testSizes := []int64{1000 * 1000, 10 * 1000 * 1000, 50 * 1000 * 1000, 100 * 1000 * 1000, 250 * 1000 * 1000}

	var sizeBenchmarks []SizeBenchmark

	for _, size := range testSizes {
		// Benchmark upload for this size
		uploadSpeed, err := benchmarkUploadSpeedForSize(ctx, st, bucketName, size)
		if err != nil {
			return nil, err
		}

		// Benchmark download for this size
		downloadSpeed, err := benchmarkDownloadSpeedForSize(ctx, st, bucketName, size)
		if err != nil {
			return nil, err
		}

		sizeBenchmarks = append(sizeBenchmarks, SizeBenchmark{
			SizeBytes:                   size,
			UploadSpeedBytesPerSecond:   uploadSpeed,
			DownloadSpeedBytesPerSecond: downloadSpeed,
		})
	}

	return &BenchmarkResult{
		SizeBenchmarks: sizeBenchmarks,
	}, nil
}

func generateRandomUnusedBucketName(st storage.Storage) (*storage.BucketName, error) {
	for {
		// Generate a random 8-byte suffix
		bytes := make([]byte, 8)
		_, err := rand.Read(bytes)
		if err != nil {
			return nil, err
		}

		// Create bucket name with prefix
		bucketNameStr := fmt.Sprintf("benchmark-%x", bytes)
		bucketName, err := metadatastore.NewBucketName(bucketNameStr)
		if err != nil {
			continue // try again if invalid
		}

		// Check if bucket exists
		_, err = st.HeadBucket(context.Background(), bucketName)
		if err != nil {
			// If error is ErrNoSuchBucket, bucket doesn't exist, so we can use it
			if err == metadatastore.ErrNoSuchBucket {
				return &bucketName, nil
			}
			// Other error, return it
			return nil, err
		}
		// Bucket exists, try again
	}
}

func benchmarkUploadSpeedForSize(ctx context.Context, st storage.Storage, bucketName storage.BucketName, sizeBytes int64) (float64, error) {
	// Calculate number of objects to upload based on size
	// Aim for approximately 100MB total data per test
	numObjects := max(100*1000*1000/sizeBytes, 1)

	// Create test data
	testData := make([]byte, sizeBytes)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Upload objects
	totalBytes := int64(0)
	startTime := time.Now()

	for i := int64(0); i < numObjects; i++ {
		key, err := metadatastore.NewObjectKey(fmt.Sprintf("benchmark-upload-%d-%d", sizeBytes, i))
		if err != nil {
			return 0, err
		}
		reader := ioutils.NewByteReadSeekCloser(testData)

		_, err = st.PutObject(ctx, bucketName, key, nil, reader, nil)
		if err != nil {
			return 0, err
		}

		totalBytes += sizeBytes
	}

	elapsed := time.Since(startTime)
	bytesPerSecond := float64(totalBytes) / elapsed.Seconds()

	return bytesPerSecond, nil
}

func benchmarkDownloadSpeedForSize(ctx context.Context, st storage.Storage, bucketName storage.BucketName, sizeBytes int64) (float64, error) {
	// Calculate number of objects to download (same as upload)
	numObjects := max(100*1000*1000/sizeBytes, 1)

	// Download objects
	totalBytes := int64(0)
	startTime := time.Now()

	for i := int64(0); i < numObjects; i++ {
		key, err := metadatastore.NewObjectKey(fmt.Sprintf("benchmark-upload-%d-%d", sizeBytes, i))
		if err != nil {
			return 0, err
		}

		reader, err := st.GetObject(ctx, bucketName, key, nil, nil)
		if err != nil {
			return 0, err
		}

		// Read all data to measure download speed
		buf := make([]byte, 64*1024) // 64KB buffer
		for {
			n, err := reader.Read(buf)
			totalBytes += int64(n)
			if err != nil {
				if err == io.EOF {
					break
				}
				return 0, err
			}
		}
		reader.Close()
	}

	elapsed := time.Since(startTime)
	bytesPerSecond := float64(totalBytes) / elapsed.Seconds()

	return bytesPerSecond, nil
}

func cleanupObjectsInBucket(ctx context.Context, st storage.Storage, bucketName storage.BucketName) error {
	objects, err := storage.ListAllObjectsOfBucket(ctx, st, bucketName)
	if err != nil {
		return err
	}

	for _, obj := range objects {
		err = st.DeleteObject(ctx, bucketName, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}
