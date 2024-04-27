package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func setupTestServer(usePathStyle bool) (s3Client *s3.Client, cleanup func()) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(storagePath, "metadata.db"))
	if err != nil {
		log.Fatalf("Could not open sqlite3 database: %s", err)
	}
	metadataStore, err := metadata.NewSqlMetadataStore(db)
	if err != nil {
		log.Fatalf("Could not create metadataStore: %s", err)
	}
	blobStore, err := blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
	if err != nil {
		log.Fatalf("Could not create filesystemBlobStore: %s", err)
	}
	storage, err := storage.NewMetadataBlobStorage(metadataStore, blobStore)
	if err != nil {
		log.Fatalf("Could not create metadataBlobStorage: %s", err)
	}

	baseEndpoint := "localhost"
	ts := httptest.NewServer(server.SetupServer(baseEndpoint, storage))

	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			endpointSplit := strings.SplitN(addr, ".", 2)
			if len(endpointSplit) == 2 {
				addr = endpointSplit[1]
			}
			return net.Dial(network, addr)
		}
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("eu-central-1"), config.WithHTTPClient(httpClient), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		log.Fatalf("Could not loadDefaultConfig: %s", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", ts.Listener.Addr().String())
	s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s:%d", baseEndpoint, addr.Port))
	})

	cleanup = func() {
		ts.Close()
		err := db.Close()
		if err != nil {
			log.Fatalf("Could not close db: %s", err)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}
	return
}

func TestBasicBucketOperationsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	bucketName := aws.String("test")
	bucketName2 := aws.String("test2")
	keyPrefix := aws.String("my/test/key")
	key := aws.String(*keyPrefix + "/hello_world.txt")
	body := []byte("Hello, world!")

	t.Parallel()

	for _, usePathStyle := range []bool{false, true} {
		pathStyleSuffix := " using host style"
		if usePathStyle {
			pathStyleSuffix = " using path style"
		}
		t.Run("it should create a bucket"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			assert.Equal(t, bucketName, createBucketResult.Location)
		})

		t.Run("it should not be able to create the same bucket twice"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})

			if err == nil {
				assert.Fail(t, "CreateBucket should failed when reusing the same bucket name")
			}

			var bucketAlreadyExistsError *types.BucketAlreadyExists
			if !errors.As(err, &bucketAlreadyExistsError) {
				assert.Fail(t, "Expected aws error BucketAlreadyExists", "err %v", err)
			}
		})

		t.Run("it should be able to see an existing bucket"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			assert.NotNil(t, createBucketResult)
			headBucketResult, err := s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			if err != nil {
				assert.Fail(t, "HeadBucket failed", "err %v", err)
			}
			assert.NotNil(t, headBucketResult)
		})

		t.Run("it should be able to list all buckets"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listBucketsResult, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})

			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}

			assert.Len(t, listBucketsResult.Buckets, 1)
			assert.Equal(t, bucketName, listBucketsResult.Buckets[0].Name)
			assert.NotNil(t, listBucketsResult.Buckets[0].CreationDate)
			assert.True(t, listBucketsResult.Buckets[0].CreationDate.Before(time.Now()))
		})

		t.Run("it should allow uploading an object"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should not allow deleting a bucket with objects in it"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			_, err = s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "BucketNotEmpty" {
				assert.Fail(t, "Expected aws error BucketNotEmpty", "err %v", err)
			}
		})

		t.Run("it should allow uploading an object a second time"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should allow downloading the object"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body, objectBytes)
		})

		t.Run("it should allow downloading the object with byte range"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:5], objectBytes)
		})

		t.Run("it should allow deleting an object"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			deleteObjectResult, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
		})

		t.Run("it should delete an existing bucket"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
		})

		t.Run("it should fail when deleting non existing bucket"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			_, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
				Bucket: aws.String("test2"),
			})

			if err == nil {
				assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
			}

			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucket" {
				assert.Fail(t, "Expected aws error NoSuchBucket", "err %v", err)
			}
		})

		t.Run("it should not see the bucket after deletion anymore"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)

			_, err = s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			var notFoundError *types.NotFound
			if !errors.As(err, &notFoundError) {
				assert.Fail(t, "Expected aws error NotFound", "err %v", err)
			}
		})

		t.Run("it should list all buckets"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createBucketResult2, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName2,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult2)

			listBucketResult, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}
			assert.Len(t, listBucketResult.Buckets, 2)
		})

		t.Run("it should list all objects"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Len(t, listObjectResult.Contents, 1)
		})

		t.Run("it should list objects starting with prefix my/test/key"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Len(t, listObjectResult.Contents, 1)
		})

		t.Run("it should list no objects when searching for prefix key"+pathStyleSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
				Prefix: aws.String("key"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Len(t, listObjectResult.Contents, 0)
		})
	}
}
