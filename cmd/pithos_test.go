package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	sqliteStorageOutboxEntryRepository "github.com/jdillenkofer/pithos/internal/storage/database/repository/storageoutboxentry/sqlite"
	"github.com/stretchr/testify/assert"
)

const accessKeyId string = "AKIAIOSFODNN7EXAMPLE"
const secretAccessKey string = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
const region string = "eu-central-1"
const blobStoreEncryptionPassword = "test"

var bucketName *string = aws.String("test")
var bucketName2 *string = aws.String("test2")
var keyPrefix *string = aws.String("my/test/key")
var key *string = aws.String(*keyPrefix + "/hello_world.txt")
var key2 *string = aws.String(*keyPrefix + "/hello_world2.txt")
var body []byte = []byte("Hello, world!")

func setupS3Client(baseEndpoint string, listenerAddr string, usePathStyle bool) *s3.Client {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			endpointSplit := strings.SplitN(addr, ".", 2)
			if len(endpointSplit) == 2 {
				addr = endpointSplit[1]
			}
			return net.Dial(network, addr)
		}
	})

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region), config.WithHTTPClient(httpClient), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, "")))
	if err != nil {
		log.Fatalf("Could not loadDefaultConfig: %s", err)
	}
	addr, err := net.ResolveTCPAddr("tcp", listenerAddr)
	if err != nil {
		log.Fatalf("Could not resolveTcpAddr: %s", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s:%d", baseEndpoint, addr.Port))
	})
	return s3Client
}

func setupTestServer(usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) (s3Client *s3.Client, cleanup func()) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}

	baseEndpoint := "localhost"

	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatalf("Couldn't open database: %s", err)
	}
	encryptionPassword := ""
	if encryptBlobStore {
		encryptionPassword = blobStoreEncryptionPassword
	}
	store := storage.CreateStorage(storagePath, db, useFilesystemBlobStore, encryptionPassword, wrapBlobStoreWithOutbox)

	if !useReplication {
		store, err = storage.NewPrometheusStorageMiddleware(store, registry)
		if err != nil {
			log.Fatalf("Could not create prometheusStorageMiddleware: %s", err)
		}
	}

	err = store.Start(ctx)
	if err != nil {
		log.Fatalf("Couldn't start storage: %s", err)
	}
	closeStorage := func() {
		err := store.Stop(ctx)
		if err != nil {
			log.Fatalf("Couldn't stop storage: %s", err)
		}
	}
	closeDatabase := func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Couldn't close database: %s", err)
		}
	}

	ts := httptest.NewServer(server.SetupServer(accessKeyId, secretAccessKey, region, baseEndpoint, store))

	if useReplication {
		originalTs := ts
		originalCloseStorage := closeStorage
		originalCloseDatabase := closeDatabase
		storagePath2, err := os.MkdirTemp("", "pithos-test-data-")
		if err != nil {
			log.Fatalf("Could not create temp directory: %s", err)
		}
		db2, err := database.OpenDatabase(storagePath2)
		if err != nil {
			log.Fatalf("Couldn't open database: %s", err)
		}
		localStore := storage.CreateStorage(storagePath2, db2, useFilesystemBlobStore, encryptionPassword, wrapBlobStoreWithOutbox)

		s3Client = setupS3Client(baseEndpoint, originalTs.Listener.Addr().String(), usePathStyle)
		var s3ClientStorage storage.Storage
		s3ClientStorage, err = storage.NewS3ClientStorage(s3Client)
		if err != nil {
			log.Fatalf("Could not create s3ClientStorage: %s", err)
		}
		s3ClientStorage, err = storage.NewTracingStorageMiddleware("S3ClientStorage", s3ClientStorage)
		if err != nil {
			log.Fatalf("Error during TracingStorageMiddleware: %s", err)
		}

		var outboxStorage storage.Storage
		storageOutboxEntryRepository, err := sqliteStorageOutboxEntryRepository.New(db2)
		if err != nil {
			log.Fatalf("Could not create StorageOutboxEntryRepository: %s", err)

		}
		outboxStorage, err = storage.NewOutboxStorage(db2, s3ClientStorage, storageOutboxEntryRepository)
		if err != nil {
			log.Fatalf("Could not create outboxStorage: %s", err)
		}

		outboxStorage, err = storage.NewTracingStorageMiddleware("OutboxStorage", outboxStorage)
		if err != nil {
			log.Fatalf("Error during TracingStorageMiddleware: %s", err)
		}

		var store2 storage.Storage
		store2, err = storage.NewReplicationStorage(localStore, outboxStorage)
		if err != nil {
			log.Fatalf("Could not create replicationStorage: %s", err)
		}

		store2, err = storage.NewTracingStorageMiddleware("ReplicationStorage", store2)
		if err != nil {
			log.Fatalf("Error during TracingStorageMiddleware: %s", err)
		}

		store2, err = storage.NewPrometheusStorageMiddleware(store2, registry)
		if err != nil {
			log.Fatalf("Could not create prometheusStorageMiddleware: %s", err)
		}

		err = store2.Start(ctx)
		if err != nil {
			log.Fatalf("Couldn't start storage: %s", err)
		}

		closeStorage = func() {
			originalTs.Close()
			originalCloseStorage()
			store2.Stop(ctx)
		}
		closeDatabase = func() {
			originalCloseDatabase()
			db2.Close()
			err = os.RemoveAll(storagePath2)
			if err != nil {
				log.Fatalf("Could not remove storagePath %s: %s", storagePath2, err)
			}
		}
		ts = httptest.NewServer(server.SetupServer(accessKeyId, secretAccessKey, region, baseEndpoint, store2))
	}

	s3Client = setupS3Client(baseEndpoint, ts.Listener.Addr().String(), usePathStyle)

	cleanup = func() {
		ts.Close()
		closeStorage()
		closeDatabase()
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}
	return
}

func runTestsWithAllConfigurations(t *testing.T, testFunc func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool)) {
	for _, usePathStyle := range []bool{false, true} {
		hostOrPathStyleSuffix := ""
		if usePathStyle {
			hostOrPathStyleSuffix = " using path style"
		} else {
			hostOrPathStyleSuffix = " using host style"
		}
		for _, useReplication := range []bool{false, true} {
			replicationSuffix := hostOrPathStyleSuffix
			if useReplication {
				replicationSuffix += " replicated"
			}
			for _, useFilesystemBlobStore := range []bool{false, true} {
				blobStoreSuffix := replicationSuffix
				if useFilesystemBlobStore {
					blobStoreSuffix += " with filesystemBlobStore"
				} else {
					blobStoreSuffix += " with sqlBlobStore"
				}
				for _, encryptBlobStore := range []bool{false, true} {
					encryptBlobStoreSuffix := blobStoreSuffix
					if encryptBlobStore {
						encryptBlobStoreSuffix += " (encrypted)"
					}
					for _, wrapBlobStoreWithOutbox := range []bool{false, true} {
						testSuffix := encryptBlobStoreSuffix
						if wrapBlobStoreWithOutbox {
							testSuffix = encryptBlobStoreSuffix + " (using transactional outbox)"
						}
						testFunc(t, testSuffix, usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
					}
				}
			}
		}
	}
}

func TestCreateBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should create a bucket"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			assert.Equal(t, bucketName, createBucketResult.Location)
		})

		t.Run("it should not be able to create the same bucket twice"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
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
	})
}

func TestHeadBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should be able to see an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}

			assert.NotNil(t, createBucketResult)
			headBucketResult, err := s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			if err != nil {
				assert.Fail(t, "HeadBucket failed", "err %v", err)
			}
			assert.NotNil(t, headBucketResult)
		})
	})
}

func TestListBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should be able to list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listBucketsResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})

			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}

			assert.Len(t, listBucketsResult.Buckets, 1)
			assert.Equal(t, bucketName, listBucketsResult.Buckets[0].Name)
			assert.NotNil(t, listBucketsResult.Buckets[0].CreationDate)
			assert.True(t, listBucketsResult.Buckets[0].CreationDate.Before(time.Now()))
		})

		t.Run("it should list all buckets"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createBucketResult2, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName2,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult2)

			listBucketResult, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
			if err != nil {
				assert.Fail(t, "ListBuckets failed", "err %v", err)
			}
			assert.Len(t, listBucketResult.Buckets, 2)
		})
	})
}

func TestPutObject(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should allow uploading an object"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})

		t.Run("it should allow uploading an object a second time"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)
		})
	})
}

func TestMultipartUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should allow multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err = s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(1)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

		t.Run("it should allow multipart uploads with two parts"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

		t.Run("it should allow cancellation of multipart uploads"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUpload, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUpload)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should allow two multipart uploads on same key after complete first"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			completeMultipartUploadResult, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult)
			assert.Equal(t, bucketName, completeMultipartUploadResult.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult.Key)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

		t.Run("it should allow two multipart uploads on same key after abort first"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			createMultiPartUploadResult, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult.Key)
			uploadId := createMultiPartUploadResult.UploadId
			assert.NotNil(t, uploadId)

			uploadPartResult, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			uploadPartResult, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult)

			abortMultipartUploadResult, err := s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId,
			})
			if err != nil {
				assert.Fail(t, "AbortMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, abortMultipartUploadResult)

			createMultiPartUploadResult2, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket: bucketName,
				Key:    key,
			})

			if err != nil {
				assert.Fail(t, "CreateMultiPartUpload failed", "err %v", err)
			}
			assert.NotNil(t, createMultiPartUploadResult2)
			assert.Equal(t, *bucketName, *createMultiPartUploadResult2.Bucket)
			assert.Equal(t, *key, *createMultiPartUploadResult2.Key)
			uploadId2 := createMultiPartUploadResult2.UploadId
			assert.NotNil(t, uploadId2)

			uploadPartResult2, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[2:]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(2),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			uploadPartResult2, err = s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     bucketName,
				Body:       bytes.NewReader(body[0:2]),
				Key:        key,
				UploadId:   uploadId2,
				PartNumber: aws.Int32(1),
			})

			if err != nil {
				assert.Fail(t, "UploadPart failed", "err %v", err)
			}
			assert.NotNil(t, uploadPartResult2)

			completeMultipartUploadResult2, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:   bucketName,
				Key:      key,
				UploadId: uploadId2,
			})
			if err != nil {
				assert.Fail(t, "CompleteMultipartUpload failed", "err %v", err)
			}
			assert.NotNil(t, completeMultipartUploadResult2)
			assert.Equal(t, bucketName, completeMultipartUploadResult2.Bucket)
			assert.Equal(t, key, completeMultipartUploadResult2.Key)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)
			assert.Equal(t, key, listObjectResult.Contents[0].Key)
			assert.True(t, strings.HasSuffix(*completeMultipartUploadResult2.ETag, "-"+strconv.Itoa(2)+"\""))
			assert.Equal(t, completeMultipartUploadResult2.ETag, listObjectResult.Contents[0].ETag)
			assert.False(t, *listObjectResult.IsTruncated)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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
	})
}

func TestGetObject(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should allow downloading the object"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

		t.Run("it should allow downloading the object with byte range"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

		t.Run("it should allow downloading the object with byte range without end"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[1:], objectBytes)
		})

		t.Run("it should allow downloading the object with suffix byte range"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)
			assert.Equal(t, body[7:], objectBytes)
		})

		t.Run("it should allow downloading the object with multi byte range"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader(body),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			getObjectResult, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: bucketName,
				Key:    key,
				Range:  aws.String("bytes=1-4, 5-6"),
			})
			if err != nil {
				assert.Fail(t, "GetObject failed", "err %v", err)
			}
			assert.NotNil(t, getObjectResult)
			assert.NotNil(t, getObjectResult.ContentType)
			assert.Contains(t, *getObjectResult.ContentType, "multipart/byteranges; boundary=")
			boundary, _ := strings.CutPrefix(*getObjectResult.ContentType, "multipart/byteranges; boundary=")
			assert.NotNil(t, getObjectResult.Body)
			objectBytes, err := io.ReadAll(getObjectResult.Body)
			assert.Nil(t, err)

			newContent := []byte{}
			/*
				+ 00000000  2d 2d 30 31 48 58 45 52  50 59 56 4a 39 41 47 56  |--01HXERPYVJ9AGV|
				+ 00000010  44 42 34 57 4a 41 44 4a  43 56 59 4e 0d 0a 43 6f  |DB4WJADJCVYN..Co|
				+ 00000020  6e 74 65 6e 74 2d 52 61  6e 67 65 3a 20 62 79 74  |ntent-Range: byt|
				+ 00000030  65 73 20 31 2d 34 2f 31  33 0d 0a 0d 0a 65 6c 6c  |es 1-4/13....ell|
				+ 00000040  6f 0d 0a 2d 2d 30 31 48  58 45 52 50 59 56 4a 39  |o..--01HXERPYVJ9|
				+ 00000050  41 47 56 44 42 34 57 4a  41 44 4a 43 56 59 4e 0d  |AGVDB4WJADJCVYN.|
				+ 00000060  0a 43 6f 6e 74 65 6e 74  2d 52 61 6e 67 65 3a 20  |.Content-Range: |
				+ 00000070  62 79 74 65 73 20 35 2d  36 2f 31 33 0d 0a 0d 0a  |bytes 5-6/13....|
				+ 00000080  2c 20 0d 0a 2d 2d 30 31  48 58 45 52 50 59 56 4a  |, ..--01HXERPYVJ|
				+ 00000090  39 41 47 56 44 42 34 57  4a 41 44 4a 43 56 59 4e  |9AGVDB4WJADJCVYN|
				+ 000000a0  2d 2d 0d 0a                                       |--..|
			*/

			newContent = append(newContent, []byte("--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 1-4/13\r\n\r\n")...)
			newContent = append(newContent, body[1:5]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("\r\nContent-Range: bytes 5-6/13\r\n\r\n")...)
			newContent = append(newContent, body[5:7]...)
			newContent = append(newContent, []byte("\r\n--")...)
			newContent = append(newContent, []byte(boundary)...)
			newContent = append(newContent, []byte("--\r\n")...)
			assert.Equal(t, newContent, objectBytes)
		})
	})
}

func TestDeleteObject(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should allow deleting an object"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			deleteObjectResult, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: bucketName,
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "DeleteObject failed", "err %v", err)
			}
			assert.NotNil(t, deleteObjectResult)
		})
	})
}

func TestDeleteBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should delete an existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)
		})

		t.Run("it should fail when deleting non existing bucket"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			_, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
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

		t.Run("it should not see the bucket after deletion anymore"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			deleteBucketResult, err := s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "DeleteBucket failed", "err %v", err)
			}
			assert.NotNil(t, deleteBucketResult)

			_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
				Bucket: bucketName,
			})

			var notFoundError *types.NotFound
			if !errors.As(err, &notFoundError) {
				assert.Fail(t, "Expected aws error NotFound", "err %v", err)
			}
		})

		t.Run("it should not allow deleting a bucket with objects in it"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			_, err = s3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
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
	})
}

func TestListObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests")
	}

	t.Parallel()

	runTestsWithAllConfigurations(t, func(t *testing.T, testSuffix string, usePathStyle bool, useReplication bool, useFilesystemBlobStore bool, encryptBlobStore bool, wrapBlobStoreWithOutbox bool) {
		t.Run("it should list no objects"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}
			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list a single object"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list two objects"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 2)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			object2 := listObjectResult.Contents[1]
			assert.Equal(t, key2, object2.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object2.StorageClass)
			assert.Equal(t, int64(21), *object2.Size)
			assert.Equal(t, "\"72b52198921c896c2e7f5b3ef0ad42be\"", *object2.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should truncate when listing objects"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			maxKeys := int32(1)
			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  bucketName,
				MaxKeys: &maxKeys,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.True(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects starting with prefix my/test/key"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
				Prefix: keyPrefix,
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, *key, *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list no objects when searching for prefix key"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket: bucketName,
				Prefix: aws.String("key"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects with delimiter \"/\" one folder"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)
			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects with delimiter \"/\" two folders"+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    key2,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)
			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 0)
			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects with delimiter \"/\""+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    aws.String("my.txt"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)

			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, "my.txt", *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(21), *object.Size)
			assert.Equal(t, "\"72b52198921c896c2e7f5b3ef0ad42be\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects with prefix \"my/\" and delimiter \"/\""+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    aws.String("my.txt"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
				Prefix:    aws.String("my/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 1)

			commonPrefix := *listObjectResult.CommonPrefixes[0].Prefix
			assert.Equal(t, "my/test/", commonPrefix)
			assert.Len(t, listObjectResult.Contents, 0)

			assert.False(t, *listObjectResult.IsTruncated)
		})

		t.Run("it should list objects with prefix \"my/test/key\" and delimiter \"/\""+testSuffix, func(t *testing.T) {
			s3Client, cleanup := setupTestServer(usePathStyle, useReplication, useFilesystemBlobStore, encryptBlobStore, wrapBlobStoreWithOutbox)
			t.Cleanup(cleanup)
			createBucketResult, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
				Bucket: bucketName,
			})
			if err != nil {
				assert.Fail(t, "CreateBucket failed", "err %v", err)
			}
			assert.NotNil(t, createBucketResult)

			putObjectResult, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, first object!")),
				Key:    key,
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			putObjectResult, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: bucketName,
				Body:   bytes.NewReader([]byte("Hello, second object!")),
				Key:    aws.String("my.txt"),
			})
			if err != nil {
				assert.Fail(t, "PutObject failed", "err %v", err)
			}
			assert.NotNil(t, putObjectResult)

			listObjectResult, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:    bucketName,
				Delimiter: aws.String("/"),
				Prefix:    aws.String("my/test/key/"),
			})
			if err != nil {
				assert.Fail(t, "ListObjects failed", "err %v", err)
			}

			assert.Equal(t, bucketName, listObjectResult.Name)
			assert.Len(t, listObjectResult.CommonPrefixes, 0)

			assert.Len(t, listObjectResult.Contents, 1)

			object := listObjectResult.Contents[0]
			assert.Equal(t, "my/test/key/hello_world.txt", *object.Key)
			assert.Equal(t, types.ObjectStorageClassStandard, object.StorageClass)
			assert.Equal(t, int64(20), *object.Size)
			assert.Equal(t, "\"8e614ccc40d41a959c87067c6e8092a9\"", *object.ETag)

			assert.False(t, *listObjectResult.IsTruncated)
		})
	})
}
