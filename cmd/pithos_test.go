package main

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	server "github.com/jdillenkofer/pithos/internal/server"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/jdillenkofer/pithos/internal/storage/metadata"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func runTestServer(storage storage.Storage) *httptest.Server {
	return httptest.NewServer(server.SetupServer(storage))
}

func createS3Client(ts *httptest.Server) *s3.S3 {
	config := aws.NewConfig().WithS3ForcePathStyle(true).WithRegion("eu-central-1").WithEndpoint(ts.URL).WithCredentials(credentials.AnonymousCredentials)
	session := session.Must(session.NewSession(config))
	s3Client := s3.New(session)
	return s3Client
}

func Test_BasicBucketOperations(t *testing.T) {
	storagePath := "../data"
	db, err := sql.Open("sqlite3", filepath.Join(storagePath, "metadata.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	metadataStore, err := metadata.NewSqlMetadataStore(db)
	if err != nil {
		log.Fatal(err)
	}
	blobStore, err := blob.NewFilesystemBlobStore(filepath.Join(storagePath, "blobs"))
	if err != nil {
		log.Fatal(err)
	}
	storage, err := storage.NewMetadataBlobStorage(metadataStore, blobStore)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	clearStorage := func() {
		err := storage.Clear()
		if err != nil {
			log.Fatal(err)
		}
	}
	ts := runTestServer(storage)
	defer ts.Close()

	bucketName := aws.String("test")
	bucketName2 := aws.String("test2")
	keyPrefix := aws.String("my/test/key")
	key := aws.String(*keyPrefix + "/hello_world.txt")
	body := []byte("Hello, world!")
	s3Client := createS3Client(ts)

	t.Run("it should create a bucket", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		assert.Equal(t, bucketName, createBucketResult.Location)
	})

	t.Run("it should not be able to create the same bucket twice", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})

		if err == nil {
			assert.Fail(t, "CreateBucket should failed when reusing the same bucket name")
		}

		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != "BucketAlreadyExists" {
			assert.Fail(t, "Expected aws error BucketAlreadyExists", "err %v", err)
		}
	})

	t.Run("it should be able to see an existing bucket", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}

		assert.NotNil(t, createBucketResult)
		headBucketResult, err := s3Client.HeadBucket(&s3.HeadBucketInput{
			Bucket: bucketName,
		})

		if err != nil {
			assert.Fail(t, "HeadBucket failed", "err %v", err)
		}
		assert.NotNil(t, headBucketResult)
	})

	t.Run("it should be able to list all buckets", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		listBucketsResult, err := s3Client.ListBuckets(&s3.ListBucketsInput{})

		if err != nil {
			assert.Fail(t, "ListBuckets failed", "err %v", err)
		}

		assert.Len(t, listBucketsResult.Buckets, 1)
		assert.Equal(t, bucketName, listBucketsResult.Buckets[0].Name)
		assert.NotNil(t, listBucketsResult.Buckets[0].CreationDate)
		assert.True(t, listBucketsResult.Buckets[0].CreationDate.Before(time.Now()))
	})

	t.Run("it should allow uploading an object", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)
	})

	t.Run("it should not allow deleting a bucket with objects in it", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: bucketName,
		})
		if err == nil {
			assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
		}

		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != "BucketNotEmpty" {
			assert.Fail(t, "Expected aws error BucketNotEmpty", "err %v", err)
		}
	})

	t.Run("it should allow uploading an object a second time", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		putObjectResult, err = s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader(body),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)
	})

	t.Run("it should allow downloading the object again", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader(body),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		getObjectResult, err := s3Client.GetObject(&s3.GetObjectInput{
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

	t.Run("it should allow deleting an object", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		deleteObjectResult, err := s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: bucketName,
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "DeleteObject failed", "err %v", err)
		}
		assert.NotNil(t, deleteObjectResult)
	})

	t.Run("it should delete an existing bucket", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		deleteBucketResult, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "DeleteBucket failed", "err %v", err)
		}
		assert.NotNil(t, deleteBucketResult)
	})

	t.Run("it should fail when deleting non existing bucket", func(t *testing.T) {
		_, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String("test2"),
		})

		if err == nil {
			assert.Fail(t, "DeleteBucket should fail when using non existing bucket name")
		}

		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != "NoSuchBucket" {
			assert.Fail(t, "Expected aws error NoSuchBucket", "err %v", err)
		}
	})

	t.Run("it should not see the bucket after deletion anymore", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		deleteBucketResult, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "DeleteBucket failed", "err %v", err)
		}
		assert.NotNil(t, deleteBucketResult)

		_, err = s3Client.HeadBucket(&s3.HeadBucketInput{
			Bucket: bucketName,
		})
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != "NotFound" {
			assert.Fail(t, "Expected aws error NotFound", "err %v", err)
		}
	})

	t.Run("it should list all buckets", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		createBucketResult2, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName2,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult2)

		listBucketResult, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			assert.Fail(t, "ListBuckets failed", "err %v", err)
		}
		assert.Len(t, listBucketResult.Buckets, 2)
	})

	t.Run("it should list all objects", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		listObjectResult, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "ListObjects failed", "err %v", err)
		}
		assert.Len(t, listObjectResult.Contents, 1)
	})

	t.Run("it should list objects starting with prefix \"my/test/key\"", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		listObjectResult, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: bucketName,
			Prefix: keyPrefix,
		})
		if err != nil {
			assert.Fail(t, "ListObjects failed", "err %v", err)
		}
		assert.Len(t, listObjectResult.Contents, 1)
	})

	t.Run("it should list no objects when searching for prefix \"key\"", func(t *testing.T) {
		t.Cleanup(clearStorage)
		createBucketResult, err := s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: bucketName,
		})
		if err != nil {
			assert.Fail(t, "CreateBucket failed", "err %v", err)
		}
		assert.NotNil(t, createBucketResult)

		putObjectResult, err := s3Client.PutObject(&s3.PutObjectInput{
			Bucket: bucketName,
			Body:   bytes.NewReader([]byte("Hello, first object!")),
			Key:    key,
		})
		if err != nil {
			assert.Fail(t, "PutObject failed", "err %v", err)
		}
		assert.NotNil(t, putObjectResult)

		listObjectResult, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: bucketName,
			Prefix: aws.String("key"),
		})
		if err != nil {
			assert.Fail(t, "ListObjects failed", "err %v", err)
		}
		assert.Len(t, listObjectResult.Contents, 0)
	})
}
