package storage

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/jdillenkofer/pithos/internal/ioutils"
)

func StorageTester(storage Storage, content []byte) error {
	ctx := context.Background()
	err := storage.Start(ctx)
	if err != nil {
		return err
	}
	defer storage.Stop(ctx)

	bucketName := "bucket"
	key := "test"
	data := ioutils.NewByteReadSeekCloser(content)

	err = storage.CreateBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	bucket, err := storage.HeadBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	if bucketName != bucket.Name {
		return errors.New("invalid bucketName")
	}

	buckets, err := storage.ListBuckets(ctx)
	if err != nil {
		return err
	}

	if len(buckets) != 1 {
		return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
	}

	if bucketName != buckets[0].Name {
		return errors.New("invalid bucketName")
	}

	err = storage.PutObject(ctx, bucketName, key, data)
	if err != nil {
		return err
	}

	object, err := storage.HeadObject(ctx, bucketName, key)
	if err != nil {
		return err
	}

	if object.Size != int64(len(content)) {
		return errors.New("invalid blob length")
	}

	listBucketResult, err := storage.ListObjects(ctx, bucketName, "", "", "", 1000)
	if err != nil {
		return err
	}

	if len(listBucketResult.Objects) != 1 {
		return errors.New("invalid objects length")
	}

	if key != listBucketResult.Objects[0].Key {
		return errors.New("invalid object key")
	}

	err = storage.DeleteObject(ctx, bucketName, key)
	if err != nil {
		return err
	}

	initiateMultipartUploadResult, err := storage.CreateMultipartUpload(ctx, bucketName, key)
	if err != nil {
		return err
	}

	_, err = data.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data)
	if err != nil {
		return err
	}

	_, err = storage.CompleteMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId)
	if err != nil {
		return err
	}

	err = storage.DeleteObject(ctx, bucketName, key)
	if err != nil {
		return err
	}

	initiateMultipartUploadResult, err = storage.CreateMultipartUpload(ctx, bucketName, key)
	if err != nil {
		return err
	}

	_, err = data.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = storage.UploadPart(ctx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, data)
	if err != nil {
		return err
	}

	err = storage.AbortMultipartUpload(ctx, bucketName, key, initiateMultipartUploadResult.UploadId)
	if err != nil {
		return err
	}

	err = storage.DeleteBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	buckets, err = storage.ListBuckets(ctx)
	if err != nil {
		return err
	}

	if len(buckets) != 0 {
		return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
	}

	return nil
}
