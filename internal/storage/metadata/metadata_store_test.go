package metadata

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	"github.com/oklog/ulid/v2"
)

func MetadataStoreTester(metadataStore MetadataStore, db *sql.DB) error {
	ctx := context.Background()
	err := metadataStore.Start(ctx)
	if err != nil {
		return err
	}
	defer metadataStore.Stop(ctx)

	bucketName := "bucket"
	key := "test"

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.CreateBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	bucket, err := metadataStore.HeadBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if bucketName != bucket.Name {
		return errors.New("invalid bucketName")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	buckets, err := metadataStore.ListBuckets(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(buckets) != 1 {
		return errors.New("expected 1 bucket got " + strconv.Itoa(len(buckets)))
	}

	if bucketName != buckets[0].Name {
		return errors.New("invalid bucketName")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.PutObject(ctx, tx, bucketName, &Object{
		Key:          key,
		LastModified: time.Now(),
		ETag:         "",
		Size:         0,
		Blobs:        []Blob{},
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	object, err := metadataStore.HeadObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(object.Blobs) != 0 {
		return errors.New("invalid blob length")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	listBucketResult, err := metadataStore.ListObjects(ctx, tx, bucketName, "", "", "", 1000)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(listBucketResult.Objects) != 1 {
		return errors.New("invalid objects length")
	}

	if key != listBucketResult.Objects[0].Key {
		return errors.New("invalid object key")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.DeleteObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	initiateMultipartUploadResult, err := metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   blobstore.BlobId(ulid.Make()),
		Size: 0,
		ETag: "",
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	_, err = metadataStore.CompleteMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.DeleteObject(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	initiateMultipartUploadResult, err = metadataStore.CreateMultipartUpload(ctx, tx, bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.UploadPart(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId, 1, Blob{
		Id:   blobstore.BlobId(ulid.Make()),
		Size: 0,
		ETag: "",
	})
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	_, err = metadataStore.AbortMultipartUpload(ctx, tx, bucketName, key, initiateMultipartUploadResult.UploadId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = metadataStore.DeleteBucket(ctx, tx, bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	buckets, err = metadataStore.ListBuckets(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if len(buckets) != 0 {
		return errors.New("expected 0 bucket got " + strconv.Itoa(len(buckets)))
	}

	return nil
}
