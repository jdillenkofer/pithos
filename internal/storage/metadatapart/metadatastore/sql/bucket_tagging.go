package sql

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/buckettag"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) requireBucketForTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}
	return nil
}

func (sms *sqlMetadataStore) GetBucketTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (map[string]string, error) {
	if err := sms.requireBucketForTagging(ctx, tx, bucketName); err != nil {
		return nil, err
	}
	entities, err := sms.bucketTagRepository.FindTagsByBucketNameOrderByKeyAsc(ctx, tx, bucketName.String())
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string)
	for _, tag := range entities {
		tags[tag.Key] = tag.Value
	}
	return tags, nil
}

func (sms *sqlMetadataStore) PutBucketTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, tags map[string]string) error {
	if err := sms.requireBucketForTagging(ctx, tx, bucketName); err != nil {
		return err
	}
	if err := sms.bucketTagRepository.DeleteTagsByBucketName(ctx, tx, bucketName.String()); err != nil {
		return err
	}
	for key, value := range tags {
		if err := sms.bucketTagRepository.SaveTag(ctx, tx, buckettag.Entity{BucketName: bucketName.String(), Key: key, Value: value}); err != nil {
			return err
		}
	}
	return nil
}

func (sms *sqlMetadataStore) DeleteBucketTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	if err := sms.requireBucketForTagging(ctx, tx, bucketName); err != nil {
		return err
	}
	return sms.bucketTagRepository.DeleteTagsByBucketName(ctx, tx, bucketName.String())
}
