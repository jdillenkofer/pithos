package sql

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) findObjectForTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectTaggingOptions) (*object.Entity, error) {
	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if !*exists {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if opts != nil && opts.VersionID != nil {
		return sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, bucketName, key, *opts.VersionID)
	}
	return sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, bucketName, key)
}

func (sms *sqlMetadataStore) GetObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectTaggingOptions) (map[string]string, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetObjectTagging")
	defer span.End()

	objectEntity, err := sms.findObjectForTagging(ctx, tx, bucketName, key, opts)
	if err != nil {
		return nil, err
	}
	if objectEntity == nil {
		return nil, metadatastore.ErrNoSuchKey
	}

	return sms.loadObjectTags(ctx, tx, *objectEntity.Id)
}

func (sms *sqlMetadataStore) PutObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, tags map[string]string, opts *metadatastore.ObjectTaggingOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutObjectTagging")
	defer span.End()

	objectEntity, err := sms.findObjectForTagging(ctx, tx, bucketName, key, opts)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	if err := sms.replaceObjectTags(ctx, tx, *objectEntity.Id, tags); err != nil {
		return err
	}

	// Bump the object's updated_at / optimistic lock version so the tag change
	// is reflected in the object metadata and concurrent writers are detected.
	return sms.objectRepository.SaveObject(ctx, tx, objectEntity)
}

func (sms *sqlMetadataStore) DeleteObjectTagging(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectTaggingOptions) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteObjectTagging")
	defer span.End()

	objectEntity, err := sms.findObjectForTagging(ctx, tx, bucketName, key, opts)
	if err != nil {
		return err
	}
	if objectEntity == nil {
		return metadatastore.ErrNoSuchKey
	}

	if err := sms.tagRepository.DeleteTagsByObjectId(ctx, tx, *objectEntity.Id); err != nil {
		return err
	}

	return sms.objectRepository.SaveObject(ctx, tx, objectEntity)
}
