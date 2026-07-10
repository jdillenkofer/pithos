package metadatapart

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func taggingMetaOptions(opts *storage.ObjectTaggingOptions) *metadatastore.ObjectTaggingOptions {
	if opts == nil {
		return nil
	}
	return &metadatastore.ObjectTaggingOptions{VersionID: opts.VersionID}
}

func (mbs *metadataPartStorage) validateTaggingTarget(ctx context.Context, tx database.Tx, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	var object *metadatastore.Object
	var err error
	if opts != nil && opts.VersionID != nil {
		object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
	} else {
		object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
	}
	if err != nil {
		return err
	}
	if !object.IsDeleteMarker {
		return nil
	}
	versionID := ""
	if object.VersionID != nil {
		versionID = *object.VersionID
	}
	if opts != nil && opts.VersionID != nil {
		return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: object.LastModified}
	}
	return &storage.CurrentDeleteMarkerError{VersionID: versionID}
}

func (mbs *metadataPartStorage) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetObjectTagging")
	defer span.End()

	var tags map[string]string
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		if err := mbs.validateTaggingTarget(ctx, tx, bucketName, key, opts); err != nil {
			return err
		}
		metaOpts := taggingMetaOptions(opts)
		tags, err = mbs.metadataStore.GetObjectTagging(ctx, tx.SqlTx(), bucketName, key, metaOpts)
		return err
	})
	if err != nil {
		return nil, err
	}

	return tags, nil
}

func (mbs *metadataPartStorage) PutObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, tags map[string]string, opts *storage.ObjectTaggingOptions) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutObjectTagging")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if err := mbs.validateTaggingTarget(ctx, tx, bucketName, key, opts); err != nil {
			return err
		}
		metaOpts := taggingMetaOptions(opts)
		return mbs.metadataStore.PutObjectTagging(ctx, tx.SqlTx(), bucketName, key, tags, metaOpts)
	})
}

func (mbs *metadataPartStorage) DeleteObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteObjectTagging")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if err := mbs.validateTaggingTarget(ctx, tx, bucketName, key, opts); err != nil {
			return err
		}
		metaOpts := taggingMetaOptions(opts)
		return mbs.metadataStore.DeleteObjectTagging(ctx, tx.SqlTx(), bucketName, key, metaOpts)
	})
}
