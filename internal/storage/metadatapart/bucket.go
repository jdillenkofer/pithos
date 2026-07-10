package metadatapart

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (mbs *metadataPartStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CreateBucket")
	defer span.End()
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucketName)
	})
}

func (mbs *metadataPartStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteBucket")
	defer span.End()
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.DeleteBucket(ctx, tx.SqlTx(), bucketName)
	})
}

func convertBucket(mBucket metadatastore.Bucket) storage.Bucket {
	return storage.Bucket{
		Name:         mBucket.Name,
		CreationDate: mBucket.CreationDate,
	}
}

func (mbs *metadataPartStorage) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListBuckets")
	defer span.End()

	var mBuckets []metadatastore.Bucket
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mBuckets, err = mbs.metadataStore.ListBuckets(ctx, tx.SqlTx())
		return err
	})
	if err != nil {
		return nil, err
	}

	return sliceutils.Map(convertBucket, mBuckets), nil
}

func (mbs *metadataPartStorage) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.HeadBucket")
	defer span.End()

	var mBucket *metadatastore.Bucket
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mBucket, err = mbs.metadataStore.HeadBucket(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	b := convertBucket(*mBucket)
	return &b, err
}

func (mbs *metadataPartStorage) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetBucketWebsiteConfiguration")
	defer span.End()

	var config *storage.WebsiteConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		config, err = mbs.metadataStore.GetBucketWebsiteConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (mbs *metadataPartStorage) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutBucketWebsiteConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketWebsiteConfiguration(ctx, tx.SqlTx(), bucketName, config)
	})
}

func (mbs *metadataPartStorage) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteBucketWebsiteConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.DeleteBucketWebsiteConfiguration(ctx, tx.SqlTx(), bucketName)
	})
}

func (mbs *metadataPartStorage) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetBucketCORSConfiguration")
	defer span.End()

	var config *storage.BucketCORSConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		config, err = mbs.metadataStore.GetBucketCORSConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (mbs *metadataPartStorage) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutBucketCORSConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketCORSConfiguration(ctx, tx.SqlTx(), bucketName, config)
	})
}

func (mbs *metadataPartStorage) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteBucketCORSConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.DeleteBucketCORSConfiguration(ctx, tx.SqlTx(), bucketName)
	})
}

func (mbs *metadataPartStorage) GetBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetBucketLifecycleConfiguration")
	defer span.End()

	var config *storage.BucketLifecycleConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		config, err = mbs.metadataStore.GetBucketLifecycleConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (mbs *metadataPartStorage) PutBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketLifecycleConfiguration) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutBucketLifecycleConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketLifecycleConfiguration(ctx, tx.SqlTx(), bucketName, config)
	})
}

func (mbs *metadataPartStorage) DeleteBucketLifecycleConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteBucketLifecycleConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.DeleteBucketLifecycleConfiguration(ctx, tx.SqlTx(), bucketName)
	})
}

func (mbs *metadataPartStorage) GetBucketNotificationConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketNotificationConfiguration, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetBucketNotificationConfiguration")
	defer span.End()

	var config *storage.BucketNotificationConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		config, err = mbs.metadataStore.GetBucketNotificationConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (mbs *metadataPartStorage) PutBucketNotificationConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketNotificationConfiguration) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutBucketNotificationConfiguration")
	defer span.End()

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketNotificationConfiguration(ctx, tx.SqlTx(), bucketName, config)
	})
}

func (mbs *metadataPartStorage) GetBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetBucketVersioningConfiguration")
	defer span.End()

	var config *metadatastore.BucketVersioningConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		config, err = mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	if config.Status == nil {
		return &storage.BucketVersioningConfiguration{}, nil
	}
	status := storage.BucketVersioningStatus(*config.Status)
	return &storage.BucketVersioningConfiguration{Status: &status}, nil
}

func (mbs *metadataPartStorage) PutBucketVersioningConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketVersioningConfiguration) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutBucketVersioningConfiguration")
	defer span.End()

	metaConfig := &metadatastore.BucketVersioningConfiguration{}
	if config != nil && config.Status != nil {
		status := metadatastore.BucketVersioningStatus(*config.Status)
		metaConfig.Status = &status
	}

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName, metaConfig)
	})
}
