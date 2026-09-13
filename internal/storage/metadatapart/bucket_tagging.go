package metadatapart

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

func (mbs *metadataPartStorage) GetBucketTagging(ctx context.Context, bucketName storage.BucketName) (map[string]string, error) {
	var tags map[string]string
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		tags, err = mbs.metadataStore.GetBucketTagging(ctx, tx.SqlTx(), bucketName)
		return err
	})
	return tags, err
}

func (mbs *metadataPartStorage) PutBucketTagging(ctx context.Context, bucketName storage.BucketName, tags map[string]string) error {
	return database.WithTx(ctx, mbs.db, nil, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutBucketTagging(ctx, tx.SqlTx(), bucketName, tags)
	})
}

func (mbs *metadataPartStorage) DeleteBucketTagging(ctx context.Context, bucketName storage.BucketName) error {
	return database.WithTx(ctx, mbs.db, nil, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.DeleteBucketTagging(ctx, tx.SqlTx(), bucketName)
	})
}
