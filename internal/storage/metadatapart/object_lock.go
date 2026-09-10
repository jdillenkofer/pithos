package metadatapart

import (
	"context"
	"database/sql"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

func (mbs *metadataPartStorage) GetObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	var result *storage.ObjectLockConfiguration
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		result, err = mbs.metadataStore.GetObjectLockConfiguration(ctx, tx.SqlTx(), bucketName)
		return err
	})
	return result, err
}
func (mbs *metadataPartStorage) PutObjectLockConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.ObjectLockConfiguration) error {
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutObjectLockConfiguration(ctx, tx.SqlTx(), bucketName, config)
	})
}
func (mbs *metadataPartStorage) GetObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.ObjectRetention, error) {
	var result *storage.ObjectRetention
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		result, err = mbs.metadataStore.GetObjectRetention(ctx, tx.SqlTx(), bucketName, key, opts)
		return err
	})
	return result, err
}
func (mbs *metadataPartStorage) PutObjectRetention(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, retention *storage.ObjectRetention, opts *storage.ObjectLockOptions) error {
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutObjectRetention(ctx, tx.SqlTx(), bucketName, key, retention, opts)
	})
}
func (mbs *metadataPartStorage) GetObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectLockOptions) (*storage.LegalHoldStatus, error) {
	var result *storage.LegalHoldStatus
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		result, err = mbs.metadataStore.GetObjectLegalHold(ctx, tx.SqlTx(), bucketName, key, opts)
		return err
	})
	return result, err
}
func (mbs *metadataPartStorage) PutObjectLegalHold(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, status storage.LegalHoldStatus, opts *storage.ObjectLockOptions) error {
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.PutObjectLegalHold(ctx, tx.SqlTx(), bucketName, key, status, opts)
	})
}
