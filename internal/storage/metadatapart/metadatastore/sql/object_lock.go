package sql

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
)

func (sms *sqlMetadataStore) LockBuckets(ctx context.Context, tx *sql.Tx, names ...metadatastore.BucketName) error {
	names = append([]metadatastore.BucketName(nil), names...)
	sort.Slice(names, func(i, j int) bool { return names[i].String() < names[j].String() })
	for i, name := range names {
		if i > 0 && name.String() == names[i-1].String() {
			continue
		}
		if err := sms.lockBucket(ctx, tx, name); err != nil {
			return err
		}
	}
	return nil
}

// Every metadata mutation acquires the bucket before reading mutable state.
// PostgreSQL row locks serialize activation, versioning changes, protection
// changes and deletion. SQLite already serializes writable transactions.
func (sms *sqlMetadataStore) lockBucket(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName) error {
	return sms.objectLockRepository.LockBucket(ctx, tx, name)
}

func (sms *sqlMetadataStore) loadLockConfiguration(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName) (*metadatastore.ObjectLockConfiguration, error) {
	return sms.objectLockRepository.FindBucketConfiguration(ctx, tx, name)
}

func (sms *sqlMetadataStore) GetObjectLockConfiguration(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName) (*metadatastore.ObjectLockConfiguration, error) {
	if _, err := sms.HeadBucket(ctx, tx, name); err != nil {
		return nil, err
	}
	config, err := sms.loadLockConfiguration(ctx, tx, name)
	if err == nil && config == nil {
		err = metadatastore.ErrObjectLockConfigurationNotFound
	}
	return config, err
}

func (sms *sqlMetadataStore) PutObjectLockConfiguration(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, config *metadatastore.ObjectLockConfiguration) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if err := sms.lockBucket(ctx, tx, name); err != nil {
		return err
	}
	err := sms.objectLockRepository.SaveBucketConfiguration(ctx, tx, name, config)
	if err == nil {
		metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Configuration: config})
	}
	return err
}

func (sms *sqlMetadataStore) loadObjectLock(ctx context.Context, tx *sql.Tx, id ulid.ULID) (metadatastore.ObjectLock, error) {
	return sms.objectLockRepository.FindObjectLock(ctx, tx, id)
}

func (sms *sqlMetadataStore) saveObjectLock(ctx context.Context, tx *sql.Tx, id ulid.ULID, lock metadatastore.ObjectLock) error {
	return sms.objectLockRepository.SaveObjectLock(ctx, tx, id, lock)
}

func (sms *sqlMetadataStore) lockTarget(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectLockOptions, write bool) (*object.Entity, metadatastore.ObjectLock, error) {
	empty := metadatastore.ObjectLock{}
	if write {
		if err := sms.lockBucket(ctx, tx, name); err != nil {
			return nil, empty, err
		}
	}
	if _, err := sms.GetObjectLockConfiguration(ctx, tx, name); err != nil {
		return nil, empty, err
	}
	var entity *object.Entity
	var err error
	if opts != nil && opts.VersionID != nil {
		entity, err = sms.objectRepository.FindObjectByBucketNameAndKeyAndVersionID(ctx, tx, name, key, *opts.VersionID)
	} else {
		entity, err = sms.objectRepository.FindObjectByBucketNameAndKey(ctx, tx, name, key)
	}
	if err != nil {
		return nil, empty, err
	}
	if entity == nil {
		return nil, empty, metadatastore.ErrNoSuchKey
	}
	if entity.IsDeleteMarker {
		return nil, empty, metadatastore.ErrObjectLockMethodNotAllowed
	}
	if write {
		if err := sms.objectLockRepository.LockObject(ctx, tx, *entity.Id); err != nil {
			return nil, empty, err
		}
	}

	lock, err := sms.loadObjectLock(ctx, tx, *entity.Id)
	return entity, lock, err
}

func (sms *sqlMetadataStore) GetObjectRetention(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectLockOptions) (*metadatastore.ObjectRetention, error) {
	entity, lock, err := sms.lockTarget(ctx, tx, name, key, opts, false)
	if err == nil {
		metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: key.String(), VersionID: entity.VersionID, Effective: lock})
	}
	return lock.Retention, err
}

func (sms *sqlMetadataStore) PutObjectRetention(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, key metadatastore.ObjectKey, retention *metadatastore.ObjectRetention, opts *metadatastore.ObjectLockOptions) error {
	entity, lock, err := sms.lockTarget(ctx, tx, name, key, opts, true)
	if err != nil {
		return err
	}
	retention = metadatastore.NormalizeObjectRetention(retention)
	used, err := lock.CheckRetentionChange(retention, time.Now(), opts != nil && opts.BypassGovernanceRetention)
	if err == nil {
		lock.Retention = retention
		err = sms.saveObjectLock(ctx, tx, *entity.Id, lock)
		if err == nil {
			lock, err = sms.loadObjectLock(ctx, tx, *entity.Id)
		}
	}
	metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: entity.Key.String(), VersionID: entity.VersionID, Effective: lock, BypassUsed: used, Err: err})
	return err
}

func (sms *sqlMetadataStore) GetObjectLegalHold(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, key metadatastore.ObjectKey, opts *metadatastore.ObjectLockOptions) (*metadatastore.LegalHoldStatus, error) {
	entity, lock, err := sms.lockTarget(ctx, tx, name, key, opts, false)
	if err == nil {
		metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: key.String(), VersionID: entity.VersionID, Effective: lock})
	}
	return lock.LegalHold, err
}

func (sms *sqlMetadataStore) PutObjectLegalHold(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, key metadatastore.ObjectKey, status metadatastore.LegalHoldStatus, opts *metadatastore.ObjectLockOptions) error {
	if !status.Valid() {
		return metadatastore.ErrInvalidObjectLockConfiguration
	}
	entity, lock, err := sms.lockTarget(ctx, tx, name, key, opts, true)
	if err != nil {
		return err
	}
	lock.LegalHold = &status
	err = sms.saveObjectLock(ctx, tx, *entity.Id, lock)
	metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: entity.Key.String(), VersionID: entity.VersionID, Effective: lock, Err: err})
	return err
}

func (sms *sqlMetadataStore) checkVersionDeletion(ctx context.Context, tx *sql.Tx, entity *object.Entity, bypass bool) error {
	if entity != nil && entity.IsDeleteMarker {
		metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: entity.Key.String(), VersionID: entity.VersionID})
		return nil
	}
	if entity == nil || entity.UploadStatus != object.UploadStatusCompleted {
		return nil
	}
	lock, err := sms.loadObjectLock(ctx, tx, *entity.Id)
	if err != nil {
		return err
	}
	used, err := lock.CheckDelete(time.Now(), bypass)
	metadatastore.ObserveObjectLock(ctx, metadatastore.ObjectLockObservation{Key: entity.Key.String(), VersionID: entity.VersionID, Effective: lock, BypassUsed: used, Err: err})
	return err
}

func (sms *sqlMetadataStore) effectiveLock(ctx context.Context, tx *sql.Tx, name metadatastore.BucketName, requested metadatastore.ObjectLock) (metadatastore.ObjectLock, error) {
	config, err := sms.loadLockConfiguration(ctx, tx, name)
	if err != nil {
		return metadatastore.ObjectLock{}, err
	}
	return metadatastore.EffectiveObjectLock(config, requested, time.Now().UTC())
}
