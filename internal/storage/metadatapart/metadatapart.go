package metadatapart

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/gc"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/task"
)

type partRange struct {
	id    partstore.PartId
	store partstore.PartStore
	skip  int64
	limit *int64
}

type lazyPartSequenceReadCloser struct {
	ctx   context.Context
	tx    database.Tx
	parts []partRange

	partIndex int
	current   io.ReadCloser
	closed    bool
}

func (l *lazyPartSequenceReadCloser) openNextPart() error {
	for l.partIndex < len(l.parts) {
		part := l.parts[l.partIndex]
		l.partIndex++

		rc, err := part.store.GetPart(l.ctx, l.tx, part.id)
		if err != nil {
			return err
		}

		if part.skip > 0 {
			if _, err := ioutils.SkipNBytes(rc, part.skip); err != nil {
				rc.Close()
				return err
			}
		}

		if part.limit != nil {
			rc = ioutils.NewLimitedEndReadCloser(rc, *part.limit)
		}

		l.current = rc
		return nil
	}

	return io.EOF
}

func (l *lazyPartSequenceReadCloser) Read(p []byte) (int, error) {
	if l.closed {
		return 0, io.EOF
	}

	for {
		if l.current == nil {
			if err := l.openNextPart(); err != nil {
				if err == io.EOF {
					return 0, io.EOF
				}
				return 0, err
			}
		}

		n, err := l.current.Read(p)
		if err == io.EOF {
			_ = l.current.Close()
			l.current = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (l *lazyPartSequenceReadCloser) Close() error {
	l.closed = true
	if l.current != nil {
		err := l.current.Close()
		l.current = nil
		return err
	}
	return nil
}

type metadataPartStorage struct {
	*lifecycle.ValidatedLifecycle
	db            database.Database
	metadataStore metadatastore.MetadataStore
	partStores    *partstore.NamedPartStores
	partGC        gc.PartGarbageCollector
	gcTaskHandle  *task.TaskHandle
	tracer        trace.Tracer
}

// Compile-time check to ensure metadataPartStorage implements storage.Storage
var _ storage.Storage = (*metadataPartStorage)(nil)
var _ storage.TransactionalStorage = (*metadataPartStorage)(nil)

func NewStorage(db database.Database, metadataStore metadatastore.MetadataStore, partStore partstore.PartStore) (storage.Storage, error) {
	return NewStorageWithNamedPartStores(db, metadataStore, partStore, nil, nil)
}

// NewStorageWithNamedPartStores builds a storage whose part data is spread
// over named part stores: writes route to the store mapped from the object's
// storage class (falling back to defaultPartStore), reads resolve the store
// recorded per part.
func NewStorageWithNamedPartStores(db database.Database, metadataStore metadatastore.MetadataStore, defaultPartStore partstore.PartStore, extraPartStores map[string]partstore.PartStore, storageClassToPartStore map[string]string) (storage.Storage, error) {
	for storageClass := range storageClassToPartStore {
		if !metadatastore.IsValidStorageClass(storageClass) {
			return nil, fmt.Errorf("storage class %q in part store mapping is not a recognized storage class", storageClass)
		}
	}
	partStores, err := partstore.NewNamedPartStores(defaultPartStore, extraPartStores, storageClassToPartStore)
	if err != nil {
		return nil, err
	}
	lifecycle, err := lifecycle.NewValidatedLifecycle("MetadataPartStorage")
	if err != nil {
		return nil, err
	}
	partGC, err := gc.New(db, metadataStore, partStores)
	if err != nil {
		return nil, err
	}
	return &metadataPartStorage{
		ValidatedLifecycle: lifecycle,
		db:                 db,
		metadataStore:      metadataStore,
		partStores:         partStores,
		partGC:             partGC,
		gcTaskHandle:       nil,
		tracer:             otel.Tracer("internal/storage/metadatapart"),
	}, nil
}

func (mbs *metadataPartStorage) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return database.WithTx(ctx, mbs.db, opts, func(ctx context.Context, tx database.Tx) error {
		return fn(ctx, mbs)
	})
}

func (mbs *metadataPartStorage) Start(ctx context.Context) error {
	if err := mbs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := mbs.metadataStore.Start(ctx); err != nil {
		return err
	}
	if err := mbs.partStores.Start(ctx); err != nil {
		return err
	}

	mbs.gcTaskHandle = task.Start(mbs.partGC.RunGCLoop)

	return nil
}

func (mbs *metadataPartStorage) Stop(ctx context.Context) error {
	if err := mbs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	slog.Debug("Stopping GCLoop task")
	if mbs.gcTaskHandle != nil {
		mbs.gcTaskHandle.Cancel()
		joinedWithTimeout := mbs.gcTaskHandle.JoinWithTimeout(30 * time.Second)
		if joinedWithTimeout {
			slog.Debug("GCLoop joined with timeout of 30s")
		} else {
			slog.Debug("GCLoop joined without timeout")
		}
	}
	if err := mbs.metadataStore.Stop(ctx); err != nil {
		return err
	}
	if err := mbs.partStores.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (mbs *metadataPartStorage) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CreateBucket")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return mbs.metadataStore.CreateBucket(ctx, tx.SqlTx(), bucketName)
	})
}

func (mbs *metadataPartStorage) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteBucket")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
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

func convertObject(mObject metadatastore.Object) storage.Object {
	return storage.Object{
		Key:               mObject.Key,
		ContentType:       mObject.ContentType,
		LastModified:      mObject.LastModified,
		VersionID:         mObject.VersionID,
		IsDeleteMarker:    mObject.IsDeleteMarker,
		ETag:              mObject.ETag,
		ChecksumCRC32:     mObject.ChecksumCRC32,
		ChecksumCRC32C:    mObject.ChecksumCRC32C,
		ChecksumCRC64NVME: mObject.ChecksumCRC64NVME,
		ChecksumSHA1:      mObject.ChecksumSHA1,
		ChecksumSHA256:    mObject.ChecksumSHA256,
		ChecksumType:      mObject.ChecksumType,
		Size:              mObject.Size,
		StorageClass:      mObject.StorageClass,
		Tags:              mObject.Tags,
		Metadata:          mObject.Metadata,
	}
}

func convertListBucketResult(mListBucketResult metadatastore.ListBucketResult) storage.ListBucketResult {
	return storage.ListBucketResult{
		Objects:        sliceutils.Map(convertObject, mListBucketResult.Objects),
		CommonPrefixes: mListBucketResult.CommonPrefixes,
		IsTruncated:    mListBucketResult.IsTruncated,
	}
}

func (mbs *metadataPartStorage) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListObjects")
	defer span.End()

	var mListBucketResult *metadatastore.ListBucketResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListBucketResult, err = mbs.metadataStore.ListObjects(ctx, tx.SqlTx(), bucketName, metadatastore.ListObjectsOptions{
			Prefix:        opts.Prefix,
			Delimiter:     opts.Delimiter,
			StartAfter:    opts.StartAfter,
			MaxKeys:       opts.MaxKeys,
			SkipPartFetch: true,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listBucketResult := convertListBucketResult(*mListBucketResult)
	return &listBucketResult, nil
}

func (mbs *metadataPartStorage) ListObjectVersions(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListObjectVersions")
	defer span.End()

	var metaResult *metadatastore.ListObjectVersionsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		metaResult, err = mbs.metadataStore.ListObjectVersions(ctx, tx.SqlTx(), bucketName, metadatastore.ListObjectVersionsOptions{
			Prefix:          opts.Prefix,
			Delimiter:       opts.Delimiter,
			KeyMarker:       opts.KeyMarker,
			VersionIDMarker: opts.VersionIDMarker,
			MaxKeys:         opts.MaxKeys,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	versions := sliceutils.Map(func(v metadatastore.ObjectVersion) storage.ObjectVersion {
		return storage.ObjectVersion{Key: v.Key, VersionID: v.VersionID, IsDeleteMarker: v.IsDeleteMarker, IsLatest: v.IsLatest, LastModified: v.LastModified, Size: v.Size, ETag: v.ETag, StorageClass: v.StorageClass}
	}, metaResult.Versions)

	return &storage.ListObjectVersionsResult{
		Versions:            versions,
		CommonPrefixes:      metaResult.CommonPrefixes,
		IsTruncated:         metaResult.IsTruncated,
		NextKeyMarker:       metaResult.NextKeyMarker,
		NextVersionIDMarker: metaResult.NextVersionIDMarker,
	}, nil
}

func (mbs *metadataPartStorage) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.HeadObject")
	defer span.End()

	var mObject *metadatastore.Object
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		if opts != nil && opts.VersionID != nil {
			mObject, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else {
			mObject, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			return err
		}

		if mObject.IsDeleteMarker {
			versionID := ""
			if mObject.VersionID != nil {
				versionID = *mObject.VersionID
			}
			if opts != nil && opts.VersionID != nil {
				return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: mObject.LastModified}
			}
			return &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if opts.IfMatchETag != nil {
				if *opts.IfMatchETag != storage.ETagWildcard && mObject.ETag != *opts.IfMatchETag {
					return storage.ErrPreconditionFailed
				}
			}
			if opts.IfNoneMatchETag != nil {
				if *opts.IfNoneMatchETag == storage.ETagWildcard || mObject.ETag == *opts.IfNoneMatchETag {
					return storage.ErrNotModified
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	o := convertObject(*mObject)
	return &o, err
}

// normalizeAndValidateRanges converts suffix ranges to absolute ranges and validates all ranges.
// Returns an error if any range is invalid.
func normalizeAndValidateRanges(ranges []storage.ByteRange, objectSize int64) ([]storage.ByteRange, error) {
	normalized := make([]storage.ByteRange, len(ranges))

	for i, byteRange := range ranges {
		// Handle suffix range (e.g., bytes=-500 means last 500 bytes)
		if byteRange.Start == nil && byteRange.End != nil {
			if *byteRange.End <= 0 {
				return nil, storage.ErrInvalidRange
			}
			suffixLength := min(*byteRange.End, objectSize)
			start := objectSize - suffixLength
			end := objectSize
			normalized[i] = storage.ByteRange{Start: &start, End: &end}
			continue
		}

		// Validate normal ranges
		if byteRange.Start != nil && *byteRange.Start < 0 {
			return nil, storage.ErrInvalidRange
		}
		// Per RFC 7233: if the range end exceeds the object size, clamp it to the object size.
		if byteRange.End != nil && *byteRange.End > objectSize {
			clamped := objectSize
			byteRange.End = &clamped
		}
		if byteRange.Start != nil && byteRange.End != nil && *byteRange.Start >= *byteRange.End {
			return nil, storage.ErrInvalidRange
		}

		normalized[i] = byteRange
	}

	return normalized, nil
}

// evaluateCopySourceConditions enforces the x-amz-copy-source-if-* preconditions
// against the source object of a server-side copy. Time comparisons use second
// granularity (HTTP dates carry no sub-second component) to match S3 behaviour.
// Returns storage.ErrPreconditionFailed when a precondition fails.
func evaluateCopySourceConditions(conditions storage.CopySourceConditions, object *metadatastore.Object) error {
	ifMatchPassed := false
	if conditions.IfMatch != nil {
		ifMatchPassed = *conditions.IfMatch == storage.ETagWildcard || *conditions.IfMatch == object.ETag
		if !ifMatchPassed {
			return storage.ErrPreconditionFailed
		}
	}
	if conditions.IfNoneMatch != nil {
		if *conditions.IfNoneMatch == storage.ETagWildcard || *conditions.IfNoneMatch == object.ETag {
			return storage.ErrPreconditionFailed
		}
	}
	lastModified := object.LastModified.Truncate(time.Second)
	if conditions.IfUnmodifiedSince != nil && !(conditions.IfMatch != nil && ifMatchPassed) && lastModified.After(*conditions.IfUnmodifiedSince) {
		return storage.ErrPreconditionFailed
	}
	if conditions.IfModifiedSince != nil && !lastModified.After(*conditions.IfModifiedSince) {
		return storage.ErrPreconditionFailed
	}
	return nil
}

// createRangeReader creates a reader for a specific byte range of an object.
func (mbs *metadataPartStorage) createRangeReader(ctx context.Context, tx database.Tx, object *metadatastore.Object, byteRange storage.ByteRange) (io.ReadCloser, error) {
	startByte := byteRange.Start
	endByte := byteRange.End

	parts := make([]partRange, 0)
	var partsSizeUntilNow int64
	var globalStart int64
	if startByte != nil {
		globalStart = *startByte
	}
	globalEnd := object.Size
	if endByte != nil {
		globalEnd = *endByte
	}
	if globalStart >= globalEnd {
		return nil, storage.ErrInvalidRange
	}

	for _, part := range object.Parts {
		partStart := partsSizeUntilNow
		partEnd := partStart + part.Size

		// Skip parts before the requested range.
		if globalStart >= partEnd {
			partsSizeUntilNow = partEnd
			continue
		}
		// Stop once we're past the requested range.
		if globalEnd <= partStart {
			break
		}

		rangeStartInPart := int64(0)
		if globalStart > partStart {
			rangeStartInPart = globalStart - partStart
		}
		rangeEndInPart := part.Size
		if globalEnd < partEnd {
			rangeEndInPart = globalEnd - partStart
		}
		if rangeEndInPart < rangeStartInPart {
			return nil, fmt.Errorf("invalid part range computed")
		}

		store, err := mbs.partStores.ByName(part.StoreName)
		if err != nil {
			return nil, err
		}

		limit := rangeEndInPart - rangeStartInPart
		parts = append(parts, partRange{
			id:    part.Id,
			store: store,
			skip:  rangeStartInPart,
			limit: &limit,
		})

		partsSizeUntilNow = partEnd
	}
	if len(parts) == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	return &lazyPartSequenceReadCloser{
		ctx:   ctx,
		tx:    tx,
		parts: parts,
	}, nil
}

func (mbs *metadataPartStorage) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.GetObject")
	defer span.End()

	// When every part store can read without an ambient transaction
	// (filesystem, sftp — everything except DB-backed part content), the
	// metadata lookup runs in a short transaction that ends before streaming
	// starts. Slow clients then no longer pin a pooled database connection for
	// the lifetime of their download. A single DB-backed part store in the set
	// keeps the transaction open until every reader is closed (an object's
	// parts may live in any store), preserving snapshot semantics for the part
	// content itself.
	txFreeStreaming := mbs.partStores.SupportsTxFreeGetPart()

	// The context handed to WithTx carries the (short-lived) transaction;
	// BeginTx reuses a transaction found in the context, so tx-free readers
	// must capture the pre-transaction context instead or a later internal
	// BeginTx (e.g. the outbox lookup) would pick up the finalized one.
	streamCtx := ctx

	var storageObject storage.Object
	getObjectInTx := func(ctx context.Context, tx database.Tx) ([]io.ReadCloser, error) {
		var object *metadatastore.Object
		var err error
		if opts != nil && opts.VersionID != nil {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else {
			object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			return nil, err
		}

		if object.IsDeleteMarker {
			versionID := ""
			if object.VersionID != nil {
				versionID = *object.VersionID
			}
			if opts != nil && opts.VersionID != nil {
				return nil, &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: object.LastModified}
			}
			return nil, &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if opts.IfMatchETag != nil {
				if *opts.IfMatchETag != storage.ETagWildcard && object.ETag != *opts.IfMatchETag {
					return nil, storage.ErrPreconditionFailed
				}
			}
			if opts.IfNoneMatchETag != nil {
				if *opts.IfNoneMatchETag == storage.ETagWildcard || object.ETag == *opts.IfNoneMatchETag {
					return nil, storage.ErrNotModified
				}
			}
		}

		// Default to full object if no ranges specified
		effectiveRanges := ranges
		if len(effectiveRanges) == 0 {
			effectiveRanges = []storage.ByteRange{{Start: nil, End: nil}}
		}

		// Normalize suffix ranges and validate
		effectiveRanges, err = normalizeAndValidateRanges(effectiveRanges, object.Size)
		if err != nil {
			return nil, err
		}

		// The range readers are lazy: they don't touch the part store until
		// first read. With tx-free streaming they carry no transaction at all
		// and use the pre-transaction context.
		readerCtx := ctx
		readerTx := tx
		if txFreeStreaming {
			readerCtx = streamCtx
			readerTx = nil
		}

		// Create readers for each range
		var readers []io.ReadCloser
		for _, byteRange := range effectiveRanges {
			reader, err := mbs.createRangeReader(readerCtx, readerTx, object, byteRange)
			if err != nil {
				for _, r := range readers {
					r.Close()
				}
				return nil, err
			}
			readers = append(readers, reader)
		}

		storageObject = convertObject(*object)
		return readers, nil
	}

	var readers []io.ReadCloser
	var err error
	if txFreeStreaming {
		err = database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
			readers, err = getObjectInTx(ctx, tx)
			return err
		})
	} else {
		readers, err = database.WithTxReadClosers(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, getObjectInTx)
	}
	if err != nil {
		return nil, nil, err
	}

	return &storageObject, readers, nil
}

func (mbs *metadataPartStorage) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.PutObject")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	ifNoneMatchStar := opts != nil && opts.IfNoneMatchStar

	var object metadatastore.Object
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		if !ifNoneMatchStar {
			versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
			if err != nil {
				return err
			}
			versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled

			if !versioningEnabled {
				// In unversioned/suspended mode, writes overwrite the null version.
				// Remove its part content before metadata replacement.
				previousObject, err := mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, "null")
				if err != nil && err != storage.ErrNoSuchKey {
					return err
				}
				if previousObject != nil {
					for _, part := range previousObject.Parts {
						store, err := mbs.partStores.ByName(part.StoreName)
						if err != nil {
							return err
						}
						err = store.DeletePart(ctx, tx, part.Id)
						if err != nil {
							return err
						}
					}
				}
			}
		}

		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		var requestedStorageClass *string
		if opts != nil {
			requestedStorageClass = opts.StorageClass
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(requestedStorageClass))

		originalSize, calculatedChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
			return store.PutPart(ctx, tx, *partId, reader)
		})
		if err != nil {
			return err
		}

		err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
		if err != nil {
			return err
		}

		object = metadatastore.Object{
			Key:               key,
			ContentType:       contentType,
			LastModified:      time.Now(),
			ETag:              *calculatedChecksums.ETag,
			ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
			ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
			ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
			ChecksumType:      ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject),
			Size:              *originalSize,
			Parts: []metadatastore.Part{
				{
					Id:                *partId,
					ETag:              *calculatedChecksums.ETag,
					ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
					ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
					ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
					ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
					ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
					Size:              *originalSize,
					StoreName:         storeName,
				},
			},
		}
		if opts != nil {
			object.Tags = opts.Tags
			if opts.Metadata != nil {
				object.Metadata = *opts.Metadata
			}
			object.StorageClass = opts.StorageClass
		}

		metadataPutObjectOptions := &metadatastore.PutObjectOptions{IfNoneMatchStar: ifNoneMatchStar}
		if opts != nil {
			metadataPutObjectOptions.IfMatchETag = opts.IfMatchETag
		}
		err = mbs.metadataStore.PutObject(ctx, tx.SqlTx(), bucketName, &object, metadataPutObjectOptions)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &storage.PutObjectResult{
		VersionID:         object.VersionID,
		ETag:              &object.ETag,
		ChecksumCRC32:     object.ChecksumCRC32,
		ChecksumCRC32C:    object.ChecksumCRC32C,
		ChecksumCRC64NVME: object.ChecksumCRC64NVME,
		ChecksumSHA1:      object.ChecksumSHA1,
		ChecksumSHA256:    object.ChecksumSHA256,
	}, nil
}

func (mbs *metadataPartStorage) CopyObject(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, opts *storage.CopyObjectOptions) (*storage.CopyObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CopyObject")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	var result storage.CopyObjectResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var srcObject *metadatastore.Object
		var err error
		if opts != nil && opts.SourceVersionID != nil {
			srcObject, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), srcBucket, srcKey, *opts.SourceVersionID)
		} else {
			srcObject, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), srcBucket, srcKey)
		}
		if err != nil {
			return err
		}
		if srcObject.IsDeleteMarker {
			versionID := ""
			if srcObject.VersionID != nil {
				versionID = *srcObject.VersionID
			}
			if opts != nil && opts.SourceVersionID != nil {
				return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: srcObject.LastModified}
			}
			return &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if err := evaluateCopySourceConditions(opts.CopySourceConditions, srcObject); err != nil {
				return err
			}
		}

		lastModified := time.Now()
		dstObject := metadatastore.Object{
			Key:          dstKey,
			LastModified: lastModified,
		}
		if opts != nil {
			// The destination class comes from the copy request only; the
			// source's class is never carried over (matching AWS).
			dstObject.StorageClass = opts.StorageClass
		}
		dstStoreName, dstStore := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(dstObject.StorageClass))

		if opts != nil && opts.Range != nil {
			// Ranged copy: produce a single-part destination object containing the
			// requested byte range, with a freshly computed ETag.
			normalizedRanges, err := normalizeAndValidateRanges([]storage.ByteRange{*opts.Range}, srcObject.Size)
			if err != nil {
				return err
			}
			rangeReader, err := mbs.createRangeReader(ctx, tx, srcObject, normalizedRanges[0])
			if err != nil {
				return err
			}
			defer rangeReader.Close()

			newPartId, err := partstore.NewRandomPartId()
			if err != nil {
				return err
			}
			size, checksums, err := checksumutils.CalculateChecksumsStreaming(ctx, rangeReader, func(r io.Reader) error {
				return dstStore.PutPart(ctx, tx, *newPartId, r)
			})
			if err != nil {
				return err
			}
			dstObject.ETag = *checksums.ETag
			dstObject.ChecksumCRC32 = checksums.ChecksumCRC32
			dstObject.ChecksumCRC32C = checksums.ChecksumCRC32C
			dstObject.ChecksumCRC64NVME = checksums.ChecksumCRC64NVME
			dstObject.ChecksumSHA1 = checksums.ChecksumSHA1
			dstObject.ChecksumSHA256 = checksums.ChecksumSHA256
			dstObject.ChecksumType = ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject)
			dstObject.Size = *size
			dstObject.Parts = []metadatastore.Part{
				{
					Id:                *newPartId,
					ETag:              *checksums.ETag,
					ChecksumCRC32:     checksums.ChecksumCRC32,
					ChecksumCRC32C:    checksums.ChecksumCRC32C,
					ChecksumCRC64NVME: checksums.ChecksumCRC64NVME,
					ChecksumSHA1:      checksums.ChecksumSHA1,
					ChecksumSHA256:    checksums.ChecksumSHA256,
					Size:              *size,
					StoreName:         dstStoreName,
				},
			}
		} else {
			// Full copy: duplicate every source part to a fresh part id, preserving
			// the part structure and therefore the exact source ETag and checksums.
			newParts := make([]metadatastore.Part, len(srcObject.Parts))
			for i, srcPart := range srcObject.Parts {
				newPartId, err := partstore.NewRandomPartId()
				if err != nil {
					return err
				}
				srcStore, err := mbs.partStores.ByName(srcPart.StoreName)
				if err != nil {
					return err
				}
				srcReader, err := srcStore.GetPart(ctx, tx, srcPart.Id)
				if err != nil {
					return err
				}
				err = dstStore.PutPart(ctx, tx, *newPartId, srcReader)
				srcReader.Close()
				if err != nil {
					return err
				}
				newParts[i] = srcPart
				newParts[i].Id = *newPartId
				newParts[i].StoreName = dstStoreName
			}
			dstObject.ETag = srcObject.ETag
			dstObject.ChecksumCRC32 = srcObject.ChecksumCRC32
			dstObject.ChecksumCRC32C = srcObject.ChecksumCRC32C
			dstObject.ChecksumCRC64NVME = srcObject.ChecksumCRC64NVME
			dstObject.ChecksumSHA1 = srcObject.ChecksumSHA1
			dstObject.ChecksumSHA256 = srcObject.ChecksumSHA256
			dstObject.ChecksumType = srcObject.ChecksumType
			dstObject.Size = srcObject.Size
			dstObject.Parts = newParts
		}

		// Content type and metadata follow the metadata directive.
		if opts != nil && opts.ReplaceMetadata {
			dstObject.ContentType = opts.ContentType
			if opts.Metadata != nil {
				dstObject.Metadata = *opts.Metadata
			}
		} else {
			dstObject.ContentType = srcObject.ContentType
			dstObject.Metadata = srcObject.Metadata
			// S3 never carries x-amz-website-redirect-location over from the
			// source; it applies only when supplied on the copy request itself.
			dstObject.Metadata.WebsiteRedirectLocation = nil
			if opts != nil && opts.Metadata != nil {
				dstObject.Metadata.WebsiteRedirectLocation = opts.Metadata.WebsiteRedirectLocation
			}
		}

		// Tags follow the tagging directive: REPLACE uses the supplied tag set,
		// COPY (the default) carries over the source object's tags.
		if opts != nil && opts.ReplaceTags {
			dstObject.Tags = opts.Tags
		} else {
			dstObject.Tags = srcObject.Tags
		}

		// Remove the previous destination object's part content (if any) before
		// overwriting. When the destination equals the source, the previous parts
		// are the original part ids; the freshly copied parts use new ids and remain.
		// On a versioning-enabled destination the previous version is retained, so
		// its part content must stay; only the overwritten null version loses its
		// parts (mirroring PutObject).
		dstVersioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), dstBucket)
		if err != nil {
			return err
		}
		dstVersioningEnabled := dstVersioningConfig.Status != nil && *dstVersioningConfig.Status == metadatastore.BucketVersioningStatusEnabled
		if !dstVersioningEnabled {
			previousDst, err := mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), dstBucket, dstKey, "null")
			if err != nil && err != storage.ErrNoSuchKey {
				return err
			}
			if previousDst != nil {
				for _, part := range previousDst.Parts {
					store, err := mbs.partStores.ByName(part.StoreName)
					if err != nil {
						return err
					}
					if err := store.DeletePart(ctx, tx, part.Id); err != nil {
						return err
					}
				}
			}
		}

		if err := mbs.metadataStore.PutObject(ctx, tx.SqlTx(), dstBucket, &dstObject, nil); err != nil {
			return err
		}

		result = storage.CopyObjectResult{ETag: dstObject.ETag, LastModified: lastModified, SourceVersionID: srcObject.VersionID, VersionID: dstObject.VersionID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (mbs *metadataPartStorage) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, reader io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.AppendObject")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	var combinedChecksums checksumutils.ChecksumValues
	var totalSize int64
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}
		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled

		// Fetch the existing object (if any).
		existingObject, err := mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		var currentDeleteMarkerErr *storage.CurrentDeleteMarkerError
		if err != nil && !errors.As(err, &currentDeleteMarkerErr) && err != storage.ErrNoSuchKey {
			return err
		}
		if currentDeleteMarkerErr != nil {
			existingObject = nil
		}

		// Validate WriteOffset condition.
		if opts != nil && opts.WriteOffset != nil {
			if existingObject == nil {
				// Object does not exist — only allowed when offset == 0.
				if *opts.WriteOffset != 0 {
					return storage.ErrInvalidWriteOffset
				}
			} else {
				// Object exists — offset must equal current size.
				if *opts.WriteOffset != existingObject.Size {
					return storage.ErrInvalidWriteOffset
				}
			}
		}

		// Write the new part's bytes. Appends keep the existing object's
		// storage class, so new data routes to the store of that class.
		var existingStorageClass *string
		if existingObject != nil {
			existingStorageClass = existingObject.StorageClass
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(existingStorageClass))

		newPartId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		newPartSize, newPartChecksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(r io.Reader) error {
			return store.PutPart(ctx, tx, *newPartId, r)
		})
		if err != nil {
			return err
		}

		// Validate checksums for the new data chunk (if provided by the caller).
		if err = metadatastore.ValidateChecksums(checksumInput, *newPartChecksums); err != nil {
			return err
		}

		newPart := metadatastore.Part{
			Id:                *newPartId,
			ETag:              *newPartChecksums.ETag,
			ChecksumCRC32:     newPartChecksums.ChecksumCRC32,
			ChecksumCRC32C:    newPartChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: newPartChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      newPartChecksums.ChecksumSHA1,
			ChecksumSHA256:    newPartChecksums.ChecksumSHA256,
			Size:              *newPartSize,
			StoreName:         storeName,
		}

		// Build the combined part list (existing parts first, then new part).
		var allParts []metadatastore.Part
		totalSize = 0
		if existingObject != nil {
			totalSize = existingObject.Size
		}

		// S3 enforces a maximum of 10,000 parts per object.
		const maxAppendParts = 10_000
		existingPartCount := 0
		if existingObject != nil {
			existingPartCount = len(existingObject.Parts)
		}
		if existingPartCount+1 > maxAppendParts {
			return storage.ErrTooManyParts
		}

		if existingObject != nil {
			if versioningEnabled {
				// Versioned append creates a new object version, so existing parts need
				// fresh IDs instead of being reused in the new version metadata.
				allParts = make([]metadatastore.Part, 0, len(existingObject.Parts)+1)
				for _, existingPart := range existingObject.Parts {
					existingPartStore, partErr := mbs.partStores.ByName(existingPart.StoreName)
					if partErr != nil {
						return partErr
					}
					existingPartReader, partErr := existingPartStore.GetPart(ctx, tx, existingPart.Id)
					if partErr != nil {
						return partErr
					}

					newExistingPartID, partErr := partstore.NewRandomPartId()
					if partErr != nil {
						existingPartReader.Close()
						return partErr
					}

					partErr = existingPartStore.PutPart(ctx, tx, *newExistingPartID, existingPartReader)
					existingPartReader.Close()
					if partErr != nil {
						return partErr
					}

					clonedPart := existingPart
					clonedPart.Id = *newExistingPartID
					allParts = append(allParts, clonedPart)
				}
			} else {
				allParts = append(allParts, existingObject.Parts...)
			}
		}

		allParts = append(allParts, newPart)
		totalSize += *newPartSize

		// Compute the whole-object ETag as MD5-of-part-ETags (multipart-style).
		partChecksums := make([]checksumutils.PartChecksums, len(allParts))
		for i, p := range allParts {
			partChecksums[i] = checksumutils.PartChecksums{
				ETag: p.ETag,
				Size: p.Size,
			}
		}
		combinedChecksums, err = checksumutils.CalculateMultipartChecksums(partChecksums, checksumutils.ChecksumTypeFullObject)
		if err != nil {
			return err
		}

		// Determine content type: preserve existing or fall back to nil (unchanged).
		var contentType *string
		if existingObject != nil {
			contentType = existingObject.ContentType
		}

		updatedObject := &metadatastore.Object{
			Key:          key,
			ContentType:  contentType,
			LastModified: time.Now(),
			ETag:         *combinedChecksums.ETag,
			ChecksumType: ptrutils.ToPtr(metadatastore.ChecksumTypeFullObject),
			Size:         totalSize,
			Parts:        allParts,
		}

		metaOpts := &metadatastore.AppendObjectOptions{}
		if err = mbs.metadataStore.AppendObject(ctx, tx.SqlTx(), bucketName, updatedObject, metaOpts); err != nil {
			// The sql layer uses a CAS (DELETE WHERE id=X AND etag=Y) to detect a
			// concurrent write that changed the object between our HeadObject read
			// and this update. It surfaces that as ErrCASFailure. From the caller's
			// perspective the object size moved under them, so we normalise the
			// error to ErrInvalidWriteOffset (HTTP 400).
			if err == storage.ErrCASFailure {
				return storage.ErrInvalidWriteOffset
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &storage.AppendObjectResult{
		ETag: *combinedChecksums.ETag,
		Size: totalSize,
	}, nil
}

func (mbs *metadataPartStorage) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteObject")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	var result *storage.DeleteObjectResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}

		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled
		versioningSuspended := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusSuspended

		var object *metadatastore.Object
		if opts != nil && opts.VersionID != nil {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, *opts.VersionID)
		} else if versioningSuspended {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, key, "null")
		} else {
			object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		}
		if err != nil {
			if err == storage.ErrNoSuchKey && (opts == nil || opts.VersionID == nil) && (versioningEnabled || versioningSuspended) {
				// Enabled/suspended key-only delete of a missing (or null-version-less)
				// key still creates a delete marker below.
				object = nil
			} else if err == storage.ErrNoSuchKey {
				// Object does not exist.
				if opts != nil && opts.IfMatchETag != nil {
					// Conditional delete: object must exist.
					return storage.ErrPreconditionFailed
				}
				// No condition: silently succeed per S3 semantics.
				result = &storage.DeleteObjectResult{}
				return nil
			} else {
				return err
			}
		}

		shouldDeleteParts := opts != nil && opts.VersionID != nil
		if opts == nil || opts.VersionID == nil {
			if versioningSuspended {
				shouldDeleteParts = true // null version hard-delete on suspended key-only delete
			} else if !versioningEnabled {
				shouldDeleteParts = true // unversioned hard delete
			}
		}

		if shouldDeleteParts && object != nil && !object.IsDeleteMarker {
			for _, part := range object.Parts {
				store, err := mbs.partStores.ByName(part.StoreName)
				if err != nil {
					return err
				}
				err = store.DeletePart(ctx, tx, part.Id)
				if err != nil {
					return err
				}
			}
		}

		var metaOpts *metadatastore.DeleteObjectOptions
		if opts != nil {
			metaOpts = &metadatastore.DeleteObjectOptions{
				VersionID:   opts.VersionID,
				IfMatchETag: opts.IfMatchETag,
			}
		}
		metaResult, err := mbs.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucketName, key, metaOpts)
		if err != nil {
			return err
		}

		result = &storage.DeleteObjectResult{VersionID: metaResult.VersionID, IsDeleteMarker: metaResult.IsDeleteMarker}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (mbs *metadataPartStorage) TransitionObjectStorageClass(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, targetStorageClass string, opts *storage.TransitionObjectStorageClassOptions) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.TransitionObjectStorageClass")
	defer span.End()

	if !metadatastore.IsValidStorageClass(targetStorageClass) {
		return storage.ErrInvalidStorageClass
	}

	// Source parts are deleted inline in the same transaction as the metadata
	// swap (crash-safe: the delete is deferred to commit and rolled back on
	// abort, so a crash mid-transition leaves the source intact). GC remains a
	// safety net for target parts orphaned by a crash between copy and commit.
	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	targetStoreName, targetStore := mbs.partStores.StoreForClass(targetStorageClass)

	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		object, err := mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, key)
		if err != nil {
			return err
		}
		if object.IsDeleteMarker {
			return storage.ErrNoSuchKey
		}
		if opts != nil && opts.IfMatchETag != nil {
			if *opts.IfMatchETag != storage.ETagWildcard && object.ETag != *opts.IfMatchETag {
				return storage.ErrPreconditionFailed
			}
		}

		// When every part already lives in the target store (e.g. the class was
		// only remapped in config, or the classes share a store), just relabel
		// the object without moving any data.
		needsMove := false
		for _, part := range object.Parts {
			if !partStoreNamesEqual(part.StoreName, targetStoreName) {
				needsMove = true
				break
			}
		}

		newParts := object.Parts
		if needsMove {
			newParts = make([]metadatastore.Part, len(object.Parts))
			for i, srcPart := range object.Parts {
				srcStore, err := mbs.partStores.ByName(srcPart.StoreName)
				if err != nil {
					return err
				}
				newPartId, err := partstore.NewRandomPartId()
				if err != nil {
					return err
				}
				srcReader, err := srcStore.GetPart(ctx, tx, srcPart.Id)
				if err != nil {
					return err
				}
				err = targetStore.PutPart(ctx, tx, *newPartId, srcReader)
				srcReader.Close()
				if err != nil {
					return err
				}
				// Free the source bytes now that a copy exists in the target
				// store. DeletePart defers the physical removal to commit, so the
				// source survives an aborted transition and in-flight readers
				// holding an open handle keep working.
				if err := srcStore.DeletePart(ctx, tx, srcPart.Id); err != nil {
					return err
				}
				newParts[i] = srcPart
				newParts[i].Id = *newPartId
				newParts[i].StoreName = targetStoreName
			}
		}

		return mbs.metadataStore.TransitionObject(ctx, tx.SqlTx(), bucketName, key, object.ETag, targetStorageClass, newParts)
	})
}

// partStoreNamesEqual compares two part store names where nil means the default
// store.
func partStoreNamesEqual(a, b *string) bool {
	aName := partstore.DefaultPartStoreName
	if a != nil {
		aName = *a
	}
	bName := partstore.DefaultPartStoreName
	if b != nil {
		bName = *b
	}
	return aName == bName
}

func (mbs *metadataPartStorage) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.DeleteObjects")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	result := &storage.DeleteObjectsResult{
		Entries: make([]storage.DeleteObjectsEntry, 0, len(entries)),
	}

	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		versioningConfig, err := mbs.metadataStore.GetBucketVersioningConfiguration(ctx, tx.SqlTx(), bucketName)
		if err != nil {
			return err
		}
		versioningEnabled := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusEnabled
		versioningSuspended := versioningConfig.Status != nil && *versioningConfig.Status == metadatastore.BucketVersioningStatusSuspended

		for _, entry := range entries {
			var object *metadatastore.Object
			var err error
			if entry.VersionID != nil {
				object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, entry.Key, *entry.VersionID)
			} else if versioningSuspended {
				object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucketName, entry.Key, "null")
			} else {
				object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucketName, entry.Key)
			}
			if err != nil {
				if err == storage.ErrNoSuchKey {
					if entry.IfMatchETag != nil {
						// Object does not exist but ETag condition set → precondition failed for this entry.
						result.Entries = append(result.Entries, storage.DeleteObjectsEntry{
							Key:     entry.Key,
							Deleted: false,
							ErrCode: "PreconditionFailed",
							ErrMsg:  "At least one of the pre-conditions you specified did not hold",
						})
						continue
					}
					if !(entry.VersionID == nil && (versioningEnabled || versioningSuspended)) {
						result.Entries = append(result.Entries, storage.DeleteObjectsEntry{Key: entry.Key, Deleted: true})
						continue
					}
					object = nil
				} else {
					return err
				}
			}

			// Object exists — check conditional ETag if specified.
			if entry.IfMatchETag != nil && (object == nil || object.ETag != *entry.IfMatchETag) {
				result.Entries = append(result.Entries, storage.DeleteObjectsEntry{
					Key:     entry.Key,
					Deleted: false,
					ErrCode: "PreconditionFailed",
					ErrMsg:  "At least one of the pre-conditions you specified did not hold",
				})
				continue
			}

			shouldDeleteParts := entry.VersionID != nil
			if entry.VersionID == nil {
				if versioningSuspended {
					shouldDeleteParts = true
				} else if !versioningEnabled {
					shouldDeleteParts = true
				}
			}

			if shouldDeleteParts && object != nil && !object.IsDeleteMarker {
				for _, part := range object.Parts {
					store, err := mbs.partStores.ByName(part.StoreName)
					if err != nil {
						return err
					}
					err = store.DeletePart(ctx, tx, part.Id)
					if err != nil {
						return err
					}
				}
			}

			var metaOpts *metadatastore.DeleteObjectOptions
			if entry.IfMatchETag != nil || entry.VersionID != nil {
				metaOpts = &metadatastore.DeleteObjectOptions{VersionID: entry.VersionID, IfMatchETag: entry.IfMatchETag}
			}
			metaResult, err := mbs.metadataStore.DeleteObject(ctx, tx.SqlTx(), bucketName, entry.Key, metaOpts)
			if err != nil {
				return err
			}

			resultEntry := storage.DeleteObjectsEntry{Key: entry.Key, DeleteMarker: &metaResult.IsDeleteMarker, Deleted: true}
			if metaResult.IsDeleteMarker && entry.VersionID == nil {
				resultEntry.DeleteMarkerVersionID = metaResult.VersionID
			} else {
				resultEntry.VersionID = metaResult.VersionID
			}
			result.Entries = append(result.Entries, resultEntry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func convertInitiateMultipartUploadResult(result metadatastore.InitiateMultipartUploadResult) storage.InitiateMultipartUploadResult {
	return storage.InitiateMultipartUploadResult{
		UploadId: result.UploadId,
	}
}

func (mbs *metadataPartStorage) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CreateMultipartUpload")
	defer span.End()

	var metadataOpts *metadatastore.CreateMultipartUploadOptions
	if opts != nil {
		metadataOpts = &metadatastore.CreateMultipartUploadOptions{Tags: opts.Tags, Metadata: opts.Metadata, StorageClass: opts.StorageClass}
	}

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	var initiateMultipartUploadResult storage.InitiateMultipartUploadResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		result, err := mbs.metadataStore.CreateMultipartUpload(ctx, tx.SqlTx(), bucketName, key, contentType, checksumType, metadataOpts)
		if err != nil {
			return err
		}
		initiateMultipartUploadResult = convertInitiateMultipartUploadResult(*result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &initiateMultipartUploadResult, nil
}

func (mbs *metadataPartStorage) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, reader io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.UploadPart")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	var calculatedChecksums *checksumutils.ChecksumValues
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		// Parts route to the store of the class chosen at
		// CreateMultipartUpload, so the upload must be resolved before the
		// part bytes are streamed.
		upload, err := mbs.metadataStore.GetMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId)
		if err != nil {
			return err
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(upload.StorageClass))

		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}

		originalSize, checksums, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(reader io.Reader) error {
			return store.PutPart(ctx, tx, *partId, reader)
		})
		if err != nil {
			return err
		}
		calculatedChecksums = checksums

		err = metadatastore.ValidateChecksums(checksumInput, *calculatedChecksums)
		if err != nil {
			return err
		}

		return mbs.metadataStore.UploadPart(ctx, tx.SqlTx(), bucketName, key, uploadId, partNumber, metadatastore.Part{
			Id:                *partId,
			ETag:              *calculatedChecksums.ETag,
			ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
			ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
			ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
			ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
			ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
			Size:              *originalSize,
			StoreName:         storeName,
		})
	})
	if err != nil {
		return nil, err
	}
	return &storage.UploadPartResult{
		ETag:              *calculatedChecksums.ETag,
		ChecksumCRC32:     calculatedChecksums.ChecksumCRC32,
		ChecksumCRC32C:    calculatedChecksums.ChecksumCRC32C,
		ChecksumCRC64NVME: calculatedChecksums.ChecksumCRC64NVME,
		ChecksumSHA1:      calculatedChecksums.ChecksumSHA1,
		ChecksumSHA256:    calculatedChecksums.ChecksumSHA256,
	}, nil
}

func (mbs *metadataPartStorage) UploadPartCopy(ctx context.Context, srcBucket storage.BucketName, srcKey storage.ObjectKey, dstBucket storage.BucketName, dstKey storage.ObjectKey, uploadId storage.UploadId, partNumber int32, opts *storage.UploadPartCopyOptions) (*storage.UploadPartCopyResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.UploadPartCopy")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()

	var result storage.UploadPartCopyResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		var srcObject *metadatastore.Object
		var err error
		if opts != nil && opts.SourceVersionID != nil {
			srcObject, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), srcBucket, srcKey, *opts.SourceVersionID)
		} else {
			srcObject, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), srcBucket, srcKey)
		}
		if err != nil {
			return err
		}
		if srcObject.IsDeleteMarker {
			versionID := ""
			if srcObject.VersionID != nil {
				versionID = *srcObject.VersionID
			}
			if opts != nil && opts.SourceVersionID != nil {
				return &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: versionID, LastModified: srcObject.LastModified}
			}
			return &storage.CurrentDeleteMarkerError{VersionID: versionID}
		}

		if opts != nil {
			if err := evaluateCopySourceConditions(opts.CopySourceConditions, srcObject); err != nil {
				return err
			}
		}

		// Default to the whole source object when no range is given.
		copyRange := storage.ByteRange{}
		if opts != nil && opts.Range != nil {
			copyRange = *opts.Range
		}
		normalizedRanges, err := normalizeAndValidateRanges([]storage.ByteRange{copyRange}, srcObject.Size)
		if err != nil {
			return err
		}
		rangeReader, err := mbs.createRangeReader(ctx, tx, srcObject, normalizedRanges[0])
		if err != nil {
			return err
		}
		defer rangeReader.Close()

		// The copied part routes to the store of the class chosen at
		// CreateMultipartUpload for the destination upload.
		upload, err := mbs.metadataStore.GetMultipartUpload(ctx, tx.SqlTx(), dstBucket, dstKey, uploadId)
		if err != nil {
			return err
		}
		storeName, store := mbs.partStores.StoreForClass(metadatastore.EffectiveStorageClass(upload.StorageClass))

		newPartId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}
		size, checksums, err := checksumutils.CalculateChecksumsStreaming(ctx, rangeReader, func(r io.Reader) error {
			return store.PutPart(ctx, tx, *newPartId, r)
		})
		if err != nil {
			return err
		}

		err = mbs.metadataStore.UploadPart(ctx, tx.SqlTx(), dstBucket, dstKey, uploadId, partNumber, metadatastore.Part{
			Id:                *newPartId,
			ETag:              *checksums.ETag,
			ChecksumCRC32:     checksums.ChecksumCRC32,
			ChecksumCRC32C:    checksums.ChecksumCRC32C,
			ChecksumCRC64NVME: checksums.ChecksumCRC64NVME,
			ChecksumSHA1:      checksums.ChecksumSHA1,
			ChecksumSHA256:    checksums.ChecksumSHA256,
			Size:              *size,
			StoreName:         storeName,
		})
		if err != nil {
			return err
		}

		result = storage.UploadPartCopyResult{ETag: *checksums.ETag, LastModified: time.Now(), SourceVersionID: srcObject.VersionID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func convertCompleteMultipartUploadResult(result metadatastore.CompleteMultipartUploadResult) storage.CompleteMultipartUploadResult {
	return storage.CompleteMultipartUploadResult{
		Location:          result.Location,
		VersionID:         result.VersionID,
		ETag:              result.ETag,
		ChecksumCRC32:     result.ChecksumCRC32,
		ChecksumCRC32C:    result.ChecksumCRC32C,
		ChecksumCRC64NVME: result.ChecksumCRC64NVME,
		ChecksumSHA1:      result.ChecksumSHA1,
		ChecksumSHA256:    result.ChecksumSHA256,
		ChecksumType:      result.ChecksumType,
	}
}

func (mbs *metadataPartStorage) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.CompleteMultipartUpload")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	var completeMultipartUploadResult storage.CompleteMultipartUploadResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		result, err := mbs.metadataStore.CompleteMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId, checksumInput, opts)
		if err != nil {
			return err
		}
		for _, deletedPart := range result.DeletedParts {
			store, err := mbs.partStores.ByName(deletedPart.StoreName)
			if err != nil {
				return err
			}
			err = store.DeletePart(ctx, tx, deletedPart.Id)
			if err != nil {
				return err
			}
		}
		completeMultipartUploadResult = convertCompleteMultipartUploadResult(*result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &completeMultipartUploadResult, nil
}

func (mbs *metadataPartStorage) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.AbortMultipartUpload")
	defer span.End()

	unblockGC := mbs.partGC.PreventGCFromRunning(ctx)
	defer unblockGC()
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		abortMultipartUploadResult, err := mbs.metadataStore.AbortMultipartUpload(ctx, tx.SqlTx(), bucketName, key, uploadId)
		if err != nil {
			return err
		}
		for _, deletedPart := range abortMultipartUploadResult.DeletedParts {
			store, err := mbs.partStores.ByName(deletedPart.StoreName)
			if err != nil {
				return err
			}
			err = store.DeletePart(ctx, tx, deletedPart.Id)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func convertListMultipartUploadsResult(mlistMultipartUploadsResult metadatastore.ListMultipartUploadsResult) storage.ListMultipartUploadsResult {
	return storage.ListMultipartUploadsResult{
		BucketName:         mlistMultipartUploadsResult.Bucket,
		KeyMarker:          mlistMultipartUploadsResult.KeyMarker,
		UploadIdMarker:     mlistMultipartUploadsResult.UploadIdMarker,
		NextKeyMarker:      mlistMultipartUploadsResult.NextKeyMarker,
		Prefix:             mlistMultipartUploadsResult.Prefix,
		Delimiter:          mlistMultipartUploadsResult.Delimiter,
		NextUploadIdMarker: mlistMultipartUploadsResult.NextUploadIdMarker,
		MaxUploads:         mlistMultipartUploadsResult.MaxUploads,
		CommonPrefixes:     mlistMultipartUploadsResult.CommonPrefixes,
		Uploads: sliceutils.Map(func(mUpload metadatastore.Upload) storage.Upload {
			return storage.Upload{
				Key:          mUpload.Key,
				UploadId:     mUpload.UploadId,
				Initiated:    mUpload.Initiated,
				StorageClass: mUpload.StorageClass,
			}
		}, mlistMultipartUploadsResult.Uploads),
		IsTruncated: mlistMultipartUploadsResult.IsTruncated,
	}
}

func (mbs *metadataPartStorage) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListMultipartUploads")
	defer span.End()

	var mListMultipartUploadsResult *metadatastore.ListMultipartUploadsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListMultipartUploadsResult, err = mbs.metadataStore.ListMultipartUploads(ctx, tx.SqlTx(), bucketName, metadatastore.ListMultipartUploadsOptions{
			Prefix:         opts.Prefix,
			Delimiter:      opts.Delimiter,
			KeyMarker:      opts.KeyMarker,
			UploadIdMarker: opts.UploadIdMarker,
			MaxUploads:     opts.MaxUploads,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listMultipartUploadsResult := convertListMultipartUploadsResult(*mListMultipartUploadsResult)
	return &listMultipartUploadsResult, nil
}

func convertListPartsResult(mlistPartsResult metadatastore.ListPartsResult) storage.ListPartsResult {
	return storage.ListPartsResult{
		BucketName:           mlistPartsResult.BucketName,
		Key:                  mlistPartsResult.Key,
		UploadId:             mlistPartsResult.UploadId,
		PartNumberMarker:     mlistPartsResult.PartNumberMarker,
		NextPartNumberMarker: mlistPartsResult.NextPartNumberMarker,
		MaxParts:             mlistPartsResult.MaxParts,
		IsTruncated:          mlistPartsResult.IsTruncated,
		Parts: sliceutils.Map(func(part *metadatastore.MultipartPart) *storage.MultipartPart {
			return &storage.MultipartPart{
				ETag:              part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      part.LastModified,
				PartNumber:        part.PartNumber,
				Size:              part.Size,
			}
		}, mlistPartsResult.Parts),
		StorageClass: mlistPartsResult.StorageClass,
	}
}

func (mbs *metadataPartStorage) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	ctx, span := mbs.tracer.Start(ctx, "MetadataPartStorage.ListParts")
	defer span.End()

	var mListPartsResult *metadatastore.ListPartsResult
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mListPartsResult, err = mbs.metadataStore.ListParts(ctx, tx.SqlTx(), bucketName, key, uploadId, metadatastore.ListPartsOptions{
			PartNumberMarker: opts.PartNumberMarker,
			MaxParts:         opts.MaxParts,
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	listPartsResult := convertListPartsResult(*mListPartsResult)
	return &listPartsResult, nil
}
