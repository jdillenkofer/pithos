package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repository "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/replicationjournal"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const maxMemoryCacheSize = 10 * 1000 * 1000

var ErrMissingMapping = errors.New("replication version mapping missing; run reconcile-replication")
var ErrIndeterminatePrimary = errors.New("remote primary outcome is indeterminate; inspect the primary and run reconcile-replication")
var ErrIndeterminateReplicaCompletion = errors.New("replica multipart completion outcome is indeterminate; manual reconciliation is required")

type Options struct {
	Registerer      prometheus.Registerer
	ReplicationID   string
	SecondaryIDs    []string
	JournalDatabase database.Database
}

type replicationStorage struct {
	metrics      *replicationMetrics
	workerCancel context.CancelFunc
	workerDone   chan struct{}
	*lifecycle.ValidatedLifecycle
	delegator.DelegatingStorage
	secondaryStorages []storage.Storage
	options           Options
	db                database.Database
	localPrimary      bool
	journal           replicationjournal.Repository
	mu                sync.Mutex
}

type operationPayload struct {
	Bucket            string
	OwnerAccountID    string
	Key               string
	SourceBucket      string
	SourceKey         string
	UploadID          string
	PartNumber        int32
	ContentType       *string
	ChecksumType      *string
	Checksum          *storage.ChecksumInput
	Create            []storage.CreateBucketOptions
	Put               *storage.PutObjectOptions
	Copy              *storage.CopyObjectOptions
	Append            *storage.AppendObjectOptions
	Delete            *storage.DeleteObjectOptions
	Multipart         *storage.CreateMultipartUploadOptions
	Complete          *storage.CompleteMultipartUploadOptions
	PartCopy          *storage.UploadPartCopyOptions
	LockOptions       *storage.ObjectLockOptions
	LockConfiguration *storage.ObjectLockConfiguration
	Retention         *storage.ObjectRetention
	Hold              storage.LegalHoldStatus
	TagOptions        *storage.ObjectTaggingOptions
	Tags              map[string]string
	Versioning        *storage.BucketVersioningConfiguration
	Website           *storage.WebsiteConfiguration
	CORS              *storage.BucketCORSConfiguration
	Lifecycle         *storage.BucketLifecycleConfiguration
	Notification      *storage.BucketNotificationConfiguration
	Transition        *storage.TransitionObjectStorageClassOptions
	StorageClass      string
	Snapshot          *storage.Object
}

type operationResult struct {
	Put       *storage.PutObjectResult
	Copy      *storage.CopyObjectResult
	Append    *storage.AppendObjectResult
	Delete    *storage.DeleteObjectResult
	Complete  *storage.CompleteMultipartUploadResult
	Part      *storage.UploadPartResult
	PartCopy  *storage.UploadPartCopyResult
	UploadID  string
	VersionID *string
}

// multipartSnapshotProgress is durable per operation and secondary. Uploaded
// parts form a contiguous prefix, allowing recovery to seek past acknowledged
// bytes and resume without creating another multipart upload.
type multipartSnapshotProgress struct {
	UploadID   string                                `json:"uploadId"`
	Parts      []storage.CompleteMultipartUploadPart `json:"parts,omitempty"`
	Completing bool                                  `json:"completing,omitempty"`
	Complete   *operationResult                      `json:"complete,omitempty"`
}

func NewStorage(primary storage.Storage, secondaries ...storage.Storage) (storage.Storage, error) {
	ids := make([]string, len(secondaries))
	for i := range ids {
		ids[i] = fmt.Sprintf("secondary-%d", i)
	}
	return NewStorageWithOptions(primary, secondaries, Options{ReplicationID: "default", SecondaryIDs: ids})
}

func NewStorageWithOptions(primary storage.Storage, secondaries []storage.Storage, options Options) (storage.Storage, error) {
	if options.ReplicationID == "" || len(options.SecondaryIDs) != len(secondaries) {
		return nil, errors.New("replicationId and one stable secondaryId per replica are required")
	}
	seen := map[string]bool{}
	for _, id := range options.SecondaryIDs {
		if id == "" || seen[id] {
			return nil, errors.New("secondaryIds must be nonempty and unique")
		}
		seen[id] = true
	}
	var primaryDB database.Database
	if provider, ok := primary.(interface{ Database() database.Database }); ok {
		primaryDB = provider.Database()
	}
	db := primaryDB
	if db == nil {
		db = options.JournalDatabase
	}
	if db == nil {
		return nil, errors.New("non-local replication primary requires an explicit journal database")
	}
	lc, err := lifecycle.NewValidatedLifecycle("ReplicationStorage")
	if err != nil {
		return nil, err
	}
	j, err := repository.NewReplicationJournalRepository(db)
	if err != nil {
		return nil, err
	}
	metrics, err := newMetrics(options.Registerer, options.ReplicationID)
	if err != nil {
		return nil, err
	}
	return &replicationStorage{ValidatedLifecycle: lc, DelegatingStorage: delegator.Wrap(primary), secondaryStorages: secondaries, options: options, db: db, localPrimary: primaryDB != nil, journal: j, metrics: metrics}, nil
}

func (rs *replicationStorage) Database() database.Database { return nil }

// Replication owns the primary commit boundary: secondary failures must never
// roll back a committed primary and its durable journal entry.
func (rs *replicationStorage) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(context.Context, storage.Storage) error) error {
	return fn(ctx, rs)
}

func (rs *replicationStorage) Start(ctx context.Context) error {
	if err := rs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := rs.Next.Start(ctx); err != nil {
		return err
	}
	for _, secondary := range rs.secondaryStorages {
		if err := secondary.Start(ctx); err != nil {
			return err
		}
	}
	if lifecycle.IsDryRun(ctx) {
		return nil
	}
	if err := database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		return rs.journal.RegisterTopology(ctx, tx.SqlTx(), rs.options.ReplicationID, rs.options.SecondaryIDs)
	}); err != nil {
		return err
	}
	if lifecycle.IsMaintenance(ctx) {
		return nil
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if err := rs.replayPending(ctx); err != nil {
		slog.ErrorContext(ctx, "Replication recovery pending", "replication_id", rs.options.ReplicationID, "err", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	rs.workerCancel = cancel
	rs.workerDone = make(chan struct{})
	go func() {
		defer close(rs.workerDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				rs.mu.Lock()
				err := rs.replayPending(workerCtx)
				rs.mu.Unlock()
				if err != nil && workerCtx.Err() == nil {
					slog.WarnContext(workerCtx, "Replication retry pending", "replication_id", rs.options.ReplicationID, "err", err)
				}
			}
		}
	}()
	return nil
}
func (rs *replicationStorage) Stop(ctx context.Context) error {
	if rs.workerCancel != nil {
		rs.workerCancel()
		<-rs.workerDone
	}
	if err := rs.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	var errs []error
	for _, secondary := range rs.secondaryStorages {
		errs = append(errs, secondary.Stop(ctx))
	}
	errs = append(errs, rs.Next.Stop(ctx))
	return errors.Join(errs...)
}

func (rs *replicationStorage) mapping(ctx context.Context, secondary, bucket, key, kind, id string) (string, error) {
	var mapped *string
	err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		mapped, err = rs.journal.FindMapping(ctx, tx.SqlTx(), replicationjournal.Mapping{ReplicationID: rs.options.ReplicationID, SecondaryID: secondary, Bucket: bucket, Key: key, Kind: kind, PrimaryID: id})
		return err
	})
	if err != nil {
		return "", err
	}
	if mapped == nil {
		return "", fmt.Errorf("%w: bucket=%s key=%s kind=%s primaryId=%s secondaryId=%s", ErrMissingMapping, bucket, key, kind, id, secondary)
	}
	return *mapped, nil
}

func (rs *replicationStorage) prepareTargets(ctx context.Context, name string, p *operationPayload) error {
	var version **string
	switch name {
	case "PutObjectRetention", "PutObjectLegalHold":
		if p.LockOptions == nil {
			p.LockOptions = &storage.ObjectLockOptions{}
		}
		version = &p.LockOptions.VersionID
	case "PutObjectTagging", "DeleteObjectTagging":
		if p.TagOptions == nil {
			p.TagOptions = &storage.ObjectTaggingOptions{}
		}
		version = &p.TagOptions.VersionID
	case "TransitionObjectStorageClass":
		if p.Transition == nil {
			p.Transition = &storage.TransitionObjectStorageClassOptions{}
		}
		version = &p.Transition.VersionID
	case "DeleteObject":
		if p.Delete != nil && p.Delete.VersionID != nil {
			version = &p.Delete.VersionID
		}
	}
	if version != nil {
		if *version == nil {
			head, err := rs.Next.HeadObject(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), nil)
			if err != nil {
				return err
			}
			*version = head.VersionID
			if *version == nil {
				v := "null"
				*version = &v
			}
		}
		for _, secondary := range rs.options.SecondaryIDs {
			if _, err := rs.mapping(ctx, secondary, p.Bucket, p.Key, "VERSION", **version); err != nil {
				return err
			}
		}
	}
	if p.UploadID != "" {
		for _, secondary := range rs.options.SecondaryIDs {
			if _, err := rs.mapping(ctx, secondary, p.Bucket, p.Key, "UPLOAD", p.UploadID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rs *replicationStorage) execute(ctx context.Context, name string, p operationPayload, input io.Reader) (*operationResult, error) {
	if _, active := database.TxControllerFromContext(ctx); active {
		return nil, errors.New("replication mutation requires its own commit boundary")
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if err := rs.replayPending(ctx); err != nil {
		return nil, err
	}
	// Callers can reuse options; resolving a target must not change their values.
	cloned, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var copied operationPayload
	if err := json.Unmarshal(cloned, &copied); err != nil {
		return nil, err
	}
	p = copied
	if err := rs.prepareTargets(ctx, name, &p); err != nil {
		return nil, err
	}
	if input == nil {
		input = strings.NewReader("")
	}
	cached, err := ioutils.NewSmartCachedReadSeekCloser(input, maxMemoryCacheSize)
	if err != nil {
		return nil, err
	}
	defer cached.Close()
	op := &replicationjournal.Operation{ID: ulid.Make().String(), ReplicationID: rs.options.ReplicationID, Bucket: p.Bucket, Key: p.Key, Name: name, State: "INTENT"}
	encoded, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	op.Payload = string(encoded)
	var result *operationResult
	if rs.localPrimary {
		err = database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
			if err := rs.journal.RegisterTopology(ctx, tx.SqlTx(), rs.options.ReplicationID, rs.options.SecondaryIDs); err != nil {
				return err
			}
			var err error
			result, err = rs.apply(ctx, rs.Next, name, &p, cached)
			if err != nil {
				return err
			}
			if err := rs.finalizePrimary(ctx, tx, op, &p, result, cached); err != nil {
				return err
			}
			return nil
		})
	} else {
		err = database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
			if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
				return err
			}
			return rs.journal.SaveData(ctx, tx.SqlTx(), op.ID, cached)
		})
		if err != nil {
			return nil, err
		}
		if _, err = cached.Seek(0, io.SeekStart); err != nil {
			return nil, rs.recordFailure(ctx, op, err)
		}
		result, err = rs.apply(ctx, rs.Next, name, &p, cached)
		if err != nil {
			if definitiveRejection(err) {
				return nil, rs.finishRejected(ctx, op, err)
			}
			return nil, rs.finishIndeterminatePrimary(ctx, op, err)
		}
		err = database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
			return rs.finalizePrimary(ctx, tx, op, &p, result, cached)
		})
		if err != nil {
			// The primary succeeded, but its result could not be made durable.
			// Reissuing a version-producing request could create another version.
			op.Payload = string(encoded)
			return nil, rs.finishIndeterminatePrimary(ctx, op, err)
		}
	}
	if err != nil {
		return nil, err
	}
	if err := rs.replicate(ctx, op); err != nil {
		return nil, err
	}
	return result, nil
}

func (rs *replicationStorage) finalizePrimary(ctx context.Context, tx database.Tx, op *replicationjournal.Operation, p *operationPayload, result *operationResult, input io.ReadSeeker) error {
	ctx = storage.WithObjectLockObserver(ctx, nil)
	if err := resolveWrittenVersion(ctx, rs.Next, op.Name, p, result); err != nil {
		return err
	}
	if op.Name == "PutObjectRetention" {
		effective, err := rs.Next.GetObjectRetention(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), p.LockOptions)
		if err != nil {
			return err
		}
		p.Retention = effective
	}
	var data io.Reader = input
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if result.VersionID != nil && (op.Name == "PutObject" || op.Name == "AppendObject") {
		obj, err := rs.Next.HeadObject(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), &storage.HeadObjectOptions{VersionID: result.VersionID})
		if err != nil {
			return err
		}
		if op.Name == "PutObject" {
			if p.Put == nil {
				p.Put = &storage.PutObjectOptions{}
			}
			p.Put.ObjectLock = obj.ObjectLock
		} else {
			if p.Append == nil {
				p.Append = &storage.AppendObjectOptions{}
			}
			p.Append.ObjectLock = obj.ObjectLock
		}
	} else if result.VersionID != nil && (op.Name == "CopyObject" || op.Name == "CompleteMultipartUpload") {
		obj, readers, err := rs.Next.GetObject(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), nil, &storage.GetObjectOptions{VersionID: result.VersionID})
		if err != nil {
			return err
		}
		if len(readers) != 1 {
			return errors.New("replication snapshot requires one complete object reader")
		}
		defer readers[0].Close()
		p.Snapshot = obj
		data = readers[0]
	}
	if op.Name == "UploadPartCopy" {
		var ranges []storage.ByteRange
		var version *string
		if p.PartCopy != nil {
			version = p.PartCopy.SourceVersionID
			if p.PartCopy.Range != nil {
				ranges = []storage.ByteRange{*p.PartCopy.Range}
			}
		}
		if result.PartCopy != nil && result.PartCopy.SourceVersionID != nil {
			version = result.PartCopy.SourceVersionID
		}
		_, readers, err := rs.Next.GetObject(ctx, storage.MustNewBucketName(p.SourceBucket), storage.MustNewObjectKey(p.SourceKey), ranges, &storage.GetObjectOptions{VersionID: version})
		if err != nil {
			return err
		}
		if len(readers) != 1 {
			return errors.New("invalid part-copy snapshot")
		}
		defer readers[0].Close()
		data = readers[0]
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return err
	}
	op.Payload = string(encoded)
	encoded, err = json.Marshal(result)
	if err != nil {
		return err
	}
	value := string(encoded)
	op.PrimaryResult = &value
	op.State = "REPLICATING"
	if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
		return err
	}
	return rs.journal.SaveData(ctx, tx.SqlTx(), op.ID, data)
}

func (rs *replicationStorage) replayPending(ctx context.Context) error {
	ctx = storage.WithObjectLockObserver(ctx, nil)
	var ops []replicationjournal.Operation
	err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		ops, err = rs.journal.Pending(ctx, tx.SqlTx(), rs.options.ReplicationID)
		return err
	})
	if err != nil {
		return err
	}
	rs.metrics.pending.Set(float64(len(ops)))
	for i := range ops {
		op := &ops[i]
		if op.Attempts > 0 {
			rs.metrics.retries.Inc()
		}
		if op.State == "INTENT" {
			// INTENT is used only with a remote primary. After a restart there is
			// no safe way to know whether the request was accepted, so never replay
			// it. Reconciliation can recover authoritative object versions without
			// risking another primary mutation.
			if err := rs.markIndeterminatePrimary(ctx, op, errors.New("recovered unresolved remote-primary intent")); err != nil {
				return err
			}
			continue
		}
		if err := rs.replicate(ctx, op); err != nil {
			return err
		}
	}
	rs.metrics.pending.Set(0)
	return nil
}

type readSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

func (rs *replicationStorage) cachedData(ctx context.Context, id string) (readSeekCloser, error) {
	var cached readSeekCloser
	err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		reader, err := rs.journal.ReadData(ctx, tx.SqlTx(), id)
		if err != nil {
			return err
		}
		defer reader.Close()
		cached, err = ioutils.NewSmartCachedReadSeekCloser(reader, maxMemoryCacheSize)
		return err
	})
	return cached, err
}

func (rs *replicationStorage) recordFailure(ctx context.Context, op *replicationjournal.Operation, cause error) error {
	rs.metrics.failures.Inc()
	op.Attempts++
	op.LastError = cause.Error()
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	err := database.WithTx(saveCtx, rs.db, nil, func(ctx context.Context, tx database.Tx) error { return rs.journal.SaveOperation(ctx, tx.SqlTx(), op) })
	return errors.Join(fmt.Errorf("replication operation %s pending: %w", op.ID, cause), err)
}

func (rs *replicationStorage) replicate(ctx context.Context, op *replicationjournal.Operation) error {
	ctx = storage.WithObjectLockObserver(ctx, nil)
	var p operationPayload
	var primary operationResult
	if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
		return err
	}
	if op.PrimaryResult == nil {
		return errors.New("replication journal has no primary result")
	}
	if err := json.Unmarshal([]byte(*op.PrimaryResult), &primary); err != nil {
		return err
	}
	var acks map[string]string
	if err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		acks, err = rs.journal.Acknowledgments(ctx, tx.SqlTx(), op.ID)
		return err
	}); err != nil {
		return err
	}
	for i, secondary := range rs.secondaryStorages {
		id := rs.options.SecondaryIDs[i]
		if _, ok := acks[id]; ok {
			continue
		}
		// Decode a fresh copy so translated IDs never leak to another replica.
		var replicaPayload operationPayload
		if err := json.Unmarshal([]byte(op.Payload), &replicaPayload); err != nil {
			return err
		}
		if err := rs.translate(ctx, id, &replicaPayload); err != nil {
			return rs.recordFailure(ctx, op, err)
		}
		reader, err := rs.cachedData(ctx, op.ID)
		if err != nil {
			return rs.recordFailure(ctx, op, err)
		}
		var result *operationResult
		if p.Snapshot != nil {
			if op.Name == "CompleteMultipartUpload" {
				abortErr := secondary.AbortMultipartUpload(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), storage.MustNewUploadId(replicaPayload.UploadID))
				var api smithy.APIError
				missing := errors.Is(abortErr, storage.ErrNoSuchKey) || (errors.As(abortErr, &api) && (api.ErrorCode() == "NoSuchUpload" || api.ErrorCode() == "NoSuchKey"))
				if abortErr != nil && !missing {
					reader.Close()
					return rs.recordFailure(ctx, op, abortErr)
				}
			}
			result, err = rs.writeSnapshot(ctx, op.ID, id, secondary, &replicaPayload, reader)
		} else if op.Name == "UploadPartCopy" {
			result, err = rs.apply(ctx, secondary, "UploadPart", &replicaPayload, reader)
		} else {
			result, err = rs.apply(ctx, secondary, op.Name, &replicaPayload, reader)
		}
		reader.Close()
		if err != nil {
			return rs.recordFailure(ctx, op, err)
		}
		if err := resolveWrittenVersion(ctx, secondary, op.Name, &replicaPayload, result); err != nil {
			return rs.recordFailure(ctx, op, err)
		}
		if barrier, ok := secondary.(interface {
			Synchronize(context.Context, storage.BucketName) error
		}); ok {
			if err := barrier.Synchronize(ctx, storage.MustNewBucketName(p.Bucket)); err != nil {
				return rs.recordFailure(ctx, op, err)
			}
		}
		if p.Snapshot != nil && primary.VersionID != nil && result.VersionID == nil {
			if *primary.VersionID != "null" {
				return rs.recordFailure(ctx, op, errors.New("replica did not return a version ID for a versioned write"))
			}
			nullVersion := "null"
			result.VersionID = &nullVersion
		}
		if p.Snapshot != nil {
			actual, err := secondary.HeadObject(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), &storage.HeadObjectOptions{VersionID: result.VersionID})
			if err != nil {
				return rs.recordFailure(ctx, op, err)
			}
			expected := p.Snapshot
			if actual.Size != expected.Size || actual.ETag != expected.ETag || !sameProtection(actual.ObjectLock, expected.ObjectLock) {
				return rs.recordFailure(ctx, op, errors.New("replica confirmation does not match source size, ETag or Object Lock"))
			}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		err = database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
			if primary.VersionID != nil && result.VersionID != nil {
				if err := rs.journal.SaveMapping(ctx, tx.SqlTx(), replicationjournal.Mapping{ReplicationID: rs.options.ReplicationID, SecondaryID: id, Bucket: p.Bucket, Key: p.Key, Kind: "VERSION", PrimaryID: *primary.VersionID, SecondaryObjectID: *result.VersionID}); err != nil {
					return err
				}
			}
			if primary.UploadID != "" && result.UploadID != "" {
				if err := rs.journal.SaveMapping(ctx, tx.SqlTx(), replicationjournal.Mapping{ReplicationID: rs.options.ReplicationID, SecondaryID: id, Bucket: p.Bucket, Key: p.Key, Kind: "UPLOAD", PrimaryID: primary.UploadID, SecondaryObjectID: result.UploadID}); err != nil {
					return err
				}
			}
			if err := rs.journal.Acknowledge(ctx, tx.SqlTx(), op.ID, id, string(encoded)); err != nil {
				return err
			}
			return rs.journal.DeleteProgress(ctx, tx.SqlTx(), op.ID, id)
		})
		if err != nil {
			return rs.recordFailure(ctx, op, err)
		}
	}
	op.State = "COMPLETE"
	op.LastError = ""
	return database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
			return err
		}
		return rs.journal.DeleteData(ctx, tx.SqlTx(), op.ID)
	})
}

func resolveWrittenVersion(ctx context.Context, target storage.Storage, name string, p *operationPayload, result *operationResult) error {
	if result == nil || result.VersionID != nil {
		return nil
	}
	switch name {
	case "PutObject", "CopyObject", "AppendObject", "CompleteMultipartUpload":
	default:
		return nil
	}
	obj, err := target.HeadObject(ctx, storage.MustNewBucketName(p.Bucket), storage.MustNewObjectKey(p.Key), nil)
	if err != nil {
		return err
	}
	result.VersionID = obj.VersionID
	if result.VersionID == nil {
		nullVersion := "null"
		result.VersionID = &nullVersion
	}
	return nil
}

func (rs *replicationStorage) translate(ctx context.Context, id string, p *operationPayload) error {
	translate := func(version **string) error {
		if *version == nil {
			return nil
		}
		v, err := rs.mapping(ctx, id, p.Bucket, p.Key, "VERSION", **version)
		if err != nil {
			return err
		}
		*version = &v
		return nil
	}
	if p.LockOptions != nil {
		if err := translate(&p.LockOptions.VersionID); err != nil {
			return err
		}
	}
	if p.TagOptions != nil {
		if err := translate(&p.TagOptions.VersionID); err != nil {
			return err
		}
	}
	if p.Delete != nil {
		if err := translate(&p.Delete.VersionID); err != nil {
			return err
		}
	}
	if p.Transition != nil {
		if err := translate(&p.Transition.VersionID); err != nil {
			return err
		}
	}
	if p.UploadID != "" {
		v, err := rs.mapping(ctx, id, p.Bucket, p.Key, "UPLOAD", p.UploadID)
		if err != nil {
			return err
		}
		p.UploadID = v
	}
	return nil
}

func (rs *replicationStorage) loadSnapshotProgress(ctx context.Context, operationID, secondaryID string) (*multipartSnapshotProgress, error) {
	var encoded *string
	err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		encoded, err = rs.journal.FindProgress(ctx, tx.SqlTx(), operationID, secondaryID)
		return err
	})
	if err != nil || encoded == nil {
		return nil, err
	}
	var progress multipartSnapshotProgress
	if err := json.Unmarshal([]byte(*encoded), &progress); err != nil {
		return nil, err
	}
	return &progress, nil
}

func (rs *replicationStorage) saveSnapshotProgress(ctx context.Context, operationID, secondaryID string, progress *multipartSnapshotProgress) error {
	encoded, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	return database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		return rs.journal.SaveProgress(ctx, tx.SqlTx(), operationID, secondaryID, string(encoded))
	})
}

func (rs *replicationStorage) writeSnapshot(ctx context.Context, operationID, secondaryID string, target storage.Storage, p *operationPayload, reader io.Reader) (*operationResult, error) {
	obj := p.Snapshot
	b := storage.MustNewBucketName(p.Bucket)
	k := storage.MustNewObjectKey(p.Key)
	opts := &storage.PutObjectOptions{ObjectLock: obj.ObjectLock, Tags: obj.Tags, Metadata: &obj.Metadata, StorageClass: obj.StorageClass}
	if len(obj.PartSizes) <= 1 && !strings.Contains(obj.ETag, "-") {
		algorithm := "SHA256"
		put, err := target.PutObject(ctx, b, k, obj.ContentType, reader, &storage.ChecksumInput{ChecksumAlgorithm: &algorithm, ChecksumSHA256: obj.ChecksumSHA256}, opts)
		if err != nil {
			return nil, err
		}
		return &operationResult{Put: put, VersionID: put.VersionID}, nil
	}
	if len(obj.PartSizes) == 0 {
		return nil, errors.New("multipart snapshot lacks part boundaries")
	}
	progress, err := rs.loadSnapshotProgress(ctx, operationID, secondaryID)
	if err != nil {
		return nil, err
	}
	if progress != nil && progress.Complete != nil {
		return progress.Complete, nil
	}
	if progress == nil {
		upload, err := target.CreateMultipartUpload(ctx, b, k, obj.ContentType, obj.ChecksumType, &storage.CreateMultipartUploadOptions{ObjectLock: obj.ObjectLock, Tags: obj.Tags, Metadata: &obj.Metadata, StorageClass: obj.StorageClass})
		if err != nil {
			return nil, err
		}
		progress = &multipartSnapshotProgress{UploadID: upload.UploadId.String()}
		if err := rs.saveSnapshotProgress(ctx, operationID, secondaryID, progress); err != nil {
			abortErr := target.AbortMultipartUpload(context.WithoutCancel(ctx), b, k, upload.UploadId)
			return nil, errors.Join(err, abortErr)
		}
	}
	if progress.UploadID == "" || len(progress.Parts) > len(obj.PartSizes) {
		return nil, errors.New("invalid durable multipart snapshot progress")
	}
	// Completion may have succeeded remotely before its result could be
	// journaled. Size, ETag and protection are not a unique operation identity,
	// so a HEAD response cannot safely identify the completed version.
	if progress.Completing {
		return nil, ErrIndeterminateReplicaCompletion
	}
	uploadID, err := storage.NewUploadId(progress.UploadID)
	if err != nil {
		return nil, err
	}
	for i := range progress.Parts {
		if progress.Parts[i].PartNumber != int32(i+1) {
			return nil, errors.New("non-contiguous durable multipart snapshot progress")
		}
		if _, err := io.CopyN(io.Discard, reader, obj.PartSizes[i]); err != nil {
			return nil, err
		}
	}
	for i := len(progress.Parts); i < len(obj.PartSizes); i++ {
		size := obj.PartSizes[i]
		cached, err := ioutils.NewSmartCachedReadSeekCloser(io.LimitReader(reader, size), maxMemoryCacheSize)
		if err != nil {
			return nil, err
		}
		part, err := target.UploadPart(ctx, b, k, uploadID, int32(i+1), cached, nil)
		cached.Close()
		if err != nil {
			return nil, err
		}
		progress.Parts = append(progress.Parts, storage.CompleteMultipartUploadPart{PartNumber: int32(i + 1), ETag: part.ETag, ChecksumCRC32: part.ChecksumCRC32, ChecksumCRC32C: part.ChecksumCRC32C, ChecksumCRC64NVME: part.ChecksumCRC64NVME, ChecksumSHA1: part.ChecksumSHA1, ChecksumSHA256: part.ChecksumSHA256})
		if err := rs.saveSnapshotProgress(ctx, operationID, secondaryID, progress); err != nil {
			return nil, err
		}
	}
	progress.Completing = true
	if err := rs.saveSnapshotProgress(ctx, operationID, secondaryID, progress); err != nil {
		return nil, err
	}
	complete, err := target.CompleteMultipartUpload(ctx, b, k, uploadID, nil, &storage.CompleteMultipartUploadOptions{Parts: progress.Parts})
	if err != nil {
		return nil, err
	}
	result := &operationResult{Complete: complete, VersionID: complete.VersionID}
	progress.Complete = result
	if err := rs.saveSnapshotProgress(ctx, operationID, secondaryID, progress); err != nil {
		return nil, err
	}
	return result, nil
}

func (rs *replicationStorage) apply(ctx context.Context, target storage.Storage, name string, p *operationPayload, data io.Reader) (*operationResult, error) {
	b, err := storage.NewBucketName(p.Bucket)
	if err != nil {
		return nil, err
	}
	k := storage.ObjectKey{}
	if p.Key != "" {
		k, err = storage.NewObjectKey(p.Key)
		if err != nil {
			return nil, err
		}
	}
	result := &operationResult{}
	switch name {
	case "PrepareReconcileBucket":
		if _, err = target.HeadBucket(ctx, b); errors.Is(err, storage.ErrNoSuchBucket) {
			err = target.CreateBucket(ctx, b, storage.CreateBucketOptions{ObjectLockEnabled: p.LockConfiguration != nil, OwnerAccountID: p.OwnerAccountID})
		}
		if err == nil && p.Versioning != nil && p.Versioning.Status != nil {
			status := storage.BucketVersioningStatusEnabled
			err = target.PutBucketVersioningConfiguration(ctx, b, &storage.BucketVersioningConfiguration{Status: &status})
		}
		if err == nil && p.LockConfiguration != nil {
			err = target.PutObjectLockConfiguration(ctx, b, &storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"})
		}
	case "CreateBucket":
		err = target.CreateBucket(ctx, b, p.Create...)
	case "DeleteBucket":
		err = target.DeleteBucket(ctx, b)
	case "PutObjectLockConfiguration":
		err = target.PutObjectLockConfiguration(ctx, b, p.LockConfiguration)
	case "PutBucketVersioningConfiguration":
		err = target.PutBucketVersioningConfiguration(ctx, b, p.Versioning)
	case "PutObjectRetention":
		err = target.PutObjectRetention(ctx, b, k, p.Retention, p.LockOptions)
	case "PutObjectLegalHold":
		err = target.PutObjectLegalHold(ctx, b, k, p.Hold, p.LockOptions)
	case "PutObjectTagging":
		err = target.PutObjectTagging(ctx, b, k, p.Tags, p.TagOptions)
	case "DeleteObjectTagging":
		err = target.DeleteObjectTagging(ctx, b, k, p.TagOptions)
	case "PutObject":
		result.Put, err = target.PutObject(ctx, b, k, p.ContentType, data, p.Checksum, p.Put)
		if err == nil {
			result.VersionID = result.Put.VersionID
		}
	case "CopyObject":
		result.Copy, err = target.CopyObject(ctx, storage.MustNewBucketName(p.SourceBucket), storage.MustNewObjectKey(p.SourceKey), b, k, p.Copy)
		if err == nil {
			result.VersionID = result.Copy.VersionID
		}
	case "AppendObject":
		result.Append, err = target.AppendObject(ctx, b, k, data, p.Checksum, p.Append)
		if err == nil {
			var head *storage.Object
			head, err = target.HeadObject(ctx, b, k, nil)
			if err == nil {
				result.VersionID = head.VersionID
			}
		}
	case "DeleteObject":
		result.Delete, err = target.DeleteObject(ctx, b, k, p.Delete)
		if err == nil {
			result.VersionID = result.Delete.VersionID
		}
	case "CreateMultipartUpload":
		var upload *storage.InitiateMultipartUploadResult
		upload, err = target.CreateMultipartUpload(ctx, b, k, p.ContentType, p.ChecksumType, p.Multipart)
		if err == nil {
			result.UploadID = upload.UploadId.String()
		}
	case "UploadPart":
		result.Part, err = target.UploadPart(ctx, b, k, storage.MustNewUploadId(p.UploadID), p.PartNumber, data, p.Checksum)
	case "UploadPartCopy":
		result.PartCopy, err = target.UploadPartCopy(ctx, storage.MustNewBucketName(p.SourceBucket), storage.MustNewObjectKey(p.SourceKey), b, k, storage.MustNewUploadId(p.UploadID), p.PartNumber, p.PartCopy)
	case "CompleteMultipartUpload":
		result.Complete, err = target.CompleteMultipartUpload(ctx, b, k, storage.MustNewUploadId(p.UploadID), p.Checksum, p.Complete)
		if err == nil {
			result.VersionID = result.Complete.VersionID
		}
	case "AbortMultipartUpload":
		err = target.AbortMultipartUpload(ctx, b, k, storage.MustNewUploadId(p.UploadID))
	case "TransitionObjectStorageClass":
		err = target.TransitionObjectStorageClass(ctx, b, k, p.StorageClass, p.Transition)
	case "PutBucketWebsiteConfiguration":
		err = target.PutBucketWebsiteConfiguration(ctx, b, p.Website)
	case "DeleteBucketWebsiteConfiguration":
		err = target.DeleteBucketWebsiteConfiguration(ctx, b)
	case "PutBucketCORSConfiguration":
		err = target.PutBucketCORSConfiguration(ctx, b, p.CORS)
	case "DeleteBucketCORSConfiguration":
		err = target.DeleteBucketCORSConfiguration(ctx, b)
	case "PutBucketLifecycleConfiguration":
		err = target.PutBucketLifecycleConfiguration(ctx, b, p.Lifecycle)
	case "DeleteBucketLifecycleConfiguration":
		err = target.DeleteBucketLifecycleConfiguration(ctx, b)
	case "PutBucketNotificationConfiguration":
		err = target.PutBucketNotificationConfiguration(ctx, b, p.Notification)
	default:
		err = fmt.Errorf("unsupported journal operation %s", name)
	}
	return result, err
}

func sameProtection(a, b storage.ObjectLock) bool {
	if (a.Retention == nil) != (b.Retention == nil) {
		return false
	}
	if a.Retention != nil && (a.Retention.Mode != b.Retention.Mode || !a.Retention.RetainUntilDate.Equal(b.Retention.RetainUntilDate)) {
		return false
	}
	held := func(lock storage.ObjectLock) bool {
		return lock.LegalHold != nil && *lock.LegalHold == storage.LegalHoldOn
	}
	return held(a) == held(b)
}

func definitiveRejection(err error) bool {
	for _, known := range []error{storage.ErrNoSuchBucket, storage.ErrNoSuchKey, storage.ErrObjectLockAccessDenied, storage.ErrInvalidObjectLockConfiguration, storage.ErrPreconditionFailed, storage.ErrBucketAlreadyExists, storage.ErrBucketNotEmpty} {
		if errors.Is(err, known) {
			return true
		}
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "AccessDenied", "NoSuchBucket", "NoSuchKey", "NoSuchVersion", "NoSuchUpload", "InvalidRequest", "InvalidArgument", "BadDigest", "InvalidDigest", "PreconditionFailed", "BucketAlreadyExists", "BucketAlreadyOwnedByYou", "BucketNotEmpty":
			return true
		}
	}
	return false
}
func (rs *replicationStorage) finishRejected(ctx context.Context, op *replicationjournal.Operation, cause error) error {
	op.State = "COMPLETE"
	op.LastError = cause.Error()
	op.Attempts++
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	err := database.WithTx(saveCtx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
			return err
		}
		return rs.journal.DeleteData(ctx, tx.SqlTx(), op.ID)
	})
	return errors.Join(cause, err)
}

func (rs *replicationStorage) finishIndeterminatePrimary(ctx context.Context, op *replicationjournal.Operation, cause error) error {
	err := rs.markIndeterminatePrimary(ctx, op, cause)
	return errors.Join(fmt.Errorf("%w: operation %s: %v", ErrIndeterminatePrimary, op.ID, cause), err)
}

func (rs *replicationStorage) markIndeterminatePrimary(ctx context.Context, op *replicationjournal.Operation, cause error) error {
	op.State = "COMPLETE"
	op.PrimaryResult = nil
	op.LastError = fmt.Sprintf("%v: %v", ErrIndeterminatePrimary, cause)
	op.Attempts++
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return database.WithTx(saveCtx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
			return err
		}
		return rs.journal.DeleteData(ctx, tx.SqlTx(), op.ID)
	})
}
