package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/replicationjournal"
	"github.com/oklog/ulid/v2"
)

// Reconcile requires all external writers and lifecycle processing to be paused.
// A bucket's complete ordered work list and source bytes are committed before
// any replica is changed, including the restoration of bucket defaults.
func (rs *replicationStorage) Reconcile(ctx context.Context, buckets []storage.BucketName, dryRun bool) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !dryRun {
		if err := rs.discardIndeterminateReplicaCompletions(ctx); err != nil {
			return err
		}
		if err := rs.replayPending(ctx); err != nil {
			return err
		}
	}
	for _, bucket := range buckets {
		if err := rs.reconcileBucket(ctx, bucket, dryRun); err != nil {
			return err
		}
	}
	return nil
}

// An indeterminate snapshot completion cannot be identified safely from object
// attributes. Reconciliation supersedes it by rebuilding any missing mapping
// from the authoritative primary version list.
func (rs *replicationStorage) discardIndeterminateReplicaCompletions(ctx context.Context) error {
	var ops []replicationjournal.Operation
	if err := database.WithTx(ctx, rs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		ops, err = rs.journal.Pending(ctx, tx.SqlTx(), rs.options.ReplicationID)
		return err
	}); err != nil {
		return err
	}
	for i := range ops {
		op := &ops[i]
		if !strings.Contains(op.LastError, ErrIndeterminateReplicaCompletion.Error()) {
			continue
		}
		op.State = "COMPLETE"
		if err := database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
			if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
				return err
			}
			for _, id := range rs.options.SecondaryIDs {
				if err := rs.journal.DeleteProgress(ctx, tx.SqlTx(), op.ID, id); err != nil {
					return err
				}
			}
			return rs.journal.DeleteData(ctx, tx.SqlTx(), op.ID)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (rs *replicationStorage) reconcileBucket(ctx context.Context, bucket storage.BucketName, dryRun bool) error {
	var versions []storage.ObjectVersion
	opts := storage.ListObjectVersionsOptions{MaxKeys: 1000}
	for {
		page, err := rs.Next.ListObjectVersions(ctx, bucket, opts)
		if err != nil {
			return err
		}
		versions = append(versions, page.Versions...)
		if !page.IsTruncated {
			break
		}
		opts.KeyMarker, opts.VersionIDMarker = page.NextKeyMarker, page.NextVersionIDMarker
	}
	// ListObjectVersions returns each key newest first. Reverse the stable order
	// to preserve ties, then put IsLatest last even if timestamps are identical.
	for i, j := 0, len(versions)-1; i < j; i, j = i+1, j-1 {
		versions[i], versions[j] = versions[j], versions[i]
	}
	sort.SliceStable(versions, func(i, j int) bool {
		a, b := versions[i], versions[j]
		if a.Key.String() != b.Key.String() {
			return a.Key.String() < b.Key.String()
		}
		if a.IsLatest != b.IsLatest {
			return !a.IsLatest
		}
		return a.LastModified.Before(b.LastModified)
	})
	missing := make([]map[string]bool, len(versions))
	changedKeys := make(map[string]map[string]bool)
	count := 0
	for i, version := range versions {
		missing[i] = make(map[string]bool)
		for _, id := range rs.options.SecondaryIDs {
			_, err := rs.mapping(ctx, id, bucket.String(), version.Key.String(), "VERSION", version.VersionID)
			if err == nil {
				continue
			}
			if !errors.Is(err, ErrMissingMapping) {
				return err
			}
			missing[i][id] = true
			if changedKeys[version.Key.String()] == nil {
				changedKeys[version.Key.String()] = make(map[string]bool)
			}
			changedKeys[version.Key.String()][id] = true
			count++
			slog.InfoContext(ctx, "Reconcile missing version", "replication_id", rs.options.ReplicationID, "bucket", bucket.String(), "key", version.Key.String(), "version_id", version.VersionID, "secondary_id", id, "dry_run", dryRun)
		}
	}
	if count == 0 || dryRun {
		return nil
	}
	// Re-create an already mapped latest version when adding older history would
	// otherwise change which version is current. Existing replica versions stay.
	for i, version := range versions {
		if version.IsLatest {
			for id := range changedKeys[version.Key.String()] {
				missing[i][id] = true
			}
		}
	}
	sourceConfig, err := rs.Next.GetObjectLockConfiguration(ctx, bucket)
	if err != nil && !errors.Is(err, storage.ErrObjectLockConfigurationNotFound) {
		return err
	}
	sourceVersioning, err := rs.Next.GetBucketVersioningConfiguration(ctx, bucket)
	if err != nil {
		return err
	}
	sourceBucket, err := rs.Next.HeadBucket(ctx, bucket)
	if err != nil {
		return err
	}
	restore := make(map[string]*storage.ObjectLockConfiguration)
	for i, id := range rs.options.SecondaryIDs {
		targetConfig, err := rs.secondaryStorages[i].GetObjectLockConfiguration(ctx, bucket)
		if err != nil && !errors.Is(err, storage.ErrObjectLockConfigurationNotFound) && !errors.Is(err, storage.ErrNoSuchBucket) {
			return err
		}
		restore[id] = targetConfig
		if sourceConfig != nil {
			restore[id] = sourceConfig
		}
	}
	err = database.WithTx(ctx, rs.db, nil, func(ctx context.Context, tx database.Tx) error {
		if err := rs.journal.RegisterTopology(ctx, tx.SqlTx(), rs.options.ReplicationID, rs.options.SecondaryIDs); err != nil {
			return err
		}
		save := func(name string, p operationPayload, result operationResult, targets map[string]bool, data io.Reader) error {
			payload, err := json.Marshal(p)
			if err != nil {
				return err
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				return err
			}
			resultJSON := string(encoded)
			op := &replicationjournal.Operation{ID: ulid.Make().String(), ReplicationID: rs.options.ReplicationID, Bucket: bucket.String(), Key: p.Key, Name: name, Payload: string(payload), PrimaryResult: &resultJSON, State: "REPLICATING"}
			if err := rs.journal.SaveOperation(ctx, tx.SqlTx(), op); err != nil {
				return err
			}
			if data == nil {
				data = strings.NewReader("")
			}
			if err := rs.journal.SaveData(ctx, tx.SqlTx(), op.ID, data); err != nil {
				return err
			}
			for _, id := range rs.options.SecondaryIDs {
				if !targets[id] {
					if err := rs.journal.Acknowledge(ctx, tx.SqlTx(), op.ID, id, "{}"); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for _, id := range rs.options.SecondaryIDs {
			if err := save("PrepareReconcileBucket", operationPayload{Bucket: bucket.String(), OwnerAccountID: sourceBucket.OwnerAccountID, LockConfiguration: restore[id], Versioning: sourceVersioning}, operationResult{}, map[string]bool{id: true}, nil); err != nil {
				return err
			}
		}
		for i, version := range versions {
			if len(missing[i]) == 0 {
				continue
			}
			p := operationPayload{Bucket: bucket.String(), Key: version.Key.String()}
			result := operationResult{VersionID: &version.VersionID}
			if version.IsDeleteMarker {
				if err := save("DeleteObject", p, result, missing[i], nil); err != nil {
					return err
				}
				continue
			}
			obj, readers, err := rs.Next.GetObject(ctx, bucket, version.Key, nil, &storage.GetObjectOptions{VersionID: &version.VersionID})
			if err != nil {
				return err
			}
			if len(readers) != 1 {
				for _, r := range readers {
					r.Close()
				}
				return errors.New("invalid reconciliation snapshot")
			}
			p.Snapshot = obj
			err = save("PutObject", p, result, missing[i], readers[0])
			closeErr := readers[0].Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		if sourceVersioning != nil && sourceVersioning.Status != nil {
			targets := make(map[string]bool)
			for _, secondaries := range changedKeys {
				for id := range secondaries {
					targets[id] = true
				}
			}
			if err := save("PutBucketVersioningConfiguration", operationPayload{Bucket: bucket.String(), Versioning: sourceVersioning}, operationResult{}, targets, nil); err != nil {
				return err
			}
		}
		for _, id := range rs.options.SecondaryIDs {
			if restore[id] != nil {
				if err := save("PutObjectLockConfiguration", operationPayload{Bucket: bucket.String(), LockConfiguration: restore[id]}, operationResult{}, map[string]bool{id: true}, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return rs.replayPending(ctx)
}

// ReconcileStorage locates the explicitly selected topology through middleware.
func ReconcileStorage(ctx context.Context, root storage.Storage, id string, buckets []storage.BucketName, dryRun bool) error {
	for root != nil {
		if rs, ok := root.(*replicationStorage); ok {
			if rs.options.ReplicationID == id {
				if len(buckets) == 0 {
					list, err := rs.Next.ListBuckets(ctx)
					if err != nil {
						return err
					}
					for _, bucket := range list {
						buckets = append(buckets, bucket.Name)
					}
				}
				return rs.Reconcile(ctx, buckets, dryRun)
			}
			for _, secondary := range rs.secondaryStorages {
				if err := ReconcileStorage(ctx, secondary, id, buckets, dryRun); !errors.Is(err, ErrTopologyNotFound) {
					return err
				}
			}
		}
		if parent, ok := root.(interface{ StorageChildren() []storage.Storage }); ok {
			for _, child := range parent.StorageChildren() {
				if err := ReconcileStorage(ctx, child, id, buckets, dryRun); !errors.Is(err, ErrTopologyNotFound) {
					return err
				}
			}
			return ErrTopologyNotFound
		}
		wrapper, ok := root.(interface{ Unwrap() storage.Storage })
		if !ok {
			break
		}
		root = wrapper.Unwrap()
	}
	return ErrTopologyNotFound
}

var ErrTopologyNotFound = errors.New("replication topology not found in storage configuration")
