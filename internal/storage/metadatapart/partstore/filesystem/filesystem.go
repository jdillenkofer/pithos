package filesystem

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type filesystemPartStore struct {
	*lifecycle.ValidatedLifecycle
	root   string
	tracer trace.Tracer
}

// Compile-time check to ensure filesystemPartStore implements partstore.PartStore
var _ partstore.PartStore = (*filesystemPartStore)(nil)
var _ partstore.StatsProvider = (*filesystemPartStore)(nil)

func (bs *filesystemPartStore) SupportsStats() bool { return true }

func syncDirectory(path string) error {
	return ioutils.SyncDirectory(path)
}

func (bs *filesystemPartStore) ensureRootDir() error {
	return ioutils.EnsureDurableDirectory(bs.root, os.ModePerm)
}

func (bs *filesystemPartStore) getFilename(partId partstore.PartId) string {
	partFilename := hex.EncodeToString(partId.Bytes())
	return filepath.Join(bs.root, partFilename)
}

func (bs *filesystemPartStore) tryGetPartIdFromFilename(filename string) (partId *partstore.PartId, ok bool) {
	if len(filename) != 32 {
		return nil, false
	}
	partIdBytes, err := hex.DecodeString(filename)
	if err != nil {
		return nil, false
	}
	partId, err = partstore.NewPartIdFromBytes(partIdBytes)
	if err != nil {
		return nil, false
	}
	return partId, true
}

func New(root string) (partstore.PartStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("filesystemPartStore")
	if err != nil {
		return nil, err
	}
	bs := &filesystemPartStore{
		ValidatedLifecycle: validatedLifecycle,
		root:               root,
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/filesystem"),
	}
	return bs, nil
}

func (bs *filesystemPartStore) Start(ctx context.Context) error {
	if err := bs.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	if err := bs.ensureRootDir(); err != nil {
		_ = bs.ValidatedLifecycle.Stop(ctx)
		return err
	}
	if err := bs.cleanupTemporaryParts(); err != nil {
		_ = bs.ValidatedLifecycle.Stop(ctx)
		return err
	}
	return nil
}

func (bs *filesystemPartStore) cleanupTemporaryParts() error {
	entries, err := os.ReadDir(bs.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !partstore.IsTemporaryPartFilename(entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(bs.root, entry.Name())); err != nil {
			return err
		}
	}
	return syncDirectory(bs.root)
}

func (bs *filesystemPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.PutPart")
	defer span.End()

	filename := bs.getFilename(partId)
	if tx != nil {
		// Part ids are fresh random ids, so the part usually does not exist
		// yet. Nothing can reference it before commit, so write it in place
		// and remove it on rollback, saving the temp file and both renames.
		// An existing part is replaced atomically via the temp file below.
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, err = ioutils.Copy(f, reader)
			if err == nil {
				err = ioutils.SyncFile(f)
			}
			err = errors.Join(err, f.Close())
			if err == nil {
				err = syncDirectory(bs.root)
			}
			if err != nil {
				_ = os.Remove(filename)
				return err
			}
			tx.OnRollback(func(context.Context) error {
				if err := os.Remove(filename); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
				return nil
			})
			return nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
	}

	// A part ID always represents the same logical content. Replays and repairs
	// can replace its physical representation, even if the surrounding metadata
	// transaction rolls back. Never delete a pre-existing part on rollback or
	// truncate it in place: publish only a completely written, synced file.
	tempName := filepath.Join(bs.root, partstore.NewTemporaryPartFilename(partId))
	tempFile, err := os.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(tempName)
	_, err = ioutils.Copy(tempFile, reader)
	if err == nil {
		err = ioutils.SyncFile(tempFile)
	}
	err = errors.Join(err, tempFile.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(tempName, filename); err != nil {
		return err
	}
	return syncDirectory(bs.root)
}

func (bs *filesystemPartStore) Capabilities() partstore.Capabilities {
	return partstore.NewCapabilities(
		partstore.CapabilityTxFreeGetPart,
		partstore.CapabilityTxFreePutPart,
		partstore.CapabilityTxFreeDeletePart,
	)
}

func (bs *filesystemPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.GetPart")
	defer span.End()

	filename := bs.getFilename(partId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, partstore.ErrPartNotFound
		}
		return nil, err
	}
	return f, err
}

func (bs *filesystemPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.GetPartIds")
	defer span.End()

	dirEntries, err := os.ReadDir(bs.root)
	if err != nil {
		return nil, err
	}
	partIds := []partstore.PartId{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		if partId, ok := bs.tryGetPartIdFromFilename(dirEntry.Name()); ok {
			partIds = append(partIds, *partId)
		}
	}
	return partIds, nil
}

func (bs *filesystemPartStore) Stats(ctx context.Context, tx database.Tx) (partstore.Stats, error) {
	dirEntries, err := os.ReadDir(bs.root)
	if err != nil {
		return partstore.Stats{}, err
	}
	var stats partstore.Stats
	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		if _, ok := bs.tryGetPartIdFromFilename(entry.Name()); !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return partstore.Stats{}, err
		}
		stats.Parts++
		stats.Bytes += info.Size()
	}
	return stats, nil
}

func (bs *filesystemPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.DeletePart")
	defer span.End()

	filename := bs.getFilename(partId)
	remove := func(context.Context) error {
		if err := os.Remove(filename); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return syncDirectory(bs.root)
	}
	if tx != nil {
		// Metadata controls visibility. Keep the data until that deletion has
		// committed, so a crash or rollback cannot destroy a referenced part.
		tx.OnAfterCommit(remove)
		return nil
	}
	return remove(ctx)
}
