package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/oklog/ulid/v2"
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

func (bs *filesystemPartStore) ensureRootDir() error {
	err := os.MkdirAll(bs.root, os.ModePerm)
	return err
}

func (bs *filesystemPartStore) getFilename(partId partstore.PartId) string {
	partFilename := partstore.PartFilename(partId)
	return filepath.Join(bs.root, partstore.ShardDirName(partFilename), partFilename)
}

// migrateLegacyParts moves part files from the flat pre-sharding layout
// (<root>/<hex>) into their shard directories (<root>/<xx>/<hex>). It runs on
// every start and is a no-op once no legacy files remain, so an interrupted
// migration resumes on the next start.
func (bs *filesystemPartStore) migrateLegacyParts() error {
	dirEntries, err := os.ReadDir(bs.root)
	if err != nil {
		return err
	}
	migrated := 0
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}
		name := dirEntry.Name()
		if _, ok := partstore.TryGetPartIdFromFilename(name); !ok {
			continue
		}
		shardDir := filepath.Join(bs.root, partstore.ShardDirName(name))
		if err := os.MkdirAll(shardDir, os.ModePerm); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(bs.root, name), filepath.Join(shardDir, name)); err != nil {
			return err
		}
		migrated++
	}
	if migrated > 0 {
		slog.Info(fmt.Sprintf("Migrated %d part files to sharded directory layout in %s", migrated, bs.root))
	}
	return nil
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
		return err
	}
	return bs.migrateLegacyParts()
}

func (bs *filesystemPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.PutPart")
	defer span.End()

	filename := bs.getFilename(partId)
	shardDir := filepath.Dir(filename)
	if err := os.MkdirAll(shardDir, os.ModePerm); err != nil {
		return err
	}
	if tx != nil {
		tempFile, err := os.CreateTemp(shardDir, "."+filepath.Base(filename)+".*.tmp")
		if err != nil {
			return err
		}
		tempName := tempFile.Name()
		if _, err = ioutils.Copy(tempFile, reader); err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempName)
			return err
		}
		if err = tempFile.Close(); err != nil {
			_ = os.Remove(tempName)
			return err
		}

		backupName := filename + ".txbackup." + ulid.Make().String()
		backupCreated := false
		published := false
		tx.OnPreCommit(func(context.Context) error {
			if err := os.Rename(filename, backupName); err == nil {
				backupCreated = true
			} else if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			if err := os.Rename(tempName, filename); err != nil {
				if backupCreated {
					_ = os.Rename(backupName, filename)
					backupCreated = false
				}
				return err
			}
			published = true
			return nil
		})
		tx.OnAfterCommit(func(context.Context) error {
			if backupCreated {
				return os.Remove(backupName)
			}
			return nil
		})
		tx.OnRollback(func(context.Context) error {
			if published {
				_ = os.Remove(filename)
				if backupCreated {
					return os.Rename(backupName, filename)
				}
			}
			if !published {
				_ = os.Remove(tempName)
			}
			return nil
		})
		return nil
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = ioutils.Copy(f, reader)
	if err != nil {
		return err
	}

	return nil
}

// SupportsTxFreeGetPart reports that GetPart never uses the transaction.
func (bs *filesystemPartStore) SupportsTxFreeGetPart() bool {
	return true
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
		name := dirEntry.Name()
		if dirEntry.IsDir() {
			if !partstore.IsShardDirName(name) {
				continue
			}
			shardEntries, err := os.ReadDir(filepath.Join(bs.root, name))
			if err != nil {
				return nil, err
			}
			for _, shardEntry := range shardEntries {
				if shardEntry.IsDir() {
					continue
				}
				if partId, ok := partstore.TryGetPartIdFromFilename(shardEntry.Name()); ok {
					partIds = append(partIds, *partId)
				}
			}
			continue
		}
		// Legacy flat layout files only exist until Start's migration has run.
		if partId, ok := partstore.TryGetPartIdFromFilename(name); ok {
			partIds = append(partIds, *partId)
		}
	}
	return partIds, nil
}

func (bs *filesystemPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.DeletePart")
	defer span.End()

	filename := bs.getFilename(partId)
	if tx != nil {
		backupName := filename + ".txbackup." + ulid.Make().String()
		backupCreated := false
		tx.OnPreCommit(func(context.Context) error {
			if err := os.Rename(filename, backupName); err == nil {
				backupCreated = true
				return nil
			} else if errors.Is(err, fs.ErrNotExist) {
				return nil
			} else {
				return err
			}
		})
		tx.OnAfterCommit(func(context.Context) error {
			if backupCreated {
				return os.Remove(backupName)
			}
			return nil
		})
		tx.OnRollback(func(context.Context) error {
			if backupCreated {
				return os.Rename(backupName, filename)
			}
			return nil
		})
		return nil
	}

	err := os.Remove(filename)
	if err != nil {
		e, ok := err.(*os.PathError)
		if ok && e.Err == syscall.ENOENT {
			// The file didn't exist
		} else {
			return err
		}
	}
	return nil
}
