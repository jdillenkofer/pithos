package filesystem

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
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
	return bs.ensureRootDir()
}

func (bs *filesystemPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.PutPart")
	defer span.End()

	filename := bs.getFilename(partId)

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

func (bs *filesystemPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
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

func (bs *filesystemPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
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

func (bs *filesystemPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	_, span := bs.tracer.Start(ctx, "filesystemPartStore.DeletePart")
	defer span.End()

	filename := bs.getFilename(partId)
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
