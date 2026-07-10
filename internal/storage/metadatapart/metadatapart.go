package metadatapart

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
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

func (mbs *metadataPartStorage) deleteUnreferencedParts(ctx context.Context, tx database.Tx, parts []metadatastore.Part) error {
	for _, part := range parts {
		store, err := mbs.partStores.ByName(part.StoreName)
		if err != nil {
			return err
		}
		if err := store.DeletePart(ctx, tx, part.Id); err != nil {
			return err
		}
	}
	return nil
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
	partRegistryRepository, err := repositoryfactory.NewPartRegistryRepository(db)
	if err != nil {
		return nil, err
	}
	partGC, err := gc.New(db, metadataStore, partStores, partRegistryRepository)
	if err != nil {
		return nil, err
	}
	db = gc.NewProtectedDatabase(db, partGC)
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

func (mbs *metadataPartStorage) Database() database.Database {
	return mbs.db
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
