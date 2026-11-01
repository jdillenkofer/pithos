package sql

import (
	"bytes"
	"context"
	"database/sql"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	partContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type sqlPartStore struct {
	*lifecycle.ValidatedLifecycle
	db                    database.Database
	partContentRepository partContent.Repository
	partStoreId           string
	tracer                trace.Tracer
}

// Compile-time check to ensure sqlPartStore implements partstore.PartStore
var _ partstore.PartStore = (*sqlPartStore)(nil)
var _ partstore.StatsProvider = (*sqlPartStore)(nil)

func (bs *sqlPartStore) SupportsStats() bool { return true }

type Option func(*sqlPartStore)

func WithPartStoreId(partStoreId string) Option {
	return func(bs *sqlPartStore) {
		if partStoreId != "" {
			bs.partStoreId = partStoreId
		}
	}
}

func New(db database.Database, partContentRepository partContent.Repository, opts ...Option) (partstore.PartStore, error) {
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("sqlPartStore")
	if err != nil {
		return nil, err
	}
	bs := &sqlPartStore{
		ValidatedLifecycle:    validatedLifecycle,
		db:                    db,
		partContentRepository: partContentRepository,
		partStoreId:           partContent.DefaultPartStoreId,
		tracer:                otel.Tracer("internal/storage/metadatapart/partstore/sql"),
	}
	for _, opt := range opts {
		opt(bs)
	}
	return bs, nil
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (bs *sqlPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.PutPart")
	defer span.End()

	// Delete existing content first to avoid stale chunks if overwriting
	err := bs.partContentRepository.DeletePartContentById(ctx, tx.SqlTx(), bs.partStoreId, partId)
	if err != nil {
		return err
	}

	chunkIndex := 0
	for {
		content, err := ioutils.ReadChunk(reader, chunkSize)
		if len(content) > 0 {
			partContentEntity := partContent.Entity{
				Id:         ptrutils.ToPtr(partId),
				ChunkIndex: chunkIndex,
				Content:    content,
			}
			if saveErr := bs.partContentRepository.SavePartContent(ctx, tx.SqlTx(), bs.partStoreId, &partContentEntity); saveErr != nil {
				return saveErr
			}
			chunkIndex++
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return nil
}

func (bs *sqlPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPart")
	defer span.End()

	// Fetch the first chunk eagerly to preserve ErrPartNotFound semantics; the
	// remaining chunks are loaded lazily so we never hold more than one chunk in
	// memory at a time.
	reader := &lazyChunkReadCloser{
		ctx:         ctx,
		db:          bs.db,
		tx:          tx,
		repo:        bs.partContentRepository,
		partStoreId: bs.partStoreId,
		partId:      partId,
	}
	firstChunk, err := reader.findChunk(0)
	if err != nil {
		return nil, err
	}
	if firstChunk == nil {
		return nil, partstore.ErrPartNotFound
	}

	reader.nextChunk = 1
	reader.current = bytes.NewReader(firstChunk.Content)
	return reader, nil
}

// Capabilities advertises tx-free reads: each chunk is then read in a short
// transaction of its own. Content under a part id is only rewritten with the
// same logical content (an erasure-coding repair, for example), and GC
// deletes it only once no reader lease protects it, so the chunks of one
// read need not share a snapshot.
func (bs *sqlPartStore) Capabilities() partstore.Capabilities {
	return partstore.NewCapabilities(partstore.CapabilityTxFreeGetPart)
}

// lazyChunkReadCloser streams a part's chunks one at a time, querying the next
// chunk from the repository only once the current one is exhausted. This keeps
// the read path's memory proportional to a single chunk rather than the whole
// part. With a transaction, it relies on the transaction outliving the reader;
// without one, every chunk is read in a transaction of its own.
type lazyChunkReadCloser struct {
	ctx         context.Context
	db          database.Database
	tx          database.Tx
	repo        partContent.Repository
	partStoreId string
	partId      partstore.PartId
	nextChunk   int
	current     *bytes.Reader
	done        bool
}

func (l *lazyChunkReadCloser) Read(p []byte) (int, error) {
	for {
		if l.current != nil {
			n, err := l.current.Read(p)
			if err == io.EOF {
				l.current = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		if l.done {
			return 0, io.EOF
		}
		chunk, err := l.findChunk(l.nextChunk)
		if err != nil {
			return 0, err
		}
		if chunk == nil {
			l.done = true
			return 0, io.EOF
		}
		l.nextChunk++
		l.current = bytes.NewReader(chunk.Content)
	}
}

func (l *lazyChunkReadCloser) findChunk(index int) (*partContent.Entity, error) {
	if l.tx != nil {
		return l.repo.FindPartContentChunkByIndex(l.ctx, l.tx.SqlTx(), l.partStoreId, l.partId, index)
	}
	var chunk *partContent.Entity
	err := database.WithTx(l.ctx, l.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var err error
		chunk, err = l.repo.FindPartContentChunkByIndex(ctx, tx.SqlTx(), l.partStoreId, l.partId, index)
		return err
	})
	return chunk, err
}

func (l *lazyChunkReadCloser) Close() error {
	l.done = true
	l.current = nil
	return nil
}

func (bs *sqlPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPartIds")
	defer span.End()

	return bs.partContentRepository.FindPartContentIds(ctx, tx.SqlTx(), bs.partStoreId)
}

func (bs *sqlPartStore) Stats(ctx context.Context, tx database.Tx) (partstore.Stats, error) {
	var stats partstore.Stats
	err := tx.SqlTx().QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT id), COALESCE(SUM(LENGTH(content)), 0)
		FROM part_contents
		WHERE part_store_id = $1
	`, bs.partStoreId).Scan(&stats.Parts, &stats.Bytes)
	return stats, err
}

func (bs *sqlPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.DeletePart")
	defer span.End()

	err := bs.partContentRepository.DeletePartContentById(ctx, tx.SqlTx(), bs.partStoreId, partId)
	return err
}
