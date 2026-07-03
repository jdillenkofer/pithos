package sql

import (
	"bytes"
	"context"
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
	partContentRepository partContent.Repository
	tracer                trace.Tracer
}

// Compile-time check to ensure sqlPartStore implements partstore.PartStore
var _ partstore.PartStore = (*sqlPartStore)(nil)

func New(db database.Database, partContentRepository partContent.Repository) (partstore.PartStore, error) {
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("sqlPartStore")
	if err != nil {
		return nil, err
	}
	return &sqlPartStore{
		ValidatedLifecycle:    validatedLifecycle,
		partContentRepository: partContentRepository,
		tracer:                otel.Tracer("internal/storage/metadatapart/partstore/sql"),
	}, nil
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (bs *sqlPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.PutPart")
	defer span.End()

	// Delete existing content first to avoid stale chunks if overwriting
	err := bs.partContentRepository.DeletePartContentById(ctx, tx.SqlTx(), partId)
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
			if saveErr := bs.partContentRepository.SavePartContent(ctx, tx.SqlTx(), &partContentEntity); saveErr != nil {
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
	firstChunk, err := bs.partContentRepository.FindPartContentChunkByIndex(ctx, tx.SqlTx(), partId, 0)
	if err != nil {
		return nil, err
	}
	if firstChunk == nil {
		return nil, partstore.ErrPartNotFound
	}

	return &lazyChunkReadCloser{
		ctx:       ctx,
		tx:        tx,
		repo:      bs.partContentRepository,
		partId:    partId,
		nextChunk: 1,
		current:   bytes.NewReader(firstChunk.Content),
	}, nil
}

// lazyChunkReadCloser streams a part's chunks one at a time, querying the next
// chunk from the repository only once the current one is exhausted. This keeps
// the read path's memory proportional to a single chunk rather than the whole
// part. It relies on the read transaction outliving the reader (GetObject binds
// the tx lifetime to its returned readers).
type lazyChunkReadCloser struct {
	ctx       context.Context
	tx        database.Tx
	repo      partContent.Repository
	partId    partstore.PartId
	nextChunk int
	current   *bytes.Reader
	done      bool
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
		chunk, err := l.repo.FindPartContentChunkByIndex(l.ctx, l.tx.SqlTx(), l.partId, l.nextChunk)
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

func (l *lazyChunkReadCloser) Close() error {
	l.done = true
	l.current = nil
	return nil
}

func (bs *sqlPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPartIds")
	defer span.End()

	return bs.partContentRepository.FindPartContentIds(ctx, tx.SqlTx())
}

func (bs *sqlPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.DeletePart")
	defer span.End()

	err := bs.partContentRepository.DeletePartContentById(ctx, tx.SqlTx(), partId)
	return err
}
