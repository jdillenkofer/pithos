package sql

import (
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

const readChunkInitialCap = 128 * 1024 // 128KB

// readChunk reads up to max bytes from r into a freshly allocated, caller-owned
// slice. The buffer grows geometrically up to max so small parts don't allocate
// the full chunk size. It returns io.EOF once r is exhausted; a nil error means
// max bytes were read and more may remain.
func readChunk(r io.Reader, max int) ([]byte, error) {
	buf := make([]byte, 0, min(readChunkInitialCap, max))
	for len(buf) < max {
		if len(buf) == cap(buf) {
			newCap := min(cap(buf)*2, max)
			grown := make([]byte, len(buf), newCap)
			copy(grown, buf)
			buf = grown
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

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
		content, err := readChunk(reader, chunkSize)
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

	chunks, err := bs.partContentRepository.FindPartContentChunksById(ctx, tx.SqlTx(), partId)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, partstore.ErrPartNotFound
	}

	readers := make([]io.Reader, len(chunks))
	for i, chunk := range chunks {
		readers[i] = ioutils.NewByteReadSeekCloser(chunk.Content)
	}

	return io.NopCloser(io.MultiReader(readers...)), nil
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
