package sql

import (
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

const chunkSize = 64 * 1024 * 1024 // 64MB

func (bs *sqlPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.PutPart")
	defer span.End()

	// Delete existing content first to avoid stale chunks if overwriting
	err := bs.partContentRepository.DeletePartContentById(ctx, tx, partId)
	if err != nil {
		return err
	}

	buffer := make([]byte, chunkSize)
	chunkIndex := 0
	for {
		n, err := io.ReadFull(reader, buffer)
		if n > 0 {
			// Create a copy of the read bytes to store
			content := make([]byte, n)
			copy(content, buffer[:n])

			partContentEntity := partContent.Entity{
				Id:         ptrutils.ToPtr(partId),
				ChunkIndex: chunkIndex,
				Content:    content,
			}
			err = bs.partContentRepository.SavePartContent(ctx, tx, &partContentEntity)
			if err != nil {
				return err
			}
			chunkIndex++
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
	}

	return nil
}

func (bs *sqlPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPart")
	defer span.End()

	chunks, err := bs.partContentRepository.FindPartContentChunksById(ctx, tx, partId)
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

func (bs *sqlPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPartIds")
	defer span.End()

	return bs.partContentRepository.FindPartContentIds(ctx, tx)
}

func (bs *sqlPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.DeletePart")
	defer span.End()

	err := bs.partContentRepository.DeletePartContentById(ctx, tx, partId)
	return err
}
