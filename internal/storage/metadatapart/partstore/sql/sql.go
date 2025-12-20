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

func (bs *sqlPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.PutPart")
	defer span.End()

	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	partContentEntity := partContent.Entity{
		Id:      ptrutils.ToPtr(partId),
		Content: content,
	}
	err = bs.partContentRepository.PutPartContent(ctx, tx, &partContentEntity)
	if err != nil {
		return err
	}

	return nil
}

func (bs *sqlPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := bs.tracer.Start(ctx, "sqlPartStore.GetPart")
	defer span.End()

	partContentEntity, err := bs.partContentRepository.FindPartContentById(ctx, tx, partId)
	if err != nil {
		return nil, err
	}
	if partContentEntity == nil {
		return nil, partstore.ErrPartNotFound
	}
	reader := ioutils.NewByteReadSeekCloser(partContentEntity.Content)

	return reader, nil
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
