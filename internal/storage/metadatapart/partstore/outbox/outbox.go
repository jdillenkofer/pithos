package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	partOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/partoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/outboxruntime"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type outboxPartStore struct {
	db                         database.Database
	runtime                    *outboxruntime.Runtime[*partOutboxEntry.Entity]
	innerPartStore             partstore.PartStore
	partOutboxEntryRepository  partOutboxEntry.Repository
	tracer                     trace.Tracer
}

// Compile-time check to ensure outboxPartStore implements partstore.PartStore
var _ partstore.PartStore = (*outboxPartStore)(nil)

func New(db database.Database, innerPartStore partstore.PartStore, partOutboxEntryRepository partOutboxEntry.Repository, registerer prometheus.Registerer) (partstore.PartStore, error) {
	obs := &outboxPartStore{
		db:                        db,
		innerPartStore:            innerPartStore,
		partOutboxEntryRepository: partOutboxEntryRepository,
		tracer:                    otel.Tracer("internal/storage/metadatapart/partstore/outbox"),
	}
	obs.runtime = outboxruntime.New(db, obs, outboxruntime.NewMetrics(registerer, "part_outbox", "Number of pending part outbox entries", "Total number of processed part outbox entries", "Duration of part outbox processing in seconds", "Total number of part outbox processing errors"))
	return obs, nil
}

func (obs *outboxPartStore) Start(ctx context.Context) error {
	obs.runtime.Start()
	return obs.innerPartStore.Start(ctx)
}

func (obs *outboxPartStore) Stop(ctx context.Context) error {
	obs.runtime.Stop()
	return obs.innerPartStore.Stop(ctx)
}

func (obs *outboxPartStore) Name() string {
	return "OutboxPartStore"
}

func (obs *outboxPartStore) CountPending(ctx context.Context, tx *sql.Tx) (int, error) {
	return obs.partOutboxEntryRepository.Count(ctx, tx)
}

func (obs *outboxPartStore) FindFirstForUpdate(ctx context.Context, tx *sql.Tx) (*partOutboxEntry.Entity, error) {
	return obs.partOutboxEntryRepository.FindFirstPartOutboxEntryWithForUpdateLock(ctx, tx)
}

func (obs *outboxPartStore) ProcessEntry(ctx context.Context, tx *sql.Tx, partEntry *partOutboxEntry.Entity) error {
	if partEntry == nil {
		return fmt.Errorf("invalid part outbox entry type")
	}

	switch partEntry.Operation {
	case partOutboxEntry.PutPartOperation:
		chunks, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunksById(ctx, tx, *partEntry.Id)
		if err != nil {
			return err
		}
		readers := make([]io.Reader, len(chunks))
		for i, chunk := range chunks {
			readers[i] = bytes.NewReader(chunk.Content)
		}
		return obs.innerPartStore.PutPart(ctx, tx, partEntry.PartId, io.MultiReader(readers...))
	case partOutboxEntry.DeletePartOperation:
		return obs.innerPartStore.DeletePart(ctx, tx, partEntry.PartId)
	default:
		slog.Warn(fmt.Sprint("Invalid operation", partEntry.Operation, "during outbox processing."))
		return fmt.Errorf("invalid operation: %s", partEntry.Operation)
	}
}

func (obs *outboxPartStore) DeleteEntry(ctx context.Context, tx *sql.Tx, partEntry *partOutboxEntry.Entity) error {
	if partEntry == nil {
		return fmt.Errorf("invalid part outbox entry type")
	}
	return obs.partOutboxEntryRepository.DeletePartOutboxEntryById(ctx, tx, *partEntry.Id)
}

const chunkSize = 256 * 1000 * 1000 // 256MB

func (obs *outboxPartStore) storePartOutboxEntry(ctx context.Context, tx *sql.Tx, operation string, partId partstore.PartId) (*ulid.ULID, error) {
	entry := partOutboxEntry.Entity{
		Operation: operation,
		PartId:    partId,
	}
	err := obs.partOutboxEntryRepository.SavePartOutboxEntry(ctx, tx, &entry)
	if err != nil {
		return nil, err
	}

	obs.runtime.Trigger()

	return entry.Id, nil
}

func (obs *outboxPartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.PutPart")
	defer span.End()

	entryId, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.PutPartOperation, partId)
	if err != nil {
		return err
	}

	buffer := make([]byte, chunkSize)
	chunkIndex := 0
	for {
		n, err := io.ReadFull(reader, buffer)
		if n > 0 {
			content := make([]byte, n)
			copy(content, buffer[:n])

			chunk := partOutboxEntry.ContentChunk{
				OutboxEntryId: *entryId,
				ChunkIndex:    chunkIndex,
				Content:       content,
			}
			err = obs.partOutboxEntryRepository.SavePartOutboxContentChunk(ctx, tx, &chunk)
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

func (obs *outboxPartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPart")
	defer span.End()

	lastEntry, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryByPartId(ctx, tx, partId)
	if err != nil {
		return nil, err
	}
	if lastEntry != nil {
		if lastEntry.Operation == partOutboxEntry.DeletePartOperation {
			return nil, partstore.ErrPartNotFound
		}

		chunks, err := obs.partOutboxEntryRepository.FindPartOutboxEntryChunksById(ctx, tx, *lastEntry.Id)
		if err != nil {
			return nil, err
		}
		if len(chunks) > 0 {
			readers := make([]io.Reader, len(chunks))
			for i, chunk := range chunks {
				readers[i] = bytes.NewReader(chunk.Content)
			}
			return io.NopCloser(io.MultiReader(readers...)), nil
		}
	}
	return obs.innerPartStore.GetPart(ctx, tx, partId)
}

func (obs *outboxPartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.GetPartIds")
	defer span.End()

	// We get the lastOutboxEntry for each partId
	lastOutboxEntryGroupedByPartId, err := obs.partOutboxEntryRepository.FindLastPartOutboxEntryGroupedByPartId(ctx, tx)
	if err != nil {
		return nil, err
	}
	// These are the partIds already committed to the innerStorage
	// might be more than we actually currently have...
	innerPartIds, err := obs.innerPartStore.GetPartIds(ctx, tx)
	if err != nil {
		return nil, err
	}

	allPartIds := make(map[partstore.PartId]struct{})
	// write them all into the set
	for _, partId := range innerPartIds {
		allPartIds[partId] = struct{}{}
	}

	// then remove the once that were deleted in the outbox
	// and add the once that were added to the outbox
	for _, outboxEntry := range lastOutboxEntryGroupedByPartId {
		switch outboxEntry.Operation {
		case partOutboxEntry.DeletePartOperation:
			delete(allPartIds, outboxEntry.PartId)
		case partOutboxEntry.PutPartOperation:
			allPartIds[outboxEntry.PartId] = struct{}{}
		}
	}

	// convert the set back to a list
	partIds := []partstore.PartId{}
	for partId := range allPartIds {
		partIds = append(partIds, partId)
	}
	return partIds, nil
}

func (obs *outboxPartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	ctx, span := obs.tracer.Start(ctx, "outboxPartStore.DeletePart")
	defer span.End()

	_, err := obs.storePartOutboxEntry(ctx, tx, partOutboxEntry.DeletePartOperation, partId)
	if err != nil {
		return err
	}

	return nil
}
