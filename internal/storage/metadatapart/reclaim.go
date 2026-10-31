package metadatapart

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

const reclaimTimeout = 30 * time.Second

type writtenPart struct {
	storeName *string
	id        partstore.PartId
}

// writtenParts collects the parts a write transaction stores. Stores outside
// the database keep their bytes when the transaction rolls back, so the parts
// are registered for reclamation when it does not commit.
type writtenParts []writtenPart

func (w *writtenParts) add(storeName *string, id partstore.PartId) {
	*w = append(*w, writtenPart{storeName: storeName, id: id})
}

// reclaimWrittenParts registers the parts of a failed write transaction in a
// new transaction, so the next sweep deletes them instead of leaving them to
// the rare full pass. A part the transaction registered after all (a commit
// reported as failed, or a failing after-commit hook) keeps its row. Inside an
// enclosing transaction the outcome is not yet known and nothing is done.
func (mbs *metadataPartStorage) reclaimWrittenParts(ctx context.Context, txErr error, written writtenParts) {
	if txErr == nil || len(written) == 0 {
		return
	}
	if _, nested := database.TxControllerFromContext(ctx); nested {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reclaimTimeout)
	defer cancel()
	err := database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		for _, part := range written {
			if err := mbs.metadataStore.RegisterUnreferencedPart(ctx, tx.SqlTx(), part.id, part.storeName); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Warn("registering parts of a failed write failed; the next GC full pass reclaims them", "parts", len(written), "error", err)
	}
}
