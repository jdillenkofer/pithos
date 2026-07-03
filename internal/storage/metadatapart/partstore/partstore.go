package partstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

var ErrPartNotFound error = errors.New("part not found")

// Core part operations
type PartManager interface {
	PutPart(ctx context.Context, tx database.Tx, partId PartId, reader io.Reader) error
	// GetPart returns a ReadCloser for the part with the given partId.
	// If the part does not exist, ErrPartNotFound is returned.
	// The caller is responsible for closing the ReadCloser.
	GetPart(ctx context.Context, tx database.Tx, partId PartId) (io.ReadCloser, error)
	GetPartIds(ctx context.Context, tx database.Tx) ([]PartId, error)
	DeletePart(ctx context.Context, tx database.Tx, partId PartId) error
}

// Composite interface
type PartStore interface {
	lifecycle.Manager
	PartManager
}

// TxFreeGetPartSupporter is implemented by part stores whose GetPart can be
// called with a nil transaction. Streaming reads from such stores do not need
// to hold a database transaction (and therefore a pooled connection) open for
// the lifetime of the download. Middlewares delegate to their inner store(s).
type TxFreeGetPartSupporter interface {
	SupportsTxFreeGetPart() bool
}

// SupportsTxFreeGetPart reports whether ps allows GetPart with a nil
// transaction. Stores that don't implement TxFreeGetPartSupporter are assumed
// to require one.
func SupportsTxFreeGetPart(ps PartStore) bool {
	if s, ok := ps.(TxFreeGetPartSupporter); ok {
		return s.SupportsTxFreeGetPart()
	}
	return false
}

func Tester(partStore PartStore, db database.Database, content []byte) error {
	ctx := context.Background()
	err := partStore.Start(ctx)
	if err != nil {
		return err
	}
	defer partStore.Stop(ctx)

	partId, err := NewRandomPartId()
	if err != nil {
		return err
	}
	part := ioutils.NewByteReadSeekCloser(content)

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return partStore.PutPart(ctx, tx, *partId, part)
	})
	if err != nil {
		return err
	}

	_, err = part.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return partStore.PutPart(ctx, tx, *partId, part)
	})
	if err != nil {
		return err
	}

	// Read inside the transaction: a part store reader may stream lazily from the
	// store (e.g. the SQL store queries chunks on demand), so its lifetime is
	// bound to the transaction. Production reads honor this via WithTxReadClosers.
	var getPartResult []byte
	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		partReader, err := partStore.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		defer partReader.Close()
		getPartResult, err = io.ReadAll(partReader)
		return err
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(content, getPartResult) {
		return errors.New("read result returned invalid content")
	}

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return partStore.DeletePart(ctx, tx, *partId)
	})
	if err != nil {
		return err
	}

	err = database.WithTx(ctx, db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		partReader, err := partStore.GetPart(ctx, tx, *partId)
		if err != nil {
			return err
		}
		partReader.Close()
		return nil
	})
	if err != ErrPartNotFound {
		return errors.New("expected ErrPartNotFound")
	}

	return nil
}
