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
	PutPart(ctx context.Context, tx *database.TxContext, partId PartId, reader io.Reader) error
	// GetPart returns a ReadCloser for the part with the given partId.
	// If the part does not exist, ErrPartNotFound is returned.
	// The caller is responsible for closing the ReadCloser.
	GetPart(ctx context.Context, tx *database.TxContext, partId PartId) (io.ReadCloser, error)
	GetPartIds(ctx context.Context, tx *database.TxContext) ([]PartId, error)
	DeletePart(ctx context.Context, tx *database.TxContext, partId PartId) error
}

// Composite interface
type PartStore interface {
	lifecycle.Manager
	PartManager
}

type TxAwarePartStore interface {
	OnTxCommit(ctx context.Context, tx *database.TxContext) error
	OnTxRollback(ctx context.Context, tx *database.TxContext) error
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

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = partStore.PutPart(ctx, tx, *partId, part)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	_, err = part.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = partStore.PutPart(ctx, tx, *partId, part)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	partReader, err := partStore.GetPart(ctx, tx, *partId)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	getPartResult, err := io.ReadAll(partReader)
	partReader.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(content, getPartResult) {
		return errors.New("read result returned invalid content")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = partStore.DeletePart(ctx, tx, *partId)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	partReader, err = partStore.GetPart(ctx, tx, *partId)
	if err != ErrPartNotFound {
		if partReader != nil {
			partReader.Close()
		}
		_ = tx.Rollback(ctx)
		return errors.New("expected ErrPartNotFound")
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
