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
	PutPart(ctx context.Context, tx *sql.Tx, partId PartId, reader io.Reader) error
	// GetPart returns a ReadCloser for the part with the given partId.
	// If the part does not exist, ErrPartNotFound is returned
	// or if we return a LazyReadSeekCloser, ErrPartNotFound is returned upon the first read call.
	// The caller is responsible for closing the ReadCloser.
	GetPart(ctx context.Context, tx *sql.Tx, partId PartId) (io.ReadCloser, error)
	GetPartIds(ctx context.Context, tx *sql.Tx) ([]PartId, error)
	DeletePart(ctx context.Context, tx *sql.Tx, partId PartId) error
}

// Composite interface
type PartStore interface {
	lifecycle.Manager
	PartManager
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
		tx.Rollback()
		return err
	}
	tx.Commit()

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
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	partReader, err := partStore.GetPart(ctx, tx, *partId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

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
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	partReader, err = partStore.GetPart(ctx, tx, *partId)
	if err != ErrPartNotFound {
		// Maybe we are dealing with a part store that returns a non-nil reader even if the part is not found.
		_, err = io.ReadAll(partReader)
		partReader.Close()
		if err != ErrPartNotFound {
			tx.Rollback()
			return errors.New("expected ErrPartNotFound")
		}
	}
	tx.Commit()

	return nil
}
