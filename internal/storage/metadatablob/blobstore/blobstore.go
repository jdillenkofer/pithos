package blobstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/oklog/ulid/v2"
)

type BlobId = ulid.ULID

var ErrBlobNotFound error = errors.New("blob not found")

type BlobStore interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, reader io.Reader) error
	// GetBlob returns a ReadCloser for the blob with the given blobId.
	// If the blob does not exist, ErrBlobNotFound is returned
	// or if we return a LazyReadSeekCloser, ErrBlobNotFound is returned upon the first read call.
	// The caller is responsible for closing the ReadCloser.
	GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadCloser, error)
	GetBlobIds(ctx context.Context, tx *sql.Tx) ([]BlobId, error)
	DeleteBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) error
}

func GenerateBlobId() (*BlobId, error) {
	blobIdBytes := make([]byte, 8)
	_, err := rand.Read(blobIdBytes)
	if err != nil {
		return nil, err
	}
	blobId := BlobId(ulid.Make())
	return &blobId, nil
}

func Tester(blobStore BlobStore, db database.Database, content []byte) error {
	ctx := context.Background()
	err := blobStore.Start(ctx)
	if err != nil {
		return err
	}
	defer blobStore.Stop(ctx)

	blobId := BlobId(ulid.Make())
	blob := ioutils.NewByteReadSeekCloser(content)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = blobStore.PutBlob(ctx, tx, blobId, blob)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	_, err = blob.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = blobStore.PutBlob(ctx, tx, blobId, blob)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	blobReader, err := blobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	getBlobResult, err := io.ReadAll(blobReader)
	blobReader.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(content, getBlobResult) {
		return errors.New("read result returned invalid content")
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return err
	}
	err = blobStore.DeleteBlob(ctx, tx, blobId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	blobReader, err = blobStore.GetBlob(ctx, tx, blobId)
	if err != ErrBlobNotFound {
		// Maybe we are dealing with a blob store that returns a non-nil reader even if the blob is not found.
		_, err = io.ReadAll(blobReader)
		blobReader.Close()
		if err != ErrBlobNotFound {
			tx.Rollback()
			return errors.New("expected ErrBlobNotFound")
		}
	}
	tx.Commit()

	return nil
}
