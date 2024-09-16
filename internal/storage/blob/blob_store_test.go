package blob

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/oklog/ulid/v2"
)

func BlobStoreTester(blobStore BlobStore, db *sql.DB, content []byte) error {
	ctx := context.Background()
	blobId := BlobId(ulid.Make())
	blob := ioutils.NewByteReadSeekCloser(content)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	putBlobResult, err := blobStore.PutBlob(ctx, tx, blobId, blob)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if putBlobResult.BlobId != blobId {
		return errors.New("invalid blobId")
	}
	if putBlobResult.Size != int64(len(content)) {
		return errors.New("invalid size")
	}
	if putBlobResult.ETag == "" {
		return errors.New("invalid etag")
	}

	_, err = blob.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	putBlobResult2, err := blobStore.PutBlob(ctx, tx, blobId, blob)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	if putBlobResult2.BlobId != blobId {
		return errors.New("invalid blobId")
	}
	if putBlobResult2.Size != putBlobResult.Size {
		return errors.New("invalid size")
	}
	if putBlobResult2.ETag != putBlobResult.ETag {
		return errors.New("invalid etag")
	}

	tx, err = db.Begin()
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
	if err != nil {
		return err
	}
	if !bytes.Equal(content, getBlobResult) {
		return errors.New("read result returned invalid content")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	err = blobStore.DeleteBlob(ctx, tx, blobId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	_, err = blobStore.GetBlob(ctx, tx, blobId)
	if err != ErrBlobNotFound {
		tx.Rollback()
		return errors.New("expected ErrBlobNotFound")
	}
	tx.Commit()

	return nil
}
