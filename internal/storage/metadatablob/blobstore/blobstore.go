package blobstore

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/oklog/ulid/v2"
)

type BlobId = ulid.ULID

type PutBlobResult struct {
	BlobId BlobId
	Size   int64
	ETag   string
}

var ErrBlobNotFound error = errors.New("blob not found")

type BlobStore interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutBlob(ctx context.Context, tx *sql.Tx, blobId BlobId, reader io.Reader) (*PutBlobResult, error)
	GetBlob(ctx context.Context, tx *sql.Tx, blobId BlobId) (io.ReadSeekCloser, error)
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

func calculateMd5Sum(reader io.Reader) (*string, error) {
	hash := md5.New()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	_, err = hash.Write(data)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	hexSum := hex.EncodeToString(sum)
	return &hexSum, nil
}

func CalculateETag(reader io.Reader) (*string, error) {
	md5Sum, err := calculateMd5Sum(reader)
	if err != nil {
		return nil, err
	}
	etag := "\"" + *md5Sum + "\""
	return &etag, nil
}

func CalculateETagFromPath(path string) (*string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	etag, err := CalculateETag(f)
	if err != nil {
		return nil, err
	}
	return etag, nil
}

func Tester(blobStore BlobStore, db *sql.DB, content []byte) error {
	ctx := context.Background()
	err := blobStore.Start(ctx)
	if err != nil {
		return err
	}
	defer blobStore.Stop(ctx)

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
