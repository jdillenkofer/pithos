package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobRepository struct {
	db *sql.DB
}

func NewBlobRepository(db *sql.DB) BlobRepository {
	return BlobRepository{
		db: db,
	}
}

type BlobEntity struct {
	Id             *ulid.ULID
	BlobId         ulid.ULID
	ObjectId       ulid.ULID
	ETag           string
	Size           int64
	SequenceNumber int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func convertRowToBlobEntity(blobRows *sql.Rows) (*BlobEntity, error) {
	var id string
	var blobId string
	var objectId string
	var etag string
	var size int64
	var sequenceNumber int
	var createdAt time.Time
	var updatedAt time.Time
	err := blobRows.Scan(&id, &blobId, &objectId, &etag, &size, &sequenceNumber, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	blobEntity := BlobEntity{
		Id:             &ulidId,
		BlobId:         ulid.MustParse(blobId),
		ObjectId:       ulid.MustParse(objectId),
		ETag:           etag,
		Size:           size,
		SequenceNumber: sequenceNumber,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}
	return &blobEntity, nil
}

func (br *BlobRepository) FindInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error) {
	blobIdRows, err := tx.QueryContext(ctx, "SELECT blob_id FROM blobs")
	if err != nil {
		return nil, err
	}
	defer blobIdRows.Close()
	blobIds := []ulid.ULID{}
	for blobIdRows.Next() {
		var blobIdStr string
		err := blobIdRows.Scan(&blobIdStr)
		if err != nil {
			return nil, err
		}
		blobId := ulid.MustParse(blobIdStr)
		blobIds = append(blobIds, blobId)
	}
	return blobIds, nil
}

func (br *BlobRepository) FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]BlobEntity, error) {
	blobRows, err := tx.QueryContext(ctx, "SELECT id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at FROM blobs WHERE object_id = ? ORDER BY sequence_number ASC", objectId.String())
	if err != nil {
		return nil, err
	}
	defer blobRows.Close()
	blobs := []BlobEntity{}
	for blobRows.Next() {
		blobEntity, err := convertRowToBlobEntity(blobRows)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, *blobEntity)
	}
	return blobs, nil
}

func (br *BlobRepository) SaveBlob(ctx context.Context, tx *sql.Tx, blob *BlobEntity) error {
	if blob.Id == nil {
		id := ulid.Make()
		blob.Id = &id
		blob.CreatedAt = time.Now()
		blob.UpdatedAt = blob.CreatedAt
		_, err := tx.ExecContext(ctx, "INSERT INTO blobs (id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", blob.Id.String(), blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.CreatedAt, blob.UpdatedAt)
		return err
	}

	blob.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, "UPDATE blobs SET blob_id = ?, object_id = ?, etag = ?, size = ?, sequence_number = ?, updated_at = ? WHERE id = ?", blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.UpdatedAt, blob.Id.String())
	return err
}

func (br *BlobRepository) DeleteBlobsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM blobs WHERE object_id = ?", objectId.String())
	return err
}
