package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository/blob"
	"github.com/oklog/ulid/v2"
)

type sqliteBlobRepository struct {
	db                                                      *sql.DB
	findInUseBlobIdsPreparedStmt                            *sql.Stmt
	findBlobsByObjectIdOrderBySequenceNumberAscPreparedStmt *sql.Stmt
	insertBlobPreparedStmt                                  *sql.Stmt
	updateBlobByIdPreparedStmt                              *sql.Stmt
	deleteBlobByObjectIdPreparedStmt                        *sql.Stmt
}

const (
	findInUseBlobIdsStmt                            = "SELECT blob_id FROM blobs"
	findBlobsByObjectIdOrderBySequenceNumberAscStmt = "SELECT id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at FROM blobs WHERE object_id = ? ORDER BY sequence_number ASC"
	insertBlobStmt                                  = "INSERT INTO blobs (id, blob_id, object_id, etag, size, sequence_number, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
	updateBlobByIdStmt                              = "UPDATE blobs SET blob_id = ?, object_id = ?, etag = ?, size = ?, sequence_number = ?, updated_at = ? WHERE id = ?"
	deleteBlobByObjectIdStmt                        = "DELETE FROM blobs WHERE object_id = ?"
)

func New(db *sql.DB) (blob.BlobRepository, error) {
	findInUseBlobIdsPreparedStmt, err := db.Prepare(findInUseBlobIdsStmt)
	if err != nil {
		return nil, err
	}
	findBlobsByObjectIdOrderBySequenceNumberAscPreparedStmt, err := db.Prepare(findBlobsByObjectIdOrderBySequenceNumberAscStmt)
	if err != nil {
		return nil, err
	}
	insertBlobPreparedStmt, err := db.Prepare(insertBlobStmt)
	if err != nil {
		return nil, err
	}
	updateBlobByIdPreparedStmt, err := db.Prepare(updateBlobByIdStmt)
	if err != nil {
		return nil, err
	}
	deleteBlobByObjectIdPreparedStmt, err := db.Prepare(deleteBlobByObjectIdStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteBlobRepository{
		db:                           db,
		findInUseBlobIdsPreparedStmt: findInUseBlobIdsPreparedStmt,
		findBlobsByObjectIdOrderBySequenceNumberAscPreparedStmt: findBlobsByObjectIdOrderBySequenceNumberAscPreparedStmt,
		insertBlobPreparedStmt:           insertBlobPreparedStmt,
		updateBlobByIdPreparedStmt:       updateBlobByIdPreparedStmt,
		deleteBlobByObjectIdPreparedStmt: deleteBlobByObjectIdPreparedStmt,
	}, nil
}

func convertRowToBlobEntity(blobRows *sql.Rows) (*blob.BlobEntity, error) {
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
	blobEntity := blob.BlobEntity{
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

func (br *sqliteBlobRepository) FindInUseBlobIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error) {
	blobIdRows, err := tx.StmtContext(ctx, br.findInUseBlobIdsPreparedStmt).QueryContext(ctx)
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

func (br *sqliteBlobRepository) FindBlobsByObjectIdOrderBySequenceNumberAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]blob.BlobEntity, error) {
	blobRows, err := tx.StmtContext(ctx, br.findBlobsByObjectIdOrderBySequenceNumberAscPreparedStmt).QueryContext(ctx, objectId.String())
	if err != nil {
		return nil, err
	}
	defer blobRows.Close()
	blobs := []blob.BlobEntity{}
	for blobRows.Next() {
		blobEntity, err := convertRowToBlobEntity(blobRows)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, *blobEntity)
	}
	return blobs, nil
}

func (br *sqliteBlobRepository) SaveBlob(ctx context.Context, tx *sql.Tx, blob *blob.BlobEntity) error {
	if blob.Id == nil {
		id := ulid.Make()
		blob.Id = &id
		blob.CreatedAt = time.Now()
		blob.UpdatedAt = blob.CreatedAt
		_, err := tx.StmtContext(ctx, br.insertBlobPreparedStmt).ExecContext(ctx, blob.Id.String(), blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.CreatedAt, blob.UpdatedAt)
		return err
	}

	blob.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, br.updateBlobByIdPreparedStmt).ExecContext(ctx, blob.BlobId.String(), blob.ObjectId.String(), blob.ETag, blob.Size, blob.SequenceNumber, blob.UpdatedAt, blob.Id.String())
	return err
}

func (br *sqliteBlobRepository) DeleteBlobsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.StmtContext(ctx, br.deleteBlobByObjectIdPreparedStmt).ExecContext(ctx, objectId.String())
	return err
}
