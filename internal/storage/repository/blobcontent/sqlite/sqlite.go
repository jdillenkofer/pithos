package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/repository/blobcontent"
	"github.com/oklog/ulid/v2"
)

type sqliteBlobContentRepository struct {
	db                                *sql.DB
	findBlobContentByIdPreparedStmt   *sql.Stmt
	findBlobContentIdsPreparedStmt    *sql.Stmt
	insertBlobContentPreparedStmt     *sql.Stmt
	updateBlobContentByIdPreparedStmt *sql.Stmt
	deleteBlobContentByIdPreparedStmt *sql.Stmt
}

const (
	findBlobContentByIdStmt   = "SELECT id, content, created_at, updated_at FROM blob_contents WHERE id = ?"
	findBlobContentIdsStmt    = "SELECT id FROM blob_contents"
	insertBlobContentStmt     = "INSERT INTO blob_contents (id, content, created_at, updated_at) VALUES(?, ?, ?, ?)"
	updateBlobContentByIdStmt = "UPDATE blob_contents SET content = ?, updated_at = ? WHERE id = ?"
	deleteBlobContentByIdStmt = "DELETE FROM blob_contents WHERE id = ?"
)

func New(db *sql.DB) (blobcontent.BlobContentRepository, error) {
	findBlobContentByIdPreparedStmt, err := db.Prepare(findBlobContentByIdStmt)
	if err != nil {
		return nil, err
	}
	findBlobContentIdsPreparedStmt, err := db.Prepare(findBlobContentIdsStmt)
	if err != nil {
		return nil, err
	}
	insertBlobContentPreparedStmt, err := db.Prepare(insertBlobContentStmt)
	if err != nil {
		return nil, err
	}
	updateBlobContentByIdPreparedStmt, err := db.Prepare(updateBlobContentByIdStmt)
	if err != nil {
		return nil, err
	}
	deleteBlobContentByIdPreparedStmt, err := db.Prepare(deleteBlobContentByIdStmt)
	if err != nil {
		return nil, err
	}
	return &sqliteBlobContentRepository{
		db:                                db,
		findBlobContentByIdPreparedStmt:   findBlobContentByIdPreparedStmt,
		findBlobContentIdsPreparedStmt:    findBlobContentIdsPreparedStmt,
		insertBlobContentPreparedStmt:     insertBlobContentPreparedStmt,
		updateBlobContentByIdPreparedStmt: updateBlobContentByIdPreparedStmt,
		deleteBlobContentByIdPreparedStmt: deleteBlobContentByIdPreparedStmt,
	}, nil
}

func convertRowToBlobContentEntity(blobContentRow *sql.Row) (*blobcontent.BlobContentEntity, error) {
	var id string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobContentRow.Scan(&id, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &blobcontent.BlobContentEntity{
		Id:        &ulidId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bcr *sqliteBlobContentRepository) FindBlobContentById(ctx context.Context, tx *sql.Tx, blobContentId ulid.ULID) (*blobcontent.BlobContentEntity, error) {
	row := tx.StmtContext(ctx, bcr.findBlobContentByIdPreparedStmt).QueryRowContext(ctx, blobContentId.String())
	blobContentEntity, err := convertRowToBlobContentEntity(row)
	if err != nil {
		return nil, err
	}
	return blobContentEntity, nil
}

func (bcr *sqliteBlobContentRepository) FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error) {
	blobIdRows, err := tx.StmtContext(ctx, bcr.findBlobContentIdsPreparedStmt).QueryContext(ctx)
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

func (bcr *sqliteBlobContentRepository) PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.BlobContentEntity) error {
	_, err := tx.StmtContext(ctx, bcr.deleteBlobContentByIdPreparedStmt).ExecContext(ctx, blobContent.Id.String())
	if err != nil {
		return err
	}
	blobContent.CreatedAt = time.Now()
	blobContent.UpdatedAt = blobContent.CreatedAt
	_, err = tx.StmtContext(ctx, bcr.insertBlobContentPreparedStmt).ExecContext(ctx, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
	return err
}

func (bcr *sqliteBlobContentRepository) SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.BlobContentEntity) error {
	if blobContent.Id == nil {
		id := ulid.Make()
		blobContent.Id = &id
		blobContent.CreatedAt = time.Now()
		blobContent.UpdatedAt = blobContent.CreatedAt
		_, err := tx.StmtContext(ctx, bcr.insertBlobContentPreparedStmt).ExecContext(ctx, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
		return err
	}

	blobContent.UpdatedAt = time.Now()
	_, err := tx.StmtContext(ctx, bcr.updateBlobContentByIdPreparedStmt).ExecContext(ctx, blobContent.Content, blobContent.UpdatedAt, blobContent.Id.String())
	return err
}

func (bcr *sqliteBlobContentRepository) DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.StmtContext(ctx, bcr.deleteBlobContentByIdPreparedStmt).ExecContext(ctx, id.String())
	return err
}
