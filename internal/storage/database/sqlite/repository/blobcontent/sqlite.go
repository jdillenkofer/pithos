package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
)

type sqliteRepository struct {
}

const (
	findBlobContentByIdStmt   = "SELECT id, content, created_at, updated_at FROM blob_contents WHERE id = $1"
	findBlobContentIdsStmt    = "SELECT id FROM blob_contents"
	insertBlobContentStmt     = "INSERT INTO blob_contents (id, content, created_at, updated_at) VALUES($1, $2, $3, $4)"
	updateBlobContentByIdStmt = "UPDATE blob_contents SET content = $1, updated_at = $2 WHERE id = $3"
	deleteBlobContentByIdStmt = "DELETE FROM blob_contents WHERE id = $1"
)

func NewRepository() (blobcontent.Repository, error) {
	return &sqliteRepository{}, nil
}

func convertRowToBlobContentEntity(blobContentRow *sql.Row) (*blobcontent.Entity, error) {
	var idStr string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobContentRow.Scan(&idStr, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	id, err := blobstore.NewBlobIdFromString(idStr)
	if err != nil {
		return nil, err
	}
	return &blobcontent.Entity{
		Id:        id,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bcr *sqliteRepository) FindBlobContentById(ctx context.Context, tx *sql.Tx, id blobstore.BlobId) (*blobcontent.Entity, error) {
	row := tx.QueryRowContext(ctx, findBlobContentByIdStmt, id.String())
	blobContentEntity, err := convertRowToBlobContentEntity(row)
	if err != nil {
		return nil, err
	}
	return blobContentEntity, nil
}

func (bcr *sqliteRepository) FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	blobIdRows, err := tx.QueryContext(ctx, findBlobContentIdsStmt)
	if err != nil {
		return nil, err
	}
	defer blobIdRows.Close()
	blobIds := []blobstore.BlobId{}
	for blobIdRows.Next() {
		var blobIdStr string
		err := blobIdRows.Scan(&blobIdStr)
		if err != nil {
			return nil, err
		}
		blobId, err := blobstore.NewBlobIdFromString(blobIdStr)
		if err != nil {
			return nil, err
		}
		blobIds = append(blobIds, *blobId)
	}
	return blobIds, nil
}

func (bcr *sqliteRepository) PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.Entity) error {
	_, err := tx.ExecContext(ctx, deleteBlobContentByIdStmt, blobContent.Id.String())
	if err != nil {
		return err
	}
	blobContent.CreatedAt = time.Now().UTC()
	blobContent.UpdatedAt = blobContent.CreatedAt
	_, err = tx.ExecContext(ctx, insertBlobContentStmt, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
	return err
}

func (bcr *sqliteRepository) SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.Entity) error {
	if blobContent.Id == nil {
		blobId, err := blobstore.NewRandomBlobId()
		if err != nil {
			return err
		}
		blobContent.Id = blobId
		blobContent.CreatedAt = time.Now().UTC()
		blobContent.UpdatedAt = blobContent.CreatedAt
		_, err = tx.ExecContext(ctx, insertBlobContentStmt, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
		return err
	}

	blobContent.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateBlobContentByIdStmt, blobContent.Content, blobContent.UpdatedAt, blobContent.Id.String())
	return err
}

func (bcr *sqliteRepository) DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id blobstore.BlobId) error {
	_, err := tx.ExecContext(ctx, deleteBlobContentByIdStmt, id.String())
	return err
}
