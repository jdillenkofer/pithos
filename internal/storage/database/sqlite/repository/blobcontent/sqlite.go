package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/oklog/ulid/v2"
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
	return &blobcontent.Entity{
		Id:        &ulidId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bcr *sqliteRepository) FindBlobContentById(ctx context.Context, tx *sql.Tx, blobContentId ulid.ULID) (*blobcontent.Entity, error) {
	row := tx.QueryRowContext(ctx, findBlobContentByIdStmt, blobContentId.String())
	blobContentEntity, err := convertRowToBlobContentEntity(row)
	if err != nil {
		return nil, err
	}
	return blobContentEntity, nil
}

func (bcr *sqliteRepository) FindBlobContentIds(ctx context.Context, tx *sql.Tx) ([]ulid.ULID, error) {
	blobIdRows, err := tx.QueryContext(ctx, findBlobContentIdsStmt)
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

func (bcr *sqliteRepository) PutBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.Entity) error {
	_, err := tx.ExecContext(ctx, deleteBlobContentByIdStmt, blobContent.Id.String())
	if err != nil {
		return err
	}
	blobContent.CreatedAt = time.Now()
	blobContent.UpdatedAt = blobContent.CreatedAt
	_, err = tx.ExecContext(ctx, insertBlobContentStmt, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
	return err
}

func (bcr *sqliteRepository) SaveBlobContent(ctx context.Context, tx *sql.Tx, blobContent *blobcontent.Entity) error {
	if blobContent.Id == nil {
		id := ulid.Make()
		blobContent.Id = &id
		blobContent.CreatedAt = time.Now()
		blobContent.UpdatedAt = blobContent.CreatedAt
		_, err := tx.ExecContext(ctx, insertBlobContentStmt, blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
		return err
	}

	blobContent.UpdatedAt = time.Now()
	_, err := tx.ExecContext(ctx, updateBlobContentByIdStmt, blobContent.Content, blobContent.UpdatedAt, blobContent.Id.String())
	return err
}

func (bcr *sqliteRepository) DeleteBlobContentById(ctx context.Context, tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteBlobContentByIdStmt, id.String())
	return err
}
