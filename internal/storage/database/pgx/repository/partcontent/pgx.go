package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type pgxRepository struct {
}

const (
	findPartContentByIdStmt   = "SELECT id, content, created_at, updated_at FROM part_contents WHERE id = $1"
	findPartContentIdsStmt    = "SELECT id FROM part_contents"
	insertPartContentStmt     = "INSERT INTO part_contents (id, content, created_at, updated_at) VALUES($1, $2, $3, $4)"
	updatePartContentByIdStmt = "UPDATE part_contents SET content = $1, updated_at = $2 WHERE id = $3"
	deletePartContentByIdStmt = "DELETE FROM part_contents WHERE id = $1"
)

func NewRepository() (partcontent.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToPartContentEntity(partContentRow *sql.Row) (*partcontent.Entity, error) {
	var idStr string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := partContentRow.Scan(&idStr, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	id := partstore.MustNewPartIdFromString(idStr)
	return &partcontent.Entity{
		Id:        id,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bcr *pgxRepository) FindPartContentById(ctx context.Context, tx *sql.Tx, id partstore.PartId) (*partcontent.Entity, error) {
	row := tx.QueryRowContext(ctx, findPartContentByIdStmt, id.String())
	partContentEntity, err := convertRowToPartContentEntity(row)
	if err != nil {
		return nil, err
	}
	return partContentEntity, nil
}

func (bcr *pgxRepository) FindPartContentIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	partIdRows, err := tx.QueryContext(ctx, findPartContentIdsStmt)
	if err != nil {
		return nil, err
	}
	defer partIdRows.Close()
	partIds := []partstore.PartId{}
	for partIdRows.Next() {
		var partIdStr string
		err := partIdRows.Scan(&partIdStr)
		if err != nil {
			return nil, err
		}
		partId := partstore.MustNewPartIdFromString(partIdStr)
		partIds = append(partIds, *partId)
	}
	return partIds, nil
}

func (bcr *pgxRepository) PutPartContent(ctx context.Context, tx *sql.Tx, partContent *partcontent.Entity) error {
	_, err := tx.ExecContext(ctx, deletePartContentByIdStmt, partContent.Id.String())
	if err != nil {
		return err
	}
	partContent.CreatedAt = time.Now().UTC()
	partContent.UpdatedAt = partContent.CreatedAt
	_, err = tx.ExecContext(ctx, insertPartContentStmt, partContent.Id.String(), partContent.Content, partContent.CreatedAt, partContent.UpdatedAt)
	return err
}

func (bcr *pgxRepository) SavePartContent(ctx context.Context, tx *sql.Tx, partContent *partcontent.Entity) error {
	if partContent.Id == nil {
		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}
		partContent.Id = partId
		partContent.CreatedAt = time.Now().UTC()
		partContent.UpdatedAt = partContent.CreatedAt
		_, err = tx.ExecContext(ctx, insertPartContentStmt, partContent.Id.String(), partContent.Content, partContent.CreatedAt, partContent.UpdatedAt)
		return err
	}

	partContent.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updatePartContentByIdStmt, partContent.Content, partContent.UpdatedAt, partContent.Id.String())
	return err
}

func (bcr *pgxRepository) DeletePartContentById(ctx context.Context, tx *sql.Tx, id partstore.PartId) error {
	_, err := tx.ExecContext(ctx, deletePartContentByIdStmt, id.String())
	return err
}
