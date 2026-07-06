package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partcontent"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type sqliteRepository struct {
}

const (
	findPartContentChunksByIdStmt   = "SELECT id, chunk_index, content, created_at, updated_at FROM part_contents WHERE part_store_id = $1 AND id = $2 ORDER BY chunk_index ASC"
	findPartContentChunkByIndexStmt = "SELECT id, chunk_index, content, created_at, updated_at FROM part_contents WHERE part_store_id = $1 AND id = $2 AND chunk_index = $3"
	findPartContentIdsStmt          = "SELECT DISTINCT id FROM part_contents WHERE part_store_id = $1"
	insertPartContentStmt           = "INSERT INTO part_contents (id, part_store_id, chunk_index, content, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	upsertPartContentStmt           = "INSERT OR REPLACE INTO part_contents (id, part_store_id, chunk_index, content, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	deletePartContentByIdStmt       = "DELETE FROM part_contents WHERE part_store_id = $1 AND id = $2"
)

func NewRepository() (partcontent.Repository, error) {
	return &sqliteRepository{}, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func convertRowToPartContentEntity(scanner rowScanner) (*partcontent.Entity, error) {
	var idStr string
	var chunkIndex int
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := scanner.Scan(&idStr, &chunkIndex, &content, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	id := partstore.MustNewPartIdFromString(idStr)
	return &partcontent.Entity{
		Id:         id,
		ChunkIndex: chunkIndex,
		Content:    content,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
}

func (bcr *sqliteRepository) FindPartContentChunksById(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId) ([]*partcontent.Entity, error) {
	rows, err := tx.QueryContext(ctx, findPartContentChunksByIdStmt, partStoreId, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []*partcontent.Entity
	for rows.Next() {
		chunk, err := convertRowToPartContentEntity(rows)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (bcr *sqliteRepository) FindPartContentChunkByIndex(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId, chunkIndex int) (*partcontent.Entity, error) {
	row := tx.QueryRowContext(ctx, findPartContentChunkByIndexStmt, partStoreId, id.String(), chunkIndex)
	chunk, err := convertRowToPartContentEntity(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return chunk, nil
}

func (bcr *sqliteRepository) FindPartContentIds(ctx context.Context, tx *sql.Tx, partStoreId string) ([]partstore.PartId, error) {
	partIdRows, err := tx.QueryContext(ctx, findPartContentIdsStmt, partStoreId)
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

func (bcr *sqliteRepository) PutPartContent(ctx context.Context, tx *sql.Tx, partStoreId string, partContent *partcontent.Entity) error {
	_, err := tx.ExecContext(ctx, deletePartContentByIdStmt, partStoreId, partContent.Id.String())
	if err != nil {
		return err
	}
	partContent.CreatedAt = time.Now().UTC()
	partContent.UpdatedAt = partContent.CreatedAt
	_, err = tx.ExecContext(ctx, insertPartContentStmt, partContent.Id.String(), partStoreId, partContent.ChunkIndex, partContent.Content, partContent.CreatedAt, partContent.UpdatedAt)
	return err
}

func (bcr *sqliteRepository) SavePartContent(ctx context.Context, tx *sql.Tx, partStoreId string, partContent *partcontent.Entity) error {
	now := time.Now().UTC()
	if partContent.CreatedAt.IsZero() {
		partContent.CreatedAt = now
	}
	partContent.UpdatedAt = now
	_, err := tx.ExecContext(ctx, upsertPartContentStmt, partContent.Id.String(), partStoreId, partContent.ChunkIndex, partContent.Content, partContent.CreatedAt, partContent.UpdatedAt)
	return err
}

func (bcr *sqliteRepository) DeletePartContentById(ctx context.Context, tx *sql.Tx, partStoreId string, id partstore.PartId) error {
	_, err := tx.ExecContext(ctx, deletePartContentByIdStmt, partStoreId, id.String())
	return err
}
