package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/tag"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findTagsByObjectIdOrderByKeyAscStmt = "SELECT id, object_id, key, value, created_at, updated_at FROM object_tags WHERE object_id = $1 ORDER BY key ASC"
	insertTagStmt                       = "INSERT INTO object_tags (id, object_id, key, value, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	updateTagByIdStmt                   = "UPDATE object_tags SET object_id = $1, key = $2, value = $3, updated_at = $4 WHERE id = $5"
	deleteTagsByObjectIdStmt            = "DELETE FROM object_tags WHERE object_id = $1"
)

func NewRepository() (tag.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToTagEntity(tagRows *sql.Rows) (*tag.Entity, error) {
	var id string
	var objectId string
	var key string
	var value string
	var createdAt time.Time
	var updatedAt time.Time
	err := tagRows.Scan(&id, &objectId, &key, &value, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	tagEntity := tag.Entity{
		Id:        &ulidId,
		ObjectId:  ulid.MustParse(objectId),
		Key:       key,
		Value:     value,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return &tagEntity, nil
}

func (tr *pgxRepository) FindTagsByObjectIdOrderByKeyAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]tag.Entity, error) {
	tagRows, err := tx.QueryContext(ctx, findTagsByObjectIdOrderByKeyAscStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	defer tagRows.Close()
	tags := []tag.Entity{}
	for tagRows.Next() {
		tagEntity, err := convertRowToTagEntity(tagRows)
		if err != nil {
			return nil, err
		}
		tags = append(tags, *tagEntity)
	}
	return tags, nil
}

func (tr *pgxRepository) FindTagsByObjectIdsOrderByObjectIdAndKeyAsc(ctx context.Context, tx *sql.Tx, objectIds []ulid.ULID) ([]tag.Entity, error) {
	if len(objectIds) == 0 {
		return []tag.Entity{}, nil
	}
	placeholders := make([]string, len(objectIds))
	args := make([]any, len(objectIds))
	for i, objectId := range objectIds {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = objectId.String()
	}
	query := "SELECT id, object_id, key, value, created_at, updated_at FROM object_tags WHERE object_id IN (" + strings.Join(placeholders, ", ") + ") ORDER BY object_id ASC, key ASC"
	tagRows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer tagRows.Close()
	tags := []tag.Entity{}
	for tagRows.Next() {
		tagEntity, err := convertRowToTagEntity(tagRows)
		if err != nil {
			return nil, err
		}
		tags = append(tags, *tagEntity)
	}
	return tags, nil
}

func (tr *pgxRepository) SaveTag(ctx context.Context, tx *sql.Tx, tag *tag.Entity) error {
	if tag.Id == nil {
		id := ulid.Make()
		tag.Id = &id
		tag.CreatedAt = time.Now().UTC()
		tag.UpdatedAt = tag.CreatedAt
		_, err := tx.ExecContext(ctx, insertTagStmt, tag.Id.String(), tag.ObjectId.String(), tag.Key, tag.Value, tag.CreatedAt, tag.UpdatedAt)
		return err
	}

	tag.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateTagByIdStmt, tag.ObjectId.String(), tag.Key, tag.Value, tag.UpdatedAt, tag.Id.String())
	return err
}

func (tr *pgxRepository) DeleteTagsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteTagsByObjectIdStmt, objectId.String())
	return err
}
