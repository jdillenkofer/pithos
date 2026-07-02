package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/usermetadata"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findUserMetadataByObjectIdOrderByKeyAscStmt = "SELECT id, object_id, key, value, created_at, updated_at FROM object_user_metadata WHERE object_id = $1 ORDER BY key ASC"
	insertUserMetadataStmt                      = "INSERT INTO object_user_metadata (id, object_id, key, value, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6)"
	updateUserMetadataByIdStmt                  = "UPDATE object_user_metadata SET object_id = $1, key = $2, value = $3, updated_at = $4 WHERE id = $5"
	deleteUserMetadataByObjectIdStmt            = "DELETE FROM object_user_metadata WHERE object_id = $1"
)

func NewRepository() (usermetadata.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToUserMetadataEntity(userMetadataRows *sql.Rows) (*usermetadata.Entity, error) {
	var id string
	var objectId string
	var key string
	var value string
	var createdAt time.Time
	var updatedAt time.Time
	err := userMetadataRows.Scan(&id, &objectId, &key, &value, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	userMetadataEntity := usermetadata.Entity{
		Id:        &ulidId,
		ObjectId:  ulid.MustParse(objectId),
		Key:       key,
		Value:     value,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return &userMetadataEntity, nil
}

func (ur *pgxRepository) FindUserMetadataByObjectIdOrderByKeyAsc(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]usermetadata.Entity, error) {
	userMetadataRows, err := tx.QueryContext(ctx, findUserMetadataByObjectIdOrderByKeyAscStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	defer userMetadataRows.Close()
	userMetadata := []usermetadata.Entity{}
	for userMetadataRows.Next() {
		userMetadataEntity, err := convertRowToUserMetadataEntity(userMetadataRows)
		if err != nil {
			return nil, err
		}
		userMetadata = append(userMetadata, *userMetadataEntity)
	}
	return userMetadata, nil
}

func (ur *pgxRepository) SaveUserMetadata(ctx context.Context, tx *sql.Tx, userMetadata *usermetadata.Entity) error {
	if userMetadata.Id == nil {
		id := ulid.Make()
		userMetadata.Id = &id
		userMetadata.CreatedAt = time.Now().UTC()
		userMetadata.UpdatedAt = userMetadata.CreatedAt
		_, err := tx.ExecContext(ctx, insertUserMetadataStmt, userMetadata.Id.String(), userMetadata.ObjectId.String(), userMetadata.Key, userMetadata.Value, userMetadata.CreatedAt, userMetadata.UpdatedAt)
		return err
	}

	userMetadata.UpdatedAt = time.Now().UTC()
	_, err := tx.ExecContext(ctx, updateUserMetadataByIdStmt, userMetadata.ObjectId.String(), userMetadata.Key, userMetadata.Value, userMetadata.UpdatedAt, userMetadata.Id.String())
	return err
}

func (ur *pgxRepository) DeleteUserMetadataByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteUserMetadataByObjectIdStmt, objectId.String())
	return err
}
