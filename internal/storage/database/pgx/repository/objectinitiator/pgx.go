package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/objectinitiator"
	"github.com/oklog/ulid/v2"
)

type pgxRepository struct {
}

const (
	findObjectInitiatorByObjectIdStmt   = "SELECT object_id, account_id, principal_id, created_at, updated_at FROM object_initiators WHERE object_id = $1"
	insertObjectInitiatorStmt           = "INSERT INTO object_initiators (object_id, account_id, principal_id, created_at, updated_at) VALUES($1, $2, $3, $4, $5) ON CONFLICT(object_id) DO UPDATE SET account_id = excluded.account_id, principal_id = excluded.principal_id, updated_at = excluded.updated_at"
	deleteObjectInitiatorByObjectIdStmt = "DELETE FROM object_initiators WHERE object_id = $1"
)

func NewRepository() (objectinitiator.Repository, error) {
	return &pgxRepository{}, nil
}

func convertRowToObjectInitiatorEntity(initiatorRows *sql.Rows) (*objectinitiator.Entity, error) {
	var objectId string
	var accountId string
	var principalId string
	var createdAt time.Time
	var updatedAt time.Time
	err := initiatorRows.Scan(&objectId, &accountId, &principalId, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	return &objectinitiator.Entity{
		ObjectId:    ulid.MustParse(objectId),
		AccountId:   accountId,
		PrincipalId: principalId,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func (ir *pgxRepository) FindObjectInitiatorByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) (*objectinitiator.Entity, error) {
	initiatorRows, err := tx.QueryContext(ctx, findObjectInitiatorByObjectIdStmt, objectId.String())
	if err != nil {
		return nil, err
	}
	defer initiatorRows.Close()
	if !initiatorRows.Next() {
		return nil, nil
	}
	return convertRowToObjectInitiatorEntity(initiatorRows)
}

func (ir *pgxRepository) FindObjectInitiatorsByObjectIdsOrderByObjectId(ctx context.Context, tx *sql.Tx, objectIds []ulid.ULID) ([]objectinitiator.Entity, error) {
	if len(objectIds) == 0 {
		return []objectinitiator.Entity{}, nil
	}
	placeholders := make([]string, len(objectIds))
	args := make([]any, len(objectIds))
	for i, objectId := range objectIds {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = objectId.String()
	}
	query := "SELECT object_id, account_id, principal_id, created_at, updated_at FROM object_initiators WHERE object_id IN (" + strings.Join(placeholders, ", ") + ") ORDER BY object_id ASC"
	initiatorRows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer initiatorRows.Close()
	initiators := []objectinitiator.Entity{}
	for initiatorRows.Next() {
		initiatorEntity, err := convertRowToObjectInitiatorEntity(initiatorRows)
		if err != nil {
			return nil, err
		}
		initiators = append(initiators, *initiatorEntity)
	}
	return initiators, nil
}

func (ir *pgxRepository) SaveObjectInitiator(ctx context.Context, tx *sql.Tx, initiator *objectinitiator.Entity) error {
	now := time.Now().UTC()
	if initiator.CreatedAt.IsZero() {
		initiator.CreatedAt = now
	}
	initiator.UpdatedAt = now
	_, err := tx.ExecContext(ctx, insertObjectInitiatorStmt, initiator.ObjectId.String(), initiator.AccountId, initiator.PrincipalId, initiator.CreatedAt, initiator.UpdatedAt)
	return err
}

func (ir *pgxRepository) DeleteObjectInitiatorByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	_, err := tx.ExecContext(ctx, deleteObjectInitiatorByObjectIdStmt, objectId.String())
	return err
}
